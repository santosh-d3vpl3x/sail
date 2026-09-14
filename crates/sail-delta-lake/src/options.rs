// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use datafusion::catalog::Session;
use sail_common_datafusion::datasource::OptionLayer;
pub(crate) use sail_data_source::options::{BuildPartialOptions, PartialOptions, ResolveOptions};
use serde::{Deserialize, Serialize};

use crate::error::{DataSourceError, DataSourceResult};

pub mod r#gen {
    include!(concat!(env!("OUT_DIR"), "/options/delta.rs"));
}

pub(crate) mod parsers {
    pub(crate) use sail_data_source::options::parsers::*;

    pub(crate) use super::parse_delta_log_replay_strategy;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DeltaLogReplayStrategy {
    #[default]
    Auto,
    Sort,
    Hash,
}

pub fn parse_delta_log_replay_strategy(
    key: &str,
    value: &str,
) -> DataSourceResult<DeltaLogReplayStrategy> {
    match value.to_ascii_lowercase().as_str() {
        "auto" => Ok(DeltaLogReplayStrategy::Auto),
        "sort" => Ok(DeltaLogReplayStrategy::Sort),
        "hash" => Ok(DeltaLogReplayStrategy::Hash),
        _ => Err(DataSourceError::InvalidOption {
            key: key.to_string(),
            value: value.to_string(),
            cause: None,
        }),
    }
}

fn reject_unsupported_semantic_options(
    options: &[OptionLayer],
    unsupported_keys: &[&str],
) -> DataSourceResult<()> {
    for layer in options {
        let items = match layer {
            OptionLayer::OptionList { items } | OptionLayer::TablePropertyList { items } => items,
            _ => continue,
        };
        for (key, value) in items {
            let normalized_key = key.strip_prefix("option.").unwrap_or(key);
            if unsupported_keys
                .iter()
                .any(|candidate| normalized_key.eq_ignore_ascii_case(candidate))
            {
                return Err(DataSourceError::InvalidOption {
                    key: key.clone(),
                    value: value.clone(),
                    cause: Some(
                        "unsupported Delta option affects data correctness and must not be silently ignored"
                            .to_string(),
                    ),
                });
            }
        }
    }
    Ok(())
}

impl ResolveOptions for r#gen::DeltaReadOptions {
    fn resolve(_ctx: &dyn Session, options: Vec<OptionLayer>) -> DataSourceResult<Self> {
        // These options change which logical rows a Delta read returns. Until Sail implements
        // them, fail fast rather than returning an ordinary snapshot with silently different
        // semantics.
        reject_unsupported_semantic_options(
            &options,
            &[
                "read_change_feed",
                "readChangeFeed",
                "starting_version",
                "startingVersion",
                "starting_timestamp",
                "startingTimestamp",
                "ending_version",
                "endingVersion",
                "ending_timestamp",
                "endingTimestamp",
            ],
        )?;

        let mut partial = r#gen::DeltaReadPartialOptions::initialize();
        for layer in options {
            partial.merge(layer.build_partial_options()?);
        }
        partial.finalize()
    }
}

impl ResolveOptions for r#gen::DeltaWriteOptions {
    fn resolve(_ctx: &dyn Session, options: Vec<OptionLayer>) -> DataSourceResult<Self> {
        // Ignoring these changes write semantics: transaction IDs provide idempotence,
        // dynamic partition overwrite determines which existing partitions are removed, and
        // dataChange controls whether downstream incremental readers treat rewritten files as
        // logical data changes.
        reject_unsupported_semantic_options(
            &options,
            &[
                "txn_version",
                "txnVersion",
                "txn_app_id",
                "txnAppId",
                "partition_overwrite_mode",
                "partitionOverwriteMode",
                "data_change",
                "dataChange",
            ],
        )?;

        let mut partial = r#gen::DeltaWritePartialOptions::initialize();
        for layer in options {
            partial.merge(layer.build_partial_options()?);
        }
        partial.finalize()
    }
}
