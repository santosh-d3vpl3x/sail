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

fn reject_operational_option_if(
    options: &[OptionLayer],
    keys: &[&str],
    should_reject: impl Fn(&str) -> bool,
) -> DataSourceResult<()> {
    for layer in options {
        // Table properties are metadata, not an invocation of a reader/writer option.
        let OptionLayer::OptionList { items } = layer else {
            continue;
        };
        for (key, value) in items {
            if keys
                .iter()
                .any(|candidate| key.eq_ignore_ascii_case(candidate))
                && should_reject(value)
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

fn reject_operational_option_presence(
    options: &[OptionLayer],
    keys: &[&str],
) -> DataSourceResult<()> {
    reject_operational_option_if(options, keys, |_| true)
}

fn validate_partition_overwrite_mode(options: &[OptionLayer]) -> DataSourceResult<()> {
    // Option layers are applied in order. Validate only the effective value so an explicit
    // writer option continues to override a session fallback.
    let mut effective: Option<(&str, &str)> = None;
    for layer in options {
        let OptionLayer::OptionList { items } = layer else {
            continue;
        };
        for (key, value) in items {
            if key.eq_ignore_ascii_case("partition_overwrite_mode")
                || key.eq_ignore_ascii_case("partitionOverwriteMode")
            {
                effective = Some((key, value));
            }
        }
    }

    let Some((key, value)) = effective else {
        return Ok(());
    };
    if value.trim().eq_ignore_ascii_case("static") {
        return Ok(());
    }
    if value.trim().eq_ignore_ascii_case("dynamic") {
        return Err(DataSourceError::InvalidOption {
            key: key.to_string(),
            value: value.to_string(),
            cause: Some(
                "unsupported Delta option affects data correctness and must not be silently ignored"
                    .to_string(),
            ),
        });
    }
    Err(DataSourceError::InvalidOption {
        key: key.to_string(),
        value: value.to_string(),
        cause: Some("expected one of: static, dynamic".to_string()),
    })
}

impl ResolveOptions for r#gen::DeltaReadOptions {
    fn resolve(_ctx: &dyn Session, options: Vec<OptionLayer>) -> DataSourceResult<Self> {
        // readChangeFeed=false is equivalent to the supported snapshot read and is safe.
        reject_operational_option_if(&options, &["read_change_feed", "readChangeFeed"], |value| {
            value.trim().eq_ignore_ascii_case("true")
        })?;
        // A requested CDF/version range changes which rows must be returned, regardless of the
        // surrounding readChangeFeed spelling. Until implemented, never return a full snapshot.
        reject_operational_option_presence(
            &options,
            &[
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
        // Transaction identifiers request idempotent write semantics. Presence alone is enough
        // to be unsafe to ignore.
        reject_operational_option_presence(
            &options,
            &["txn_version", "txnVersion", "txn_app_id", "txnAppId"],
        )?;

        // Static is Sail's current overwrite behavior and is safe to accept explicitly. Dynamic
        // is not implemented and would otherwise silently delete untouched partitions. Unknown
        // values are rejected rather than being silently ignored.
        validate_partition_overwrite_mode(&options)?;

        // dataChange=true is the behavior Sail already emits. Only false requests semantics that
        // Sail cannot currently represent safely for downstream incremental readers.
        reject_operational_option_if(&options, &["data_change", "dataChange"], |value| {
            value.trim().eq_ignore_ascii_case("false")
        })?;

        let mut partial = r#gen::DeltaWritePartialOptions::initialize();
        for layer in options {
            partial.merge(layer.build_partial_options()?);
        }
        partial.finalize()
    }
}
