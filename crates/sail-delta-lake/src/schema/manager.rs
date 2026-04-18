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

use std::collections::HashMap;

use datafusion::arrow::datatypes::Schema as ArrowSchema;

use super::mapping::{
    annotate_new_fields_for_column_mapping, annotate_schema_for_column_mapping,
    compute_max_column_id,
};
use crate::spec::{
    canonicalize_and_validate_table_properties, contains_timestampntz_arrow, ColumnMappingMode,
    DeltaError as DeltaTableError, DeltaResult, Metadata, Protocol, StructType, TableFeature,
    TableProperties,
};

/// Evolve table schema and update metadata according to column mapping mode.
pub fn evolve_schema(
    existing: &StructType,
    candidate: &StructType,
    metadata: &Metadata,
    mode: ColumnMappingMode,
) -> DeltaResult<(StructType, Metadata)> {
    let updated = if matches!(mode, ColumnMappingMode::Name | ColumnMappingMode::Id) {
        let next_id = metadata
            .configuration()
            .get("delta.columnMapping.maxColumnId")
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or_else(|| compute_max_column_id(existing));

        let (annotated, last_id) =
            annotate_new_fields_for_column_mapping(existing, candidate, next_id + 1);

        let meta_with_schema = metadata.clone().with_schema(&annotated)?;
        let meta_with_max = meta_with_schema.add_config_key(
            "delta.columnMapping.maxColumnId".to_string(),
            last_id.to_string(),
        );
        (annotated, meta_with_max)
    } else {
        let meta = metadata.clone().with_schema(candidate)?;
        (candidate.clone(), meta)
    };
    Ok(updated)
}

/// Build Metadata for table creation from an existing kernel StructType.
pub fn metadata_for_create_with_struct_type(
    schema: StructType,
    partition_columns: Vec<String>,
    created_time: i64,
    configuration: HashMap<String, String>,
) -> DeltaResult<Metadata> {
    Metadata::try_new(
        None,
        None,
        schema,
        partition_columns,
        created_time,
        configuration,
    )
}

/// Build Protocol for a create/write path based on required table features.
///
/// In addition to the explicitly toggled features, this function scans the table
/// `configuration` for `delta.feature.<name> = "supported"` entries and includes
/// the corresponding [`TableFeature`] in the protocol.
pub fn protocol_for_create(
    enable_column_mapping: bool,
    enable_timestamp_ntz: bool,
    enable_in_commit_timestamps: bool,
    configuration: &HashMap<String, String>,
) -> DeltaResult<Protocol> {
    let mut reader_features = Vec::new();
    let mut writer_features = Vec::new();

    if enable_column_mapping {
        reader_features.push(TableFeature::ColumnMapping);
        writer_features.push(TableFeature::ColumnMapping);
    }
    if enable_timestamp_ntz {
        reader_features.push(TableFeature::TimestampWithoutTimezone);
        writer_features.push(TableFeature::TimestampWithoutTimezone);
    }
    if enable_in_commit_timestamps {
        writer_features.push(TableFeature::InCommitTimestamp);
    }

    // Extract features from `delta.feature.<name> = "supported"|"enabled"` configuration entries.
    // Unknown feature names always produce an error regardless of value.
    for (key, value) in configuration {
        if let Some(name) = key.strip_prefix("delta.feature.") {
            let status = value.to_lowercase();
            if status != "supported" && status != "enabled" {
                return Err(DeltaTableError::generic(format!(
                    "invalid value `{value}` for table feature property `{key}`; \
                     expected \"supported\" or \"enabled\"",
                )));
            }
            match TableFeature::parse_str_name(name) {
                Ok(feature) => {
                    if feature.is_reader_feature() && !reader_features.contains(&feature) {
                        reader_features.push(feature.clone());
                    }
                    if !writer_features.contains(&feature) {
                        writer_features.push(feature);
                    }
                }
                Err(_) => {
                    return Err(DeltaTableError::generic(format!(
                        "unknown table feature `{name}` in `{key}` = `{value}`; \
                         check for typos in the feature name",
                    )));
                }
            }
        }
    }

    // `delta.checkpointPolicy = "v2"` implicitly activates V2Checkpoint
    if configuration
        .get("delta.checkpointPolicy")
        .map(|v| v.eq_ignore_ascii_case("v2"))
        .unwrap_or(false)
    {
        if !reader_features.contains(&TableFeature::V2Checkpoint) {
            reader_features.push(TableFeature::V2Checkpoint);
        }
        if !writer_features.contains(&TableFeature::V2Checkpoint) {
            writer_features.push(TableFeature::V2Checkpoint);
        }
    }

    if reader_features.is_empty() && writer_features.is_empty() {
        return Ok(Protocol::new(1, 2, None, None));
    }

    let min_reader_version = if reader_features.is_empty() { 1 } else { 3 };

    Ok(Protocol::new(
        min_reader_version,
        7,
        Some(reader_features),
        Some(writer_features),
    ))
}

/// Actions produced for the initial version-0 commit of a new Delta table.
pub struct InitialCreateActions {
    pub protocol: Protocol,
    pub metadata: Metadata,
    /// The canonicalized + column-mapping-normalized configuration actually
    /// persisted in the metadata action.
    pub configuration: HashMap<String, String>,
    /// Effective column mapping mode derived from the configuration.
    pub column_mapping_mode: ColumnMappingMode,
}

/// Build Protocol + Metadata for the initial version-0 commit of a new Delta table.
///
/// This is the single source of truth used by both the CREATE TABLE init path and
/// the CTAS/first-write path, so the same Arrow schema and properties always produce
/// the same on-disk commit.
///
/// The returned `configuration` is canonicalized and, when column mapping is enabled,
/// extended with `delta.columnMapping.mode` and `delta.columnMapping.maxColumnId`.
pub fn build_initial_create_actions(
    arrow_schema: &ArrowSchema,
    partition_columns: Vec<String>,
    raw_configuration: HashMap<String, String>,
    created_time_ms: i64,
    name: Option<String>,
) -> DeltaResult<InitialCreateActions> {
    let mut configuration = canonicalize_and_validate_table_properties(
        raw_configuration
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str())),
    )?;

    let has_timestamp_ntz = contains_timestampntz_arrow(arrow_schema);
    let kernel_schema = StructType::try_from(arrow_schema)?;

    let column_mapping_mode = configuration
        .get("delta.columnMapping.mode")
        .and_then(|v| ColumnMappingMode::try_from(v.as_str()).ok())
        .unwrap_or_default();

    let metadata_schema = if !matches!(column_mapping_mode, ColumnMappingMode::None) {
        let annotated = annotate_schema_for_column_mapping(&kernel_schema);
        configuration.insert(
            "delta.columnMapping.mode".to_string(),
            column_mapping_mode.as_ref().to_string(),
        );
        configuration.insert(
            "delta.columnMapping.maxColumnId".to_string(),
            compute_max_column_id(&annotated).to_string(),
        );
        annotated
    } else {
        kernel_schema
    };

    let enable_in_commit_timestamps =
        TableProperties::from(configuration.iter()).enable_in_commit_timestamps();

    let protocol = protocol_for_create(
        !matches!(column_mapping_mode, ColumnMappingMode::None),
        has_timestamp_ntz,
        enable_in_commit_timestamps,
        &configuration,
    )?;

    let metadata = Metadata::try_new(
        name,
        None,
        metadata_schema,
        partition_columns,
        created_time_ms,
        configuration.clone(),
    )?;

    Ok(InitialCreateActions {
        protocol,
        metadata,
        configuration,
        column_mapping_mode,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::protocol_for_create;
    use crate::spec::{DeltaResult, TableFeature};

    #[test]
    fn protocol_for_create_treats_in_commit_timestamp_as_writer_only() -> DeltaResult<()> {
        let protocol = protocol_for_create(false, false, true, &HashMap::new())?;
        assert_eq!(protocol.min_reader_version(), 1);
        assert_eq!(protocol.min_writer_version(), 7);
        assert_eq!(protocol.reader_features(), None);
        assert_eq!(
            protocol.writer_features(),
            Some([TableFeature::InCommitTimestamp].as_slice())
        );
        Ok(())
    }

    #[test]
    fn protocol_for_create_extracts_v2_checkpoint_from_configuration() -> DeltaResult<()> {
        // "enabled" (deprecated) still accepted for backward compatibility.
        let mut config = HashMap::new();
        config.insert(
            "delta.feature.v2Checkpoint".to_string(),
            "enabled".to_string(),
        );
        let protocol = protocol_for_create(false, false, false, &config)?;
        assert_eq!(protocol.min_reader_version(), 3);
        assert_eq!(protocol.min_writer_version(), 7);
        assert!(protocol.has_reader_feature(&TableFeature::V2Checkpoint));
        assert!(protocol.has_writer_feature(&TableFeature::V2Checkpoint));
        Ok(())
    }

    #[test]
    fn protocol_for_create_extracts_v2_checkpoint_with_supported_value() -> DeltaResult<()> {
        // "supported" is the current/preferred value.
        let mut config = HashMap::new();
        config.insert(
            "delta.feature.v2Checkpoint".to_string(),
            "supported".to_string(),
        );
        let protocol = protocol_for_create(false, false, false, &config)?;
        assert_eq!(protocol.min_reader_version(), 3);
        assert_eq!(protocol.min_writer_version(), 7);
        assert!(protocol.has_reader_feature(&TableFeature::V2Checkpoint));
        assert!(protocol.has_writer_feature(&TableFeature::V2Checkpoint));
        Ok(())
    }

    #[test]
    fn protocol_for_create_activates_v2_checkpoint_from_checkpoint_policy() -> DeltaResult<()> {
        let mut config = HashMap::new();
        config.insert("delta.checkpointPolicy".to_string(), "v2".to_string());
        let protocol = protocol_for_create(false, false, false, &config)?;
        assert_eq!(protocol.min_reader_version(), 3);
        assert_eq!(protocol.min_writer_version(), 7);
        assert!(protocol.has_reader_feature(&TableFeature::V2Checkpoint));
        assert!(protocol.has_writer_feature(&TableFeature::V2Checkpoint));
        Ok(())
    }

    #[test]
    fn protocol_for_create_classic_policy_does_not_activate_v2_checkpoint() -> DeltaResult<()> {
        let mut config = HashMap::new();
        config.insert("delta.checkpointPolicy".to_string(), "classic".to_string());
        let protocol = protocol_for_create(false, false, false, &config)?;
        assert_eq!(protocol.min_reader_version(), 1);
        assert_eq!(protocol.min_writer_version(), 2);
        assert!(!protocol.has_reader_feature(&TableFeature::V2Checkpoint));
        assert!(!protocol.has_writer_feature(&TableFeature::V2Checkpoint));
        Ok(())
    }

    #[test]
    #[expect(clippy::panic)]
    fn protocol_for_create_errors_on_unknown_feature_name() {
        // Typo in the feature name must be caught instead of silently ignored.
        let mut config = HashMap::new();
        config.insert(
            "delta.feature.v2Checkpiont".to_string(), // intentional typo
            "supported".to_string(),
        );
        let Err(err) = protocol_for_create(false, false, false, &config) else {
            panic!("expected protocol_for_create to error on unknown feature name");
        };
        let msg = err.to_string();
        assert!(
            msg.contains("v2Checkpiont"),
            "error message should include the bad feature name: {msg}"
        );
    }

    #[test]
    #[expect(clippy::panic)]
    fn protocol_for_create_errors_on_invalid_feature_value() {
        // Any value other than "supported" or "enabled" must produce an error.
        let mut config = HashMap::new();
        config.insert(
            "delta.feature.v2Checkpoint".to_string(),
            "true".to_string(), // invalid
        );
        let Err(err) = protocol_for_create(false, false, false, &config) else {
            panic!("expected protocol_for_create to error on invalid feature value");
        };
        let msg = err.to_string();
        assert!(
            msg.contains("true"),
            "error message should include the bad value: {msg}"
        );
    }

    mod initial_create_actions {
        use std::collections::HashMap;

        use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema, TimeUnit};

        use super::super::build_initial_create_actions;
        use crate::spec::{ColumnMappingMode, DeltaResult, TableFeature};

        fn plain_schema() -> ArrowSchema {
            ArrowSchema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("value", DataType::Utf8, true),
            ])
        }

        #[test]
        fn plain_table_has_protocol_1_2_and_no_features() -> DeltaResult<()> {
            let actions = build_initial_create_actions(
                &plain_schema(),
                vec![],
                HashMap::new(),
                42,
                Some("my_table".to_string()),
            )?;
            assert_eq!(actions.protocol.min_reader_version(), 1);
            assert_eq!(actions.protocol.min_writer_version(), 2);
            assert_eq!(actions.protocol.reader_features(), None);
            assert_eq!(actions.protocol.writer_features(), None);
            assert!(matches!(
                actions.column_mapping_mode,
                ColumnMappingMode::None
            ));
            assert_eq!(actions.metadata.name(), Some("my_table"));
            assert!(actions.metadata.partition_columns().is_empty());
            assert!(!actions
                .configuration
                .contains_key("delta.columnMapping.mode"));
            Ok(())
        }

        #[test]
        fn partitioned_table_records_partition_columns() -> DeltaResult<()> {
            let schema = ArrowSchema::new(vec![
                Field::new("part", DataType::Utf8, false),
                Field::new("value", DataType::Int64, true),
            ]);
            let actions = build_initial_create_actions(
                &schema,
                vec!["part".to_string()],
                HashMap::new(),
                0,
                None,
            )?;
            assert_eq!(
                actions.metadata.partition_columns().as_slice(),
                &["part".to_string()]
            );
            Ok(())
        }

        #[test]
        fn column_mapping_mode_enables_feature_and_annotates_schema() -> DeltaResult<()> {
            let mut config = HashMap::new();
            config.insert("delta.columnMapping.mode".to_string(), "name".to_string());
            let actions = build_initial_create_actions(
                &plain_schema(),
                vec![],
                config,
                0,
                Some("t".to_string()),
            )?;
            assert!(matches!(
                actions.column_mapping_mode,
                ColumnMappingMode::Name
            ));
            assert!(actions
                .protocol
                .has_reader_feature(&TableFeature::ColumnMapping));
            assert!(actions
                .protocol
                .has_writer_feature(&TableFeature::ColumnMapping));
            assert_eq!(
                actions.configuration.get("delta.columnMapping.mode"),
                Some(&"name".to_string())
            );
            assert!(actions
                .configuration
                .contains_key("delta.columnMapping.maxColumnId"));
            // Annotated schema fields carry physical names in metadata.
            let schema = actions.metadata.parse_schema()?;
            for (i, field) in schema.fields().enumerate() {
                let meta = field.metadata();
                assert!(
                    meta.contains_key("delta.columnMapping.id"),
                    "field {i} missing delta.columnMapping.id"
                );
                assert!(
                    meta.contains_key("delta.columnMapping.physicalName"),
                    "field {i} missing delta.columnMapping.physicalName"
                );
            }
            Ok(())
        }

        #[test]
        fn enable_in_commit_timestamps_sets_writer_feature() -> DeltaResult<()> {
            let mut config = HashMap::new();
            config.insert(
                "delta.enableInCommitTimestamps".to_string(),
                "true".to_string(),
            );
            let actions = build_initial_create_actions(&plain_schema(), vec![], config, 0, None)?;
            assert!(actions
                .protocol
                .has_writer_feature(&TableFeature::InCommitTimestamp));
            // ICT is writer-only, so reader version stays at 1 unless other reader features force it.
            assert!(!actions
                .protocol
                .has_reader_feature(&TableFeature::InCommitTimestamp));
            Ok(())
        }

        #[test]
        fn timestamp_ntz_schema_enables_feature() -> DeltaResult<()> {
            let schema = ArrowSchema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, None), true),
            ]);
            let actions = build_initial_create_actions(&schema, vec![], HashMap::new(), 0, None)?;
            assert!(actions
                .protocol
                .has_reader_feature(&TableFeature::TimestampWithoutTimezone));
            assert!(actions
                .protocol
                .has_writer_feature(&TableFeature::TimestampWithoutTimezone));
            Ok(())
        }

        #[test]
        fn alias_properties_are_canonicalized() -> DeltaResult<()> {
            // Delta aliases should be rewritten to their canonical `delta.*` keys;
            // unrelated keys pass through unchanged (matching the writer path).
            let mut config = HashMap::new();
            config.insert("append_only".to_string(), "true".to_string());
            config.insert("column_mapping_mode".to_string(), "name".to_string());
            config.insert("unrelated".to_string(), "value".to_string());
            let actions = build_initial_create_actions(&plain_schema(), vec![], config, 0, None)?;
            assert_eq!(
                actions.configuration.get("delta.appendOnly"),
                Some(&"true".to_string())
            );
            assert_eq!(
                actions.configuration.get("delta.columnMapping.mode"),
                Some(&"name".to_string())
            );
            assert_eq!(
                actions.configuration.get("unrelated"),
                Some(&"value".to_string())
            );
            Ok(())
        }
    }
}
