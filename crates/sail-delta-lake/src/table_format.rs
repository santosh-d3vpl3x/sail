use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{not_impl_err, plan_err, DataFusionError, Result};
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::logical_expr::TableSource;
use datafusion::physical_plan::ExecutionPlan;
use futures::StreamExt;
use sail_common_datafusion::datasource::{
    MergeStrategy, OptionLayer, PhysicalSinkMode, RowLevelCommand, RowLevelWriteInfo, SinkInfo,
    SourceInfo, TableFormat, TableFormatRegistry, TableInitInfo,
};
use sail_common_datafusion::streaming::event::schema::is_flow_event_schema;
use sail_data_source::error::DataSourceResult;
use sail_data_source::options::gen::{
    DeltaReadOptions, DeltaReadPartialOptions, DeltaWriteOptions, DeltaWritePartialOptions,
};
use sail_data_source::options::{BuildPartialOptions, PartialOptions};
use sail_data_source::resolve_listing_urls;
use url::Url;

use crate::kernel::transaction::CommitBuilder;
use crate::kernel::{DeltaOperation, DeltaSnapshotConfig, SaveMode};
use crate::physical_plan::planner::{
    plan_delete, plan_merge, DeltaPhysicalPlanner, DeltaPlannerConfig, PlannerContext,
};
use crate::schema::build_initial_create_actions;
use crate::spec::{
    canonicalize_and_validate_table_properties, route_table_property_key, CommitAction,
};
use crate::storage::StorageConfig;
use crate::table::{open_table_with_object_store, open_table_with_object_store_and_table_config};
use crate::{create_delta_provider, create_delta_source, DeltaTableError};

/// Delta Lake implementation of [`TableFormat`].
#[derive(Debug)]
pub struct DeltaTableFormat;

impl DeltaTableFormat {
    pub fn register(registry: &TableFormatRegistry) -> Result<()> {
        registry.register(Arc::new(Self))?;
        Ok(())
    }
}

#[async_trait]
impl TableFormat for DeltaTableFormat {
    fn name(&self) -> &str {
        "delta"
    }

    async fn create_source(
        &self,
        ctx: &dyn Session,
        info: SourceInfo,
    ) -> Result<Arc<dyn TableSource>> {
        let SourceInfo {
            paths,
            schema,
            constraints: _,
            partition_by: _,
            bucket_by: _,
            sort_order: _,
            options,
        } = info;
        let table_url = Self::parse_table_url(ctx, paths).await?;
        let options = resolve_delta_read_options(options)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        create_delta_source(ctx, table_url, schema, options).await
    }

    async fn create_provider(
        &self,
        ctx: &dyn Session,
        info: SourceInfo,
    ) -> Result<Arc<dyn TableProvider>> {
        let SourceInfo {
            paths,
            schema,
            constraints: _,
            partition_by: _,
            bucket_by: _,
            sort_order: _,
            options,
        } = info;
        let table_url = Self::parse_table_url(ctx, paths).await?;
        let options = resolve_delta_read_options(options)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        create_delta_provider(ctx, table_url, schema, options).await
    }

    async fn create_writer(
        &self,
        ctx: &dyn Session,
        info: SinkInfo,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let path = info.path();
        let SinkInfo {
            input,
            mode,
            partition_by,
            bucket_by,
            sort_order,
            table_properties,
            options,
        } = info;

        if is_flow_event_schema(&input.schema()) {
            return not_impl_err!("writing streaming data to Delta table");
        }
        if bucket_by.is_some() {
            return not_impl_err!("bucketing for Delta format");
        }
        if partition_by.iter().any(|field| field.transform.is_some()) {
            return not_impl_err!("partition transforms for Delta format");
        }
        let partition_by = partition_by
            .into_iter()
            .map(|field| field.column)
            .collect::<Vec<_>>();

        let table_url = Self::parse_table_url(ctx, vec![path]).await?;
        let (options, routed_table_properties) =
            split_delta_write_options_and_table_properties(options);
        let delta_options = resolve_delta_write_options(options)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let object_store = ctx
            .runtime_env()
            .object_store_registry
            .get_store(&table_url)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let table = match open_table_with_object_store_and_table_config(
            table_url.clone(),
            object_store,
            Default::default(),
            // Only partition columns and table existence are needed at planning time;
            // skip replaying Add/Remove file actions which are not used here.
            DeltaSnapshotConfig {
                require_files: false,
                ..Default::default()
            },
        )
        .await
        {
            Ok(table) => Some(table),
            Err(DeltaTableError::InvalidTableLocation(_))
            | Err(DeltaTableError::FileNotFound(_)) => None,
            Err(err) => return Err(DataFusionError::External(Box::new(err))),
        };
        let table_exists = table.is_some();
        let mut metadata_configuration = resolve_delta_metadata_configuration(&table_properties)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        if table_exists {
            if !routed_table_properties.is_empty() {
                let mut keys: Vec<_> = routed_table_properties.keys().cloned().collect();
                keys.sort();
                log::warn!(
                    "ignoring write-time Delta table properties for existing table at {table_url}: {}",
                    keys.join(", ")
                );
            }
        } else {
            let routed_metadata_configuration =
                resolve_delta_metadata_configuration(&routed_table_properties)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
            metadata_configuration.extend(routed_metadata_configuration);
        }

        match mode {
            PhysicalSinkMode::ErrorIfExists if table_exists => {
                return plan_err!("Delta table already exists at path: {table_url}");
            }
            PhysicalSinkMode::IgnoreIfExists if table_exists => {
                return Ok(Arc::new(datafusion::physical_plan::empty::EmptyExec::new(
                    input.schema(),
                )));
            }
            PhysicalSinkMode::OverwritePartitions => {
                return not_impl_err!("unsupported sink mode for Delta: {mode:?}")
            }
            _ => {}
        }

        let unified_mode = mode;
        let table_schema_for_cond = None;

        // Get existing partition columns from table metadata if available
        let existing_partition_columns = if let Some(table) = &table {
            Some(
                table
                    .snapshot()
                    .map_err(|e| DataFusionError::External(Box::new(e)))?
                    .metadata()
                    .partition_columns()
                    .clone(),
            )
        } else {
            None
        };

        // Validate partition column mismatch for append/overwrite operations
        if let Some(existing_partitions) = &existing_partition_columns {
            if !partition_by.is_empty() && partition_by != *existing_partitions {
                // Allow partition column changes only when overwriting with schema changes
                // For append mode, this is always an error
                match unified_mode {
                    PhysicalSinkMode::Append => {
                        return plan_err!(
                            "Partition column mismatch. Table is partitioned by {:?}, but write specified {:?}. \
                            Cannot change partitioning on append.",
                            existing_partitions,
                            partition_by
                        );
                    }
                    PhysicalSinkMode::Overwrite | PhysicalSinkMode::OverwriteIf { .. }
                        // For overwrite mode, check if schema overwrite is allowed
                        if !delta_options.overwrite_schema => {
                            return plan_err!(
                                "Partition column mismatch. Table is partitioned by {:?}, but write specified {:?}. \
                                Set overwriteSchema=true to change partitioning.",
                                existing_partitions,
                                partition_by
                            );
                        }
                    _ => {}
                }
            }
        }

        let partition_columns = if !partition_by.is_empty() {
            partition_by
        } else {
            existing_partition_columns.unwrap_or_default()
        };

        let table_config = DeltaPlannerConfig::new(
            table_url,
            delta_options,
            metadata_configuration,
            partition_columns,
            table_schema_for_cond,
            table_exists,
        );
        let planner_ctx = PlannerContext::new(ctx, table_config);
        let planner = DeltaPhysicalPlanner::new(planner_ctx);
        let sink_exec = planner.create_plan(input, unified_mode, sort_order).await?;

        Ok(sink_exec)
    }

    async fn create_row_level_writer(
        &self,
        ctx: &dyn Session,
        info: RowLevelWriteInfo,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Strategy branching: all row-level operations check the materialization strategy
        // before delegating to the format-specific planner. MoR support will be implemented
        // here in the future; for now only Eager (Copy-on-Write) is supported.
        match info.merge_strategy {
            MergeStrategy::Eager => {}
            MergeStrategy::MergeOnRead => {
                return not_impl_err!(
                    "Merge-on-Read strategy is not yet implemented for Delta Lake"
                );
            }
        }

        match info.command {
            RowLevelCommand::Delete => {
                let table_url = Self::parse_table_url(ctx, vec![info.target.path]).await?;
                let condition = info.condition.ok_or_else(|| {
                    DataFusionError::Plan("DELETE operation requires a WHERE condition".to_string())
                })?;
                let delta_options = resolve_delta_write_options(info.target.options)?;
                let delete_config = DeltaPlannerConfig::new(
                    table_url,
                    delta_options,
                    HashMap::new(),
                    Vec::new(),
                    None,
                    true,
                );
                let delete_ctx = PlannerContext::new(ctx, delete_config);
                plan_delete(&delete_ctx, condition).await
            }
            RowLevelCommand::Merge => {
                let table_url = Self::parse_table_url(ctx, vec![info.target.path.clone()]).await?;
                let delta_options = resolve_delta_write_options(info.target.options.clone())?;
                let merge_config = DeltaPlannerConfig::new(
                    table_url,
                    delta_options,
                    HashMap::new(),
                    info.target.partition_by.clone(),
                    None,
                    true,
                );
                let merge_ctx = PlannerContext::new(ctx, merge_config);
                plan_merge(&merge_ctx, info).await
            }
            RowLevelCommand::Update => {
                not_impl_err!("UPDATE is not yet implemented for Delta Lake")
            }
        }
    }

    async fn initialize_table(&self, runtime: Arc<RuntimeEnv>, info: TableInitInfo) -> Result<()> {
        let TableInitInfo {
            location,
            schema,
            partition_columns,
            configuration,
            comment,
            if_not_exists,
            replace,
        } = info;

        if replace {
            return not_impl_err!("CREATE OR REPLACE TABLE is not yet implemented for Delta Lake");
        }

        // Mirrors Java `assertTableSchemaDefined`. Delta cannot commit a
        // version-0 Metadata action with an empty schema, so reject early
        // rather than producing a broken table.
        if schema.fields().is_empty() {
            return plan_err!("Cannot create Delta table with an empty schema at path: {location}");
        }

        let table_url = Self::parse_table_location(&location)?;
        let object_store = runtime
            .object_store_registry
            .get_store(&table_url)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        // Short-circuit when a Delta log already exists at the location.
        // For `IF NOT EXISTS`, verify that the existing table matches the
        // requested definition (mirrors Java `verifyTableMetadata`);
        // otherwise error out (mirrors Java `ErrorIfExists`).
        match open_table_with_object_store(
            table_url.clone(),
            Arc::clone(&object_store),
            StorageConfig,
        )
        .await
        {
            Ok(table) => {
                if if_not_exists {
                    verify_existing_table_matches(
                        &table,
                        schema.as_ref(),
                        &partition_columns,
                        &configuration,
                    )?;
                    return Ok(());
                }
                return plan_err!("Delta table already exists at path: {table_url}");
            }
            Err(DeltaTableError::InvalidTableLocation(_))
            | Err(DeltaTableError::FileNotFound(_)) => {}
            Err(err) => return Err(DataFusionError::External(Box::new(err))),
        }

        // Mirrors Java `assertPathEmpty`: refuse to adopt an arbitrary
        // directory as a fresh Delta table if it already has content other
        // than an (absent) `_delta_log/` directory.
        assert_path_empty(Arc::clone(&object_store), &table_url).await?;

        let table_name = Self::table_name_from_url(&table_url);

        let actions = build_initial_create_actions(
            schema.as_ref(),
            partition_columns,
            configuration,
            chrono::Utc::now().timestamp_millis(),
            table_name,
        )
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let mut metadata = actions.metadata;
        if let Some(desc) = comment {
            metadata = metadata.with_description(desc);
        }
        let protocol = actions.protocol;

        let log_store = crate::table::create_logstore_with_object_store(
            object_store,
            table_url.clone(),
            StorageConfig,
        )
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let operation = DeltaOperation::Create {
            mode: SaveMode::ErrorIfExists,
            location: table_url.to_string(),
            protocol: Box::new(protocol.clone()),
            metadata: Box::new(metadata.clone()),
        };

        CommitBuilder::default()
            .with_actions(vec![
                CommitAction::Protocol(protocol),
                CommitAction::Metadata(metadata),
            ])
            .build(None, log_store, operation)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        Ok(())
    }
}

impl DeltaTableFormat {
    /// Parse a raw `location` string into a `Url`. Supports both full URIs
    /// (e.g. `s3://…`, `file:///…`) and local filesystem paths (absolute or
    /// relative to the current working directory).
    fn parse_table_location(location: &str) -> Result<Url> {
        if let Ok(url) = Url::parse(location) {
            return Ok(url);
        }

        let path = Path::new(location);
        let path = if path.is_absolute() {
            PathBuf::from(path)
        } else {
            std::env::current_dir()
                .map_err(|e| DataFusionError::External(Box::new(e)))?
                .join(path)
        };

        Url::from_directory_path(&path)
            .map_err(|()| DataFusionError::Plan(format!("invalid directory path: {location}")))
    }

    /// Extract a human-readable table name from the URL's path, ignoring
    /// trailing empty segments (which `Url::from_directory_path` produces).
    fn table_name_from_url(url: &Url) -> Option<String> {
        url.path_segments()
            .and_then(|segments| segments.rev().find(|seg| !seg.is_empty()))
            .map(|seg| seg.to_string())
    }

    async fn parse_table_url(ctx: &dyn Session, paths: Vec<String>) -> Result<Url> {
        let mut urls = resolve_listing_urls(ctx, paths.clone()).await?;
        match (urls.pop(), urls.is_empty()) {
            (Some(path), true) => Ok(<ListingTableUrl as AsRef<Url>>::as_ref(&path).clone()),
            _ => plan_err!("expected a single path for Delta table sink: {paths:?}"),
        }
    }
}

pub fn resolve_delta_read_options(options: Vec<OptionLayer>) -> DataSourceResult<DeltaReadOptions> {
    let mut partial = DeltaReadPartialOptions::initialize();
    for layer in options {
        partial.merge(layer.build_partial_options()?);
    }
    partial.finalize()
}

pub fn resolve_delta_write_options(
    options: Vec<OptionLayer>,
) -> DataSourceResult<DeltaWriteOptions> {
    let mut partial = DeltaWritePartialOptions::initialize();
    for layer in options {
        partial.merge(layer.build_partial_options()?);
    }
    partial.finalize()
}

fn resolve_delta_metadata_configuration(
    table_properties: &HashMap<String, String>,
) -> crate::spec::DeltaResult<HashMap<String, String>> {
    canonicalize_and_validate_table_properties(
        table_properties
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str())),
    )
}

/// Ensure the directory backing a soon-to-be-created Delta table is free of
/// stray files. Entries under `_delta_log/` are ignored since those are
/// managed by Delta; any other object causes creation to be rejected.
async fn assert_path_empty(
    object_store: Arc<dyn object_store::ObjectStore>,
    table_url: &Url,
) -> Result<()> {
    let prefix = object_store::path::Path::from(table_url.path());
    let mut entries = object_store.list(Some(&prefix));
    while let Some(meta) = entries.next().await {
        let meta = match meta {
            Ok(meta) => meta,
            // `FileNotFound`/"no such directory" is fine — the target does
            // not exist yet, which is exactly what we want.
            Err(object_store::Error::NotFound { .. }) => return Ok(()),
            Err(err) => return Err(DataFusionError::External(Box::new(err))),
        };
        let location = meta.location.as_ref();
        if location.contains("/_delta_log/") || location.ends_with("/_delta_log") {
            continue;
        }
        return plan_err!(
            "Cannot create Delta table: location {table_url} is not empty (found {location})"
        );
    }
    Ok(())
}

/// Compare the existing Delta table with the requested definition and fail
/// loudly when they disagree. This matches the Java reference implementation's
/// `verifyTableMetadata` check used for `CREATE TABLE IF NOT EXISTS`.
fn verify_existing_table_matches(
    table: &crate::table::DeltaTable,
    requested_schema: &datafusion::arrow::datatypes::Schema,
    requested_partition_columns: &[String],
    requested_configuration: &HashMap<String, String>,
) -> Result<()> {
    let snapshot = table
        .snapshot()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    if snapshot.metadata().partition_columns() != requested_partition_columns {
        return plan_err!(
            "Partition column mismatch on CREATE TABLE IF NOT EXISTS: existing table is \
             partitioned by {:?} but request specified {:?}",
            snapshot.metadata().partition_columns(),
            requested_partition_columns
        );
    }

    let existing_schema = snapshot.schema();
    if existing_schema.fields().len() != requested_schema.fields().len() {
        return plan_err!(
            "Schema mismatch on CREATE TABLE IF NOT EXISTS: existing table has {} columns \
             but request specified {}",
            existing_schema.fields().len(),
            requested_schema.fields().len()
        );
    }
    for (existing, requested) in existing_schema
        .fields()
        .iter()
        .zip(requested_schema.fields().iter())
    {
        if existing.name() != requested.name() || existing.data_type() != requested.data_type() {
            return plan_err!(
                "Schema mismatch on CREATE TABLE IF NOT EXISTS: existing field {:?}:{} \
                 differs from requested {:?}:{}",
                existing.name(),
                existing.data_type(),
                requested.name(),
                requested.data_type()
            );
        }
    }

    // Compare only the user-visible `delta.*` configuration entries. Internal
    // bookkeeping keys such as `delta.columnMapping.maxColumnId` are derived
    // by `build_initial_create_actions` and would spuriously differ.
    let requested_canonical = canonicalize_and_validate_table_properties(
        requested_configuration
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str())),
    )
    .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let existing_configuration = snapshot.metadata().configuration();
    for (key, value) in &requested_canonical {
        if key == "delta.columnMapping.maxColumnId" {
            continue;
        }
        match existing_configuration.get(key) {
            Some(existing) if existing == value => {}
            Some(existing) => {
                return plan_err!(
                    "Configuration mismatch on CREATE TABLE IF NOT EXISTS: property {key} is \
                     {existing:?} in existing table but request specified {value:?}"
                );
            }
            None => {
                return plan_err!(
                    "Configuration mismatch on CREATE TABLE IF NOT EXISTS: property {key} is \
                     not set on existing table but request specified {value:?}"
                );
            }
        }
    }
    Ok(())
}

fn split_delta_write_options_and_table_properties(
    options: Vec<OptionLayer>,
) -> (Vec<OptionLayer>, HashMap<String, String>) {
    let mut table_properties = HashMap::new();
    let clean_options = options
        .into_iter()
        .map(|layer| match layer {
            OptionLayer::OptionList { items } => {
                let mut clean_items = Vec::with_capacity(items.len());
                for (key, value) in items {
                    if let Some(property_key) = route_table_property_key(&key) {
                        table_properties.insert(property_key, value);
                    } else {
                        clean_items.push((key, value));
                    }
                }
                OptionLayer::OptionList { items: clean_items }
            }
            OptionLayer::TablePropertyList { items } => {
                let mut clean_items = Vec::with_capacity(items.len());
                for (key, value) in items {
                    if let Some(property_key) = route_table_property_key(&key) {
                        table_properties.insert(property_key, value);
                    } else {
                        clean_items.push((key, value));
                    }
                }
                OptionLayer::TablePropertyList { items: clean_items }
            }
            other => other,
        })
        .collect();
    (clean_options, table_properties)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_delta_write_options_and_table_properties() {
        let options = vec![
            OptionLayer::OptionList {
                items: vec![
                    ("mergeSchema".to_string(), "true".to_string()),
                    ("column_mapping_mode".to_string(), "name".to_string()),
                ],
            },
            OptionLayer::OptionList {
                items: vec![
                    ("delta.appendOnly".to_string(), "true".to_string()),
                    ("targetFileSize".to_string(), "10".to_string()),
                ],
            },
        ];

        let (clean_options, table_properties) =
            split_delta_write_options_and_table_properties(options);

        assert_eq!(clean_options.len(), 2);
        match &clean_options[0] {
            OptionLayer::OptionList { items } => {
                assert_eq!(items, &[("mergeSchema".to_string(), "true".to_string())]);
            }
            _ => unreachable!("expected OptionList"),
        }
        match &clean_options[1] {
            OptionLayer::OptionList { items } => {
                assert_eq!(items, &[("targetFileSize".to_string(), "10".to_string())]);
            }
            _ => unreachable!("expected OptionList"),
        }
        assert_eq!(
            table_properties.get("delta.columnMapping.mode"),
            Some(&"name".to_string())
        );
        assert_eq!(
            table_properties.get("delta.appendOnly"),
            Some(&"true".to_string())
        );
    }

    #[expect(clippy::unwrap_used)]
    #[expect(clippy::expect_used)]
    mod initialize_table {
        use std::sync::Arc;

        use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
        use datafusion::execution::runtime_env::RuntimeEnv;
        use sail_common_datafusion::datasource::{TableFormat, TableInitInfo};
        use tempfile::TempDir;

        use super::super::{DeltaTableFormat, StorageConfig};
        use crate::table::open_table_with_object_store;

        fn local_file_url(dir: &std::path::Path, name: &str) -> String {
            let mut p = dir.to_path_buf();
            p.push(name);
            std::fs::create_dir_all(&p).unwrap();
            url::Url::from_directory_path(&p).unwrap().to_string()
        }

        fn simple_schema() -> Arc<ArrowSchema> {
            Arc::new(ArrowSchema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("value", DataType::Utf8, true),
            ]))
        }

        async fn load_table(location: &str) -> crate::table::DeltaTable {
            let url = url::Url::parse(location).unwrap();
            let object_store = object_store::local::LocalFileSystem::new();
            open_table_with_object_store(url, Arc::new(object_store), StorageConfig)
                .await
                .expect("open delta table")
        }

        #[tokio::test]
        async fn creates_initial_log_commit() {
            let tmp = TempDir::new().unwrap();
            let location = local_file_url(tmp.path(), "t");

            let info = TableInitInfo {
                location: location.clone(),
                schema: simple_schema(),
                partition_columns: vec![],
                configuration: Default::default(),
                comment: Some("a test table".to_string()),
                if_not_exists: false,
                replace: false,
            };

            let runtime = Arc::new(RuntimeEnv::default());
            DeltaTableFormat
                .initialize_table(runtime, info)
                .await
                .expect("initialize should succeed");

            // _delta_log/00000000000000000000.json must exist
            let log_path = url::Url::parse(&location)
                .unwrap()
                .to_file_path()
                .unwrap()
                .join("_delta_log")
                .join("00000000000000000000.json");
            assert!(log_path.exists(), "expected {}", log_path.display());

            // Table is loadable and has version 0 with matching schema.
            let table = load_table(&location).await;
            assert_eq!(table.version(), Some(0));
            let metadata = table.snapshot().unwrap().metadata();
            assert!(metadata.partition_columns().is_empty());
            assert_eq!(metadata.description(), Some("a test table"));
        }

        #[tokio::test]
        async fn creates_partitioned_table() {
            let tmp = TempDir::new().unwrap();
            let location = local_file_url(tmp.path(), "t_partitioned");

            let schema = Arc::new(ArrowSchema::new(vec![
                Field::new("part", DataType::Utf8, false),
                Field::new("value", DataType::Int64, true),
            ]));

            let info = TableInitInfo {
                location: location.clone(),
                schema,
                partition_columns: vec!["part".to_string()],
                configuration: Default::default(),
                comment: None,
                if_not_exists: false,
                replace: false,
            };

            let runtime = Arc::new(RuntimeEnv::default());
            DeltaTableFormat
                .initialize_table(runtime, info)
                .await
                .expect("initialize should succeed");

            let table = load_table(&location).await;
            assert_eq!(
                table.snapshot().unwrap().metadata().partition_columns(),
                &vec!["part".to_string()]
            );
        }

        #[tokio::test]
        async fn if_not_exists_is_idempotent() {
            let tmp = TempDir::new().unwrap();
            let location = local_file_url(tmp.path(), "t_inx");
            let runtime = Arc::new(RuntimeEnv::default());

            let make_info = || TableInitInfo {
                location: location.clone(),
                schema: simple_schema(),
                partition_columns: vec![],
                configuration: Default::default(),
                comment: None,
                if_not_exists: true,
                replace: false,
            };

            DeltaTableFormat
                .initialize_table(Arc::clone(&runtime), make_info())
                .await
                .expect("first call must succeed");
            DeltaTableFormat
                .initialize_table(Arc::clone(&runtime), make_info())
                .await
                .expect("second call with IF NOT EXISTS must be a no-op");

            // Still at version 0; no double-commit occurred.
            let table = load_table(&location).await;
            assert_eq!(table.version(), Some(0));
        }

        #[tokio::test]
        async fn errors_when_table_exists_without_if_not_exists() {
            let tmp = TempDir::new().unwrap();
            let location = local_file_url(tmp.path(), "t_err");
            let runtime = Arc::new(RuntimeEnv::default());

            let info = TableInitInfo {
                location: location.clone(),
                schema: simple_schema(),
                partition_columns: vec![],
                configuration: Default::default(),
                comment: None,
                if_not_exists: false,
                replace: false,
            };

            DeltaTableFormat
                .initialize_table(Arc::clone(&runtime), info.clone())
                .await
                .expect("first call must succeed");

            let err = DeltaTableFormat
                .initialize_table(runtime, info)
                .await
                .expect_err("second call must error");
            assert!(
                err.to_string().contains("already exists"),
                "unexpected error: {err}"
            );
        }

        #[tokio::test]
        async fn replace_is_not_yet_implemented() {
            let tmp = TempDir::new().unwrap();
            let location = local_file_url(tmp.path(), "t_replace");
            let runtime = Arc::new(RuntimeEnv::default());

            let info = TableInitInfo {
                location,
                schema: simple_schema(),
                partition_columns: vec![],
                configuration: Default::default(),
                comment: None,
                if_not_exists: false,
                replace: true,
            };

            let err = DeltaTableFormat
                .initialize_table(runtime, info)
                .await
                .expect_err("REPLACE should return not-implemented");
            assert!(
                err.to_string().to_lowercase().contains("replace"),
                "unexpected error: {err}"
            );
        }

        #[tokio::test]
        async fn column_mapping_matches_writer_path_shape() {
            use crate::spec::TableFeature;

            let tmp = TempDir::new().unwrap();
            let location = local_file_url(tmp.path(), "t_cm");

            let mut configuration = std::collections::HashMap::new();
            configuration.insert("delta.columnMapping.mode".to_string(), "name".to_string());
            configuration.insert(
                "delta.enableInCommitTimestamps".to_string(),
                "true".to_string(),
            );

            let info = TableInitInfo {
                location: location.clone(),
                schema: simple_schema(),
                partition_columns: vec![],
                configuration,
                comment: None,
                if_not_exists: false,
                replace: false,
            };

            let runtime = Arc::new(RuntimeEnv::default());
            DeltaTableFormat
                .initialize_table(runtime, info)
                .await
                .expect("initialize should succeed");

            let table = load_table(&location).await;
            let snapshot = table.snapshot().unwrap();
            let protocol = snapshot.protocol();
            assert!(protocol.has_reader_feature(&TableFeature::ColumnMapping));
            assert!(protocol.has_writer_feature(&TableFeature::ColumnMapping));
            assert!(protocol.has_writer_feature(&TableFeature::InCommitTimestamp));
            let metadata = snapshot.metadata();
            assert_eq!(
                metadata.configuration().get("delta.columnMapping.mode"),
                Some(&"name".to_string())
            );
            assert!(metadata
                .configuration()
                .contains_key("delta.columnMapping.maxColumnId"));
        }

        #[tokio::test]
        async fn rejects_empty_schema() {
            let tmp = TempDir::new().unwrap();
            let location = local_file_url(tmp.path(), "t_empty");
            let runtime = Arc::new(RuntimeEnv::default());

            let info = TableInitInfo {
                location,
                schema: Arc::new(ArrowSchema::empty()),
                partition_columns: vec![],
                configuration: Default::default(),
                comment: None,
                if_not_exists: false,
                replace: false,
            };

            let err = DeltaTableFormat
                .initialize_table(runtime, info)
                .await
                .expect_err("empty schema must be rejected");
            assert!(
                err.to_string().to_lowercase().contains("empty schema"),
                "unexpected error: {err}"
            );
        }

        #[tokio::test]
        async fn rejects_non_empty_directory() {
            let tmp = TempDir::new().unwrap();
            let location = local_file_url(tmp.path(), "t_dirty");
            // Drop a stray file in the target directory before we try to
            // initialize a Delta table there.
            let dir = url::Url::parse(&location).unwrap().to_file_path().unwrap();
            std::fs::write(dir.join("stale.parquet"), b"not empty").unwrap();

            let runtime = Arc::new(RuntimeEnv::default());
            let info = TableInitInfo {
                location: location.clone(),
                schema: simple_schema(),
                partition_columns: vec![],
                configuration: Default::default(),
                comment: None,
                if_not_exists: false,
                replace: false,
            };

            let err = DeltaTableFormat
                .initialize_table(runtime, info)
                .await
                .expect_err("non-empty location must be rejected");
            assert!(
                err.to_string().to_lowercase().contains("not empty"),
                "unexpected error: {err}"
            );
        }

        #[tokio::test]
        async fn if_not_exists_rejects_partition_mismatch() {
            let tmp = TempDir::new().unwrap();
            let location = local_file_url(tmp.path(), "t_if_parts");
            let runtime = Arc::new(RuntimeEnv::default());

            // First create with no partitioning.
            DeltaTableFormat
                .initialize_table(
                    Arc::clone(&runtime),
                    TableInitInfo {
                        location: location.clone(),
                        schema: Arc::new(ArrowSchema::new(vec![
                            Field::new("part", DataType::Utf8, false),
                            Field::new("value", DataType::Int64, true),
                        ])),
                        partition_columns: vec![],
                        configuration: Default::default(),
                        comment: None,
                        if_not_exists: false,
                        replace: false,
                    },
                )
                .await
                .expect("first call must succeed");

            // Second call asks for partitioning — should error under IF NOT EXISTS.
            let err = DeltaTableFormat
                .initialize_table(
                    runtime,
                    TableInitInfo {
                        location,
                        schema: Arc::new(ArrowSchema::new(vec![
                            Field::new("part", DataType::Utf8, false),
                            Field::new("value", DataType::Int64, true),
                        ])),
                        partition_columns: vec!["part".to_string()],
                        configuration: Default::default(),
                        comment: None,
                        if_not_exists: true,
                        replace: false,
                    },
                )
                .await
                .expect_err("mismatched partitioning must be rejected");
            assert!(
                err.to_string()
                    .to_lowercase()
                    .contains("partition column mismatch"),
                "unexpected error: {err}"
            );
        }

        #[tokio::test]
        async fn if_not_exists_rejects_schema_mismatch() {
            let tmp = TempDir::new().unwrap();
            let location = local_file_url(tmp.path(), "t_if_schema");
            let runtime = Arc::new(RuntimeEnv::default());

            DeltaTableFormat
                .initialize_table(
                    Arc::clone(&runtime),
                    TableInitInfo {
                        location: location.clone(),
                        schema: simple_schema(),
                        partition_columns: vec![],
                        configuration: Default::default(),
                        comment: None,
                        if_not_exists: false,
                        replace: false,
                    },
                )
                .await
                .expect("first call must succeed");

            let different = Arc::new(ArrowSchema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("value", DataType::Int64, true),
            ]));
            let err = DeltaTableFormat
                .initialize_table(
                    runtime,
                    TableInitInfo {
                        location,
                        schema: different,
                        partition_columns: vec![],
                        configuration: Default::default(),
                        comment: None,
                        if_not_exists: true,
                        replace: false,
                    },
                )
                .await
                .expect_err("mismatched schema must be rejected");
            assert!(
                err.to_string().to_lowercase().contains("schema mismatch"),
                "unexpected error: {err}"
            );
        }

        #[tokio::test]
        async fn if_not_exists_matches_when_equal() {
            let tmp = TempDir::new().unwrap();
            let location = local_file_url(tmp.path(), "t_if_equal");
            let runtime = Arc::new(RuntimeEnv::default());

            let make_info = || TableInitInfo {
                location: location.clone(),
                schema: simple_schema(),
                partition_columns: vec![],
                configuration: [("delta.appendOnly".to_string(), "true".to_string())]
                    .into_iter()
                    .collect(),
                comment: None,
                if_not_exists: true,
                replace: false,
            };

            DeltaTableFormat
                .initialize_table(Arc::clone(&runtime), make_info())
                .await
                .expect("first call must succeed");
            DeltaTableFormat
                .initialize_table(runtime, make_info())
                .await
                .expect("matching re-create must succeed");
        }
    }
}
