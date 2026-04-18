use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_common::{exec_datafusion_err, exec_err, internal_err, Result};
use sail_catalog::command::CatalogCommand;
use sail_catalog::manager::CatalogManager;
use sail_catalog::provider::CreateTableOptions;
use sail_common_datafusion::datasource::{is_lakehouse_format, TableFormatRegistry, TableInitInfo};
use sail_common_datafusion::extension::SessionExtensionAccessor;

/// A physical plan node that executes a [`CatalogCommand`].
///
/// This node has a single output partition and no children.
/// When executed, it delegates to [`CatalogCommand::execute()`] using the [`TaskContext`]
/// to obtain both the [`CatalogManager`] and any session-level services.
#[derive(Debug, Clone)]
pub struct CatalogCommandExec {
    command: CatalogCommand,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl CatalogCommandExec {
    pub fn new(command: CatalogCommand, schema: SchemaRef) -> Self {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Self {
            command,
            schema,
            properties,
        }
    }

    pub fn command(&self) -> &CatalogCommand {
        &self.command
    }
}

impl DisplayAs for CatalogCommandExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "CatalogCommandExec: {}", self.command.name())
    }
}

impl ExecutionPlan for CatalogCommandExec {
    fn name(&self) -> &'static str {
        Self::static_name()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return internal_err!("{} should not have children", self.name());
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return exec_err!(
                "{} expects only partition 0 but got {}",
                self.name(),
                partition
            );
        }
        let command = self.command.clone();
        let schema = self.schema.clone();

        // Build the storage-init payload up front for CREATE TABLE so we don't
        // have to clone the entire `CreateTableOptions` across the `execute()`
        // call — only the data we actually need.
        let init_payload = match &command {
            CatalogCommand::CreateTable { options, .. } => {
                TableInitPayload::try_from_options(options)
            }
            _ => None,
        };

        let stream = futures::stream::once(async move {
            // Write the transaction log first, then register the table in
            // the catalog. If log initialization fails, we never touch the
            // catalog — this avoids orphaned catalog entries. If catalog
            // registration fails afterwards, the table is still path-
            // readable, matching the Java reference implementation's
            // `commitDeltaLog -> updateCatalog` ordering.
            if let Some(payload) = init_payload {
                initialize_lakehouse_table(context.as_ref(), payload).await?;
            }

            let manager = context.extension::<CatalogManager>()?;
            let batch = command
                .execute(context.as_ref(), manager.as_ref())
                .await
                .map_err(|e| exec_datafusion_err!("{e}"))?;

            Ok(batch)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

/// Minimal, already-materialized payload carried from the synchronous `execute`
/// call into the async stream. Avoids cloning the full `CreateTableOptions`.
struct TableInitPayload {
    format: String,
    location: String,
    schema: SchemaRef,
    partition_columns: Vec<String>,
    configuration: HashMap<String, String>,
    comment: Option<String>,
    if_not_exists: bool,
    replace: bool,
}

impl TableInitPayload {
    /// Returns `Some(payload)` if this CREATE TABLE needs format-specific
    /// storage initialization (i.e. lakehouse format and explicit `LOCATION`),
    /// or `None` otherwise.
    fn try_from_options(options: &CreateTableOptions) -> Option<Self> {
        if !is_lakehouse_format(&options.format) {
            return None;
        }
        let location = options.location.as_ref()?.clone();

        let fields: Vec<Field> = options
            .columns
            .iter()
            .map(|col| Field::new(&col.name, col.data_type.clone(), col.nullable))
            .collect();
        let schema: SchemaRef = Arc::new(Schema::new(fields));

        let partition_columns = options
            .partition_by
            .iter()
            .map(|p| p.column.clone())
            .collect();

        // Both `TBLPROPERTIES` (options.properties) and `OPTIONS (...)` entries
        // (options.options) may carry format-level configuration; pass both through
        // so the format can route / canonicalize them itself.
        let mut configuration: HashMap<String, String> =
            options.properties.iter().cloned().collect();
        for (key, value) in &options.options {
            configuration
                .entry(key.clone())
                .or_insert_with(|| value.clone());
        }

        Some(Self {
            format: options.format.clone(),
            location,
            schema,
            partition_columns,
            configuration,
            comment: options.comment.clone(),
            if_not_exists: options.if_not_exists,
            replace: options.replace,
        })
    }
}

/// Run any format-specific storage initialization after catalog registration.
/// Formats that do not override `initialize_table` will use the default no-op implementation.
async fn initialize_lakehouse_table(
    context: &TaskContext,
    payload: TableInitPayload,
) -> Result<()> {
    let TableInitPayload {
        format,
        location,
        schema,
        partition_columns,
        configuration,
        comment,
        if_not_exists,
        replace,
    } = payload;

    let registry = context.extension::<TableFormatRegistry>()?;
    let table_format = registry.get(&format)?;

    let init_info = TableInitInfo {
        location,
        schema,
        partition_columns,
        configuration,
        comment,
        if_not_exists,
        replace,
    };

    table_format
        .initialize_table(context.runtime_env(), init_info)
        .await
        .map_err(|e| exec_datafusion_err!("Failed to initialize table storage: {e}"))?;

    Ok(())
}
