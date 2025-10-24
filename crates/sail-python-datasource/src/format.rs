// SPDX-License-Identifier: Apache-2.0

//! PythonDataSourceFormat - TableFormat implementation for generic Python data sources.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{not_impl_err, Result as DFResult};
use datafusion::physical_plan::ExecutionPlan;
use sail_common_datafusion::datasource::{DeleteInfo, SinkInfo, SourceInfo, TableFormat};

use crate::provider::PythonTableProvider;

/// Generic TableFormat for Python-based data sources.
///
/// This format allows users to implement data sources in Python and use them
/// with Lakesail's distributed query engine.
///
/// # Usage
///
/// ```python
/// spark.read.format("python") \
///     .option("python_module", "pysail.read.arrow_datasource") \
///     .option("python_class", "JDBCArrowDataSource") \
///     .option("url", "jdbc:postgresql://localhost/mydb") \
///     .option("dbtable", "users") \
///     .load()
/// ```
///
/// Any Python class that implements these methods can be used:
/// - `infer_schema(options: dict) -> pa.Schema`
/// - `plan_partitions(options: dict) -> List[dict]`
/// - `read_partition(partition_spec: dict, options: dict) -> Iterator[pa.RecordBatch]`
pub struct PythonDataSourceFormat;

impl PythonDataSourceFormat {
    pub fn new() -> Self {
        Self
    }
}

impl Default for PythonDataSourceFormat {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl TableFormat for PythonDataSourceFormat {
    fn name(&self) -> &str {
        "python"
    }

    async fn create_provider(
        &self,
        _ctx: &dyn Session,
        info: SourceInfo,
    ) -> DFResult<Arc<dyn TableProvider>> {
        // Merge all option sets (later sets override earlier ones)
        let mut merged_options = std::collections::HashMap::new();
        for option_set in &info.options {
            merged_options.extend(option_set.clone());
        }

        // Extract required options
        let module = merged_options
            .get("python_module")
            .ok_or_else(|| {
                datafusion::common::DataFusionError::Configuration(
                    "Missing required option 'python_module'".to_string(),
                )
            })?
            .clone();

        let class = merged_options
            .get("python_class")
            .ok_or_else(|| {
                datafusion::common::DataFusionError::Configuration(
                    "Missing required option 'python_class'".to_string(),
                )
            })?
            .clone();

        // Remove Python-specific options from what we pass to the datasource
        let mut datasource_options = merged_options.clone();
        datasource_options.remove("python_module");
        datasource_options.remove("python_class");

        // Create provider
        let provider = PythonTableProvider::try_new(module, class, datasource_options)?;

        Ok(Arc::new(provider))
    }

    async fn create_writer(
        &self,
        _ctx: &dyn Session,
        _info: SinkInfo,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        not_impl_err!("Writing to Python datasources is not yet implemented")
    }

    async fn create_deleter(
        &self,
        _ctx: &dyn Session,
        _info: DeleteInfo,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        not_impl_err!("Deleting from Python datasources is not yet implemented")
    }
}
