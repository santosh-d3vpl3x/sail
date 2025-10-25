// SPDX-License-Identifier: Apache-2.0

//! PythonTableProvider - Generic TableProvider for Python data sources.

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{Session, TableProvider, TableType};
use datafusion::common::{Result as DFResult, ToDFSchema};
use datafusion::datasource::TableProvider as _;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use serde_json::Value as JsonValue;

use crate::error::{PythonDataSourceError, Result};
use crate::exec::PythonExec;

/// Generic TableProvider for any Python data source.
///
/// This provider works with ANY Python class that implements:
/// - infer_schema(options: dict) -> pa.Schema
/// - plan_partitions(options: dict) -> List[dict]
/// - read_partition(partition_spec: dict, options: dict) -> Iterator[pa.RecordBatch]
#[derive(Debug)]
pub struct PythonTableProvider {
    /// Python module name (e.g., "pysail.read.arrow_datasource")
    module: String,
    /// Python class name (e.g., "JDBCArrowDataSource")
    class: String,
    /// Arrow schema
    schema: SchemaRef,
    /// User options (passed to Python methods)
    options: HashMap<String, String>,
}

impl PythonTableProvider {
    /// Create a new PythonTableProvider.
    ///
    /// # Arguments
    ///
    /// * `module` - Python module name (e.g., "pysail.read.arrow_datasource")
    /// * `class` - Python class name (e.g., "JDBCArrowDataSource")
    /// * `options` - User options to pass to Python datasource
    pub fn try_new(
        module: String,
        class: String,
        options: HashMap<String, String>,
    ) -> DFResult<Self> {
        // Infer schema from Python
        let schema = infer_schema_from_python(&module, &class, &options)
            .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?;

        Ok(Self {
            module,
            class,
            schema,
            options,
        })
    }
}

#[async_trait]
impl TableProvider for PythonTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _session: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        // Apply projection to schema if provided
        let schema = if let Some(projection) = projection {
            let projected_schema = self.schema.project(projection)?;
            Arc::new(projected_schema)
        } else {
            self.schema.clone()
        };

        // Plan partitions via Python
        let partitions = plan_partitions_from_python(&self.module, &self.class, &self.options)
            .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?;

        // Convert options to JSON
        let options_json = serde_json::to_value(&self.options).map_err(|e| {
            datafusion::common::DataFusionError::External(Box::new(
                PythonDataSourceError::ExecutionError(format!(
                    "Failed to serialize options: {}",
                    e
                )),
            ))
        })?;

        // Create execution plan
        let exec = PythonExec::new(
            self.module.clone(),
            self.class.clone(),
            schema,
            partitions,
            options_json,
        );

        Ok(Arc::new(exec))
    }
}

/// Infer Arrow schema from Python datasource
fn infer_schema_from_python(
    module: &str,
    class: &str,
    options: &HashMap<String, String>,
) -> Result<SchemaRef> {
    Python::with_gil(|py| {
        // Import Python module
        let py_module = py.import(module).map_err(|e| {
            PythonDataSourceError::ImportError(format!("Failed to import {}: {}", module, e))
        })?;

        // Get class
        let py_class = py_module.getattr(class).map_err(|e| {
            PythonDataSourceError::ImportError(format!("Failed to get class {}: {}", class, e))
        })?;

        // Instantiate datasource
        let datasource = py_class.call0().map_err(|e| {
            PythonDataSourceError::ExecutionError(format!("Failed to instantiate {}: {}", class, e))
        })?;

        // Convert options to Python dict
        let options_dict = PyDict::new(py);
        for (key, value) in options {
            options_dict
                .set_item(key, value)
                .map_err(|e| PythonDataSourceError::ExecutionError(e.to_string()))?;
        }

        // Call infer_schema()
        let py_schema = datasource
            .call_method1("infer_schema", (options_dict,))
            .map_err(|e| {
                PythonDataSourceError::SchemaError(format!("infer_schema() failed: {}", e))
            })?;

        // Convert PyArrow schema to Rust Arrow schema (zero-copy via FFI!)
        let schema = arrow::pyarrow::PyArrowType::<arrow::datatypes::Schema>::try_from(py_schema)
            .map_err(|e| {
                PythonDataSourceError::SchemaError(format!("Failed to convert schema: {}", e))
            })?
            .0;

        Ok(Arc::new(schema))
    })
}

/// Plan partitions via Python datasource
fn plan_partitions_from_python(
    module: &str,
    class: &str,
    options: &HashMap<String, String>,
) -> Result<Vec<JsonValue>> {
    Python::with_gil(|py| {
        // Import Python module
        let py_module = py.import(module).map_err(|e| {
            PythonDataSourceError::ImportError(format!("Failed to import {}: {}", module, e))
        })?;

        // Get class
        let py_class = py_module.getattr(class).map_err(|e| {
            PythonDataSourceError::ImportError(format!("Failed to get class {}: {}", class, e))
        })?;

        // Instantiate datasource
        let datasource = py_class.call0().map_err(|e| {
            PythonDataSourceError::ExecutionError(format!("Failed to instantiate {}: {}", class, e))
        })?;

        // Convert options to Python dict
        let options_dict = PyDict::new(py);
        for (key, value) in options {
            options_dict
                .set_item(key, value)
                .map_err(|e| PythonDataSourceError::ExecutionError(e.to_string()))?;
        }

        // Call plan_partitions()
        let py_partitions = datasource
            .call_method1("plan_partitions", (options_dict,))
            .map_err(|e| {
                PythonDataSourceError::PartitionError(format!("plan_partitions() failed: {}", e))
            })?;

        // Convert Python list to Vec<JsonValue>
        let mut partitions = Vec::new();
        for py_partition_result in py_partitions
            .iter()
            .map_err(|e| PythonDataSourceError::PartitionError(e.to_string()))?
        {
            let py_partition = py_partition_result
                .map_err(|e| PythonDataSourceError::PartitionError(e.to_string()))?;

            // Convert Python dict to JSON
            let json_str = py_partition
                .call_method0("__repr__")
                .map_err(|e| PythonDataSourceError::PartitionError(e.to_string()))?
                .extract::<String>()
                .map_err(|e| PythonDataSourceError::PartitionError(e.to_string()))?;

            // Use pythonize for proper Python->Rust conversion
            let partition_value: JsonValue = pythonize::depythonize(py_partition).map_err(|e| {
                PythonDataSourceError::PartitionError(format!("Failed to convert partition: {}", e))
            })?;

            partitions.push(partition_value);
        }

        Ok(partitions)
    })
}
