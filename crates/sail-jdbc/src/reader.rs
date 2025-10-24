// SPDX-License-Identifier: Apache-2.0

//! JDBC Arrow reader using Python bridge.
//!
//! This module provides JDBC → Arrow conversion by bridging to the Python
//! JDBCArrowDataSource implementation. This allows us to leverage the
//! existing battle-tested backends (ConnectorX, ADBC) while keeping the
//! Rust integration layer simple.
//!
//! Future: Replace with native Rust JDBC libraries for better performance.

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow_schema::Schema;
use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::error::{JDBCError, Result};
use crate::options::JDBCOptions;
use crate::partition::JDBCPartition;

/// Read Arrow schema from JDBC source
pub fn infer_schema(options: &JDBCOptions) -> Result<Arc<Schema>> {
    Python::with_gil(|py| {
        // Import Python datasource
        let pysail = py
            .import("pysail.read.arrow_datasource")
            .map_err(|e| JDBCError::PythonError(format!("failed to import pysail: {}", e)))?;

        let datasource_class = pysail
            .getattr("JDBCArrowDataSource")
            .map_err(|e| JDBCError::PythonError(format!("failed to get JDBCArrowDataSource: {}", e)))?;

        let datasource = datasource_class
            .call0()
            .map_err(|e| JDBCError::PythonError(format!("failed to create datasource: {}", e)))?;

        // Build options dict
        let opts = build_options_dict(py, options)?;

        // Call infer_schema()
        let schema_result = datasource
            .call_method1("infer_schema", (opts,))
            .map_err(|e| JDBCError::SchemaError(format!("failed to infer schema: {}", e)))?;

        // Convert PyArrow schema to Rust Arrow schema
        let schema = arrow::pyarrow::PyArrowType::<Schema>::try_from(schema_result)
            .map_err(|e| JDBCError::SchemaError(format!("failed to convert schema: {}", e)))?
            .0;

        Ok(Arc::new(schema))
    })
}

/// Read a single partition into Arrow RecordBatches
pub fn read_partition(
    partition: &JDBCPartition,
    options: &JDBCOptions,
    schema: &Schema,
) -> Result<Vec<RecordBatch>> {
    Python::with_gil(|py| {
        // Import Python datasource
        let pysail = py
            .import("pysail.read.arrow_datasource")
            .map_err(|e| JDBCError::PythonError(format!("failed to import pysail: {}", e)))?;

        let datasource_class = pysail
            .getattr("JDBCArrowDataSource")
            .map_err(|e| JDBCError::PythonError(format!("failed to get JDBCArrowDataSource: {}", e)))?;

        let datasource = datasource_class
            .call0()
            .map_err(|e| JDBCError::PythonError(format!("failed to create datasource: {}", e)))?;

        // Build options dict
        let opts = build_options_dict(py, options)?;

        // Build partition spec
        let partition_spec = PyDict::new(py);
        partition_spec.set_item("partition_id", partition.index)
            .map_err(|e| JDBCError::PythonError(e.to_string()))?;
        if let Some(pred) = &partition.predicate {
            partition_spec.set_item("predicate", pred)
                .map_err(|e| JDBCError::PythonError(e.to_string()))?;
        }

        // Call read_partition()
        let batches_iter = datasource
            .call_method1("read_partition", (partition_spec, opts))
            .map_err(|e| JDBCError::QueryError(format!("failed to read partition: {}", e)))?;

        // Convert iterator of PyArrow batches to Vec<RecordBatch>
        let mut batches = Vec::new();
        for batch_result in batches_iter.iter()
            .map_err(|e| JDBCError::QueryError(format!("failed to iterate batches: {}", e)))? {
            let batch_py = batch_result
                .map_err(|e| JDBCError::QueryError(format!("failed to get batch: {}", e)))?;

            let batch = arrow::pyarrow::PyArrowType::<RecordBatch>::try_from(batch_py)
                .map_err(|e| JDBCError::QueryError(format!("failed to convert batch: {}", e)))?
                .0;

            batches.push(batch);
        }

        Ok(batches)
    })
}

/// Build Python options dictionary from JDBCOptions
fn build_options_dict(py: Python, options: &JDBCOptions) -> Result<&PyDict> {
    let opts = PyDict::new(py);

    opts.set_item("url", &options.url)
        .map_err(|e| JDBCError::PythonError(e.to_string()))?;

    if let Some(dbtable) = &options.dbtable {
        opts.set_item("dbtable", dbtable)
            .map_err(|e| JDBCError::PythonError(e.to_string()))?;
    }

    if let Some(query) = &options.query {
        opts.set_item("query", query)
            .map_err(|e| JDBCError::PythonError(e.to_string()))?;
    }

    if let Some(user) = &options.user {
        opts.set_item("user", user)
            .map_err(|e| JDBCError::PythonError(e.to_string()))?;
    }

    if let Some(password) = &options.password {
        opts.set_item("password", password)
            .map_err(|e| JDBCError::PythonError(e.to_string()))?;
    }

    opts.set_item("engine", &options.engine)
        .map_err(|e| JDBCError::PythonError(e.to_string()))?;

    opts.set_item("fetchsize", options.fetch_size)
        .map_err(|e| JDBCError::PythonError(e.to_string()))?;

    Ok(opts)
}
