use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use arrow_pyarrow::{FromPyArrow, ToPyArrow};
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::memory::MemTable;
use datafusion::datasource::TableProvider;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::{not_impl_err, plan_err, DataFusionError, Result};
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3::PyObject;
use sail_common_datafusion::datasource::{SinkInfo, SourceInfo, TableFormat};

#[derive(Debug, Default)]
pub struct JdbcTableFormat;

#[async_trait]
impl TableFormat for JdbcTableFormat {
    fn name(&self) -> &str {
        "jdbc"
    }

    async fn create_provider(
        &self,
        _ctx: &dyn Session,
        info: SourceInfo,
    ) -> Result<Arc<dyn TableProvider>> {
        let SourceInfo {
            paths,
            schema,
            constraints: _,
            partition_by,
            bucket_by,
            sort_order,
            options,
        } = info;

        if !partition_by.is_empty() {
            return plan_err!("partition columns are not supported for JDBC sources");
        }
        if bucket_by.is_some() {
            return plan_err!("bucketing is not supported for JDBC sources");
        }
        if !sort_order.is_empty() {
            return plan_err!("sort order hints are not supported for JDBC sources");
        }

        let merged_options = merge_option_sets(options);
        let mut normalized = normalize_options(merged_options);

        let url = normalized
            .get("url")
            .or_else(|| normalized.get("uri"))
            .cloned()
            .ok_or_else(|| DataFusionError::Plan("JDBC option 'url' is required".to_string()))?;

        let table = normalized
            .get("dbtable")
            .or_else(|| normalized.get("table"))
            .cloned()
            .or_else(|| paths.first().cloned());
        let query = normalized.get("query").cloned();

        if table.is_none() && query.is_none() {
            return plan_err!("JDBC option 'dbtable' or 'query' is required");
        }

        let partition_config = build_partition_config(&mut normalized)?;

        let (mut partitions, table_schema) = fetch_batches(
            &url,
            table,
            query,
            &paths,
            schema.as_ref(),
            &normalized,
            partition_config,
        )
        .map_err(|err| DataFusionError::External(Box::new(err)))?;

        if partitions.is_empty() {
            partitions.push(vec![]);
        }

        let schema = Arc::new(table_schema);
        let table = MemTable::try_new(schema, partitions)?;
        Ok(Arc::new(table))
    }

    async fn create_writer(
        &self,
        _ctx: &dyn Session,
        _info: SinkInfo,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        not_impl_err!("writing to JDBC sources is not supported")
    }
}

fn merge_option_sets(option_sets: Vec<HashMap<String, String>>) -> HashMap<String, String> {
    let mut merged = HashMap::new();
    for options in option_sets {
        for (key, value) in options {
            merged.insert(key, value);
        }
    }
    merged
}

fn normalize_options(options: HashMap<String, String>) -> HashMap<String, String> {
    options
        .into_iter()
        .map(|(key, value)| (key.to_ascii_lowercase(), value))
        .collect()
}

#[derive(Debug)]
struct PartitionConfig {
    column: String,
    lower_bound: String,
    upper_bound: String,
    partitions: usize,
}

fn build_partition_config(
    options: &mut HashMap<String, String>,
) -> Result<Option<PartitionConfig>> {
    let column = options.remove("partitioncolumn");
    let lower = options.remove("lowerbound");
    let upper = options.remove("upperbound");
    let partitions = options.remove("numpartitions");

    match (column, lower, upper, partitions) {
        (None, None, None, None) => Ok(None),
        (Some(column), Some(lower), Some(upper), Some(partitions)) => {
            let partitions = partitions.parse::<usize>().map_err(|_| {
                DataFusionError::Plan("invalid JDBC option 'numPartitions'".to_string())
            })?;
            if partitions == 0 {
                return plan_err!("JDBC option 'numPartitions' must be greater than 0");
            }
            Ok(Some(PartitionConfig {
                column,
                lower_bound: lower,
                upper_bound: upper,
                partitions,
            }))
        }
        _ => plan_err!(
            "JDBC options 'partitionColumn', 'lowerBound', 'upperBound', and 'numPartitions' must be provided together"
        ),
    }
}

fn fetch_batches(
    url: &str,
    table: Option<String>,
    query: Option<String>,
    paths: &[String],
    schema: Option<&Schema>,
    options: &HashMap<String, String>,
    partition: Option<PartitionConfig>,
) -> PyResult<(Vec<Vec<RecordBatch>>, Schema)> {
    Python::with_gil(|py| {
        let module = PyModule::import(py, "pysail.db.jdbc")?;
        let kwargs = PyDict::new(py);
        kwargs.set_item("url", url)?;
        if let Some(table) = &table {
            kwargs.set_item("table", table)?;
        }
        if let Some(query) = &query {
            kwargs.set_item("query", query)?;
        }
        if !paths.is_empty() {
            kwargs.set_item("paths", paths)?;
        }

        if let Some(expected_schema) = schema {
            let py_schema = expected_schema.to_pyarrow(py)?;
            kwargs.set_item("schema", py_schema)?;
        }

        if let Some(partition) = &partition {
            let partition_dict = PyDict::new(py);
            partition_dict.set_item("column", &partition.column)?;
            partition_dict.set_item("lower_bound", &partition.lower_bound)?;
            partition_dict.set_item("upper_bound", &partition.upper_bound)?;
            partition_dict.set_item("num_partitions", partition.partitions)?;
            kwargs.set_item("partitioning", partition_dict)?;
        }

        let options_dict = PyDict::new(py);
        for (key, value) in options.iter() {
            options_dict.set_item(key, value)?;
        }
        kwargs.set_item("options", options_dict)?;

        let result = module.call_method("read_arrow_batches", (), Some(&kwargs))?;
        let (py_partitions, py_schema): (Vec<Vec<PyObject>>, PyObject) = result.extract()?;
        let schema = Schema::from_pyarrow_bound(&py_schema.into_bound(py))?;
        let partitions = py_partitions
            .into_iter()
            .map(|partition| {
                partition
                    .into_iter()
                    .map(|batch| {
                        let bound = batch.into_bound(py);
                        RecordBatch::from_pyarrow_bound(&bound)
                    })
                    .collect::<PyResult<Vec<RecordBatch>>>()
            })
            .collect::<PyResult<Vec<Vec<RecordBatch>>>>()?;
        Ok((partitions, schema))
    })
}
