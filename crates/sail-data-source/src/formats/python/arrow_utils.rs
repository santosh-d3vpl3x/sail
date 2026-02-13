//! Arrow conversion utilities for Python data sources.
//!
//! This module provides efficient conversion between Arrow and Python types
//! using the Arrow C Data Interface for zero-copy transfer.
//!
//! ## MVP Data Types (PR #1)
//!
//! - Numeric: Int32, Int64, Float32, Float64
//! - String: Utf8
//! - Boolean
//! - Temporal: Date32, Timestamp(Microsecond, None)
//! - Null
//!
//! Additional types are added in later PRs:
//! - PR #2: Binary, Decimal128, Int8, Int16
//! - PR #4: List<T>, Struct, Map<K,V>, LargeUtf8

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanArray, Date32Array, Float32Array, Float64Array, Int32Array, Int64Array,
    NullArray, RecordBatch, StringArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow_schema::SchemaRef;
use datafusion_common::{DataFusionError, Result};
use pyo3::prelude::*;
use pyo3::types::PyAnyMethods;

/// Convert a Python PyArrow RecordBatch to a Rust Arrow RecordBatch.
///
/// Uses the Arrow C Data Interface for zero-copy conversion.
pub fn py_record_batch_to_rust(
    _py: Python<'_>,
    py_batch: &Bound<'_, PyAny>,
) -> Result<RecordBatch> {
    use arrow_pyarrow::FromPyArrow;

    RecordBatch::from_pyarrow_bound(py_batch).map_err(|e| {
        DataFusionError::External(Box::new(std::io::Error::other(format!(
            "Failed to convert PyArrow RecordBatch: {}",
            e
        ))))
    })
}

/// Convert a Rust Arrow Schema to a Python PyArrow Schema.
pub fn rust_schema_to_py(py: Python<'_>, schema: &SchemaRef) -> Result<Py<PyAny>> {
    use arrow_pyarrow::ToPyArrow;

    ToPyArrow::to_pyarrow(schema.as_ref(), py)
        .map(|obj| obj.unbind())
        .map_err(|e| {
            DataFusionError::External(Box::new(std::io::Error::other(format!(
                "Failed to convert schema to PyArrow: {}",
                e
            ))))
        })
}

/// Convert a Python PyArrow Schema to a Rust Arrow Schema.
pub fn py_schema_to_rust(_py: Python<'_>, py_schema: &Bound<'_, PyAny>) -> Result<SchemaRef> {
    use arrow_pyarrow::FromPyArrow;

    let schema = Schema::from_pyarrow_bound(py_schema).map_err(|e| {
        DataFusionError::External(Box::new(std::io::Error::other(format!(
            "Failed to convert PyArrow Schema: {}",
            e
        ))))
    })?;

    Ok(Arc::new(schema))
}

/// Convert a Rust Arrow RecordBatch to a Python PyArrow RecordBatch.
///
/// Uses the Arrow C Data Interface for zero-copy conversion.
/// This is used for the Arrow-based write path (DataSourceArrowWriter).
pub fn rust_record_batch_to_py(py: Python<'_>, batch: &RecordBatch) -> Result<PyObject> {
    use arrow_pyarrow::ToPyArrow;

    batch.to_pyarrow(py).map_err(|e| {
        DataFusionError::External(Box::new(std::io::Error::other(format!(
            "Failed to convert RecordBatch to PyArrow: {}",
            e
        ))))
    })
}

/// Convert a Rust Arrow RecordBatch to Python Row objects.
///
/// This is used for the Row-based write path (DataSourceWriter).
/// Each row is converted to a PySpark Row object (tuple-like).
pub fn record_batch_to_py_rows(py: Python<'_>, batch: &RecordBatch) -> Result<Vec<PyObject>> {
    use pyo3::types::PyTuple;

    let num_rows = batch.num_rows();
    let num_cols = batch.num_columns();
    let mut rows = Vec::with_capacity(num_rows);

    for row_idx in 0..num_rows {
        let mut row_values = Vec::with_capacity(num_cols);

        for col_idx in 0..num_cols {
            let column = batch.column(col_idx);
            let value = extract_python_value(py, column, row_idx)?;
            row_values.push(value);
        }

        // Create a tuple for each row (PySpark Row is tuple-like)
        let row_tuple = PyTuple::new(py, row_values).map_err(py_err)?;
        rows.push(row_tuple.into());
    }

    Ok(rows)
}

/// Extract a Python value from an Arrow array at a given index.
fn extract_python_value(py: Python<'_>, array: &ArrayRef, row_idx: usize) -> Result<PyObject> {
    use arrow::array::Array;

    if array.is_null(row_idx) {
        return Ok(py.None());
    }

    match array.data_type() {
        DataType::Null => Ok(py.None()),
        DataType::Boolean => {
            let arr = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution("Failed to downcast to BooleanArray".to_string())
                })?;
            Ok(arr.value(row_idx).into_py(py))
        }
        DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
                DataFusionError::Execution("Failed to downcast to Int32Array".to_string())
            })?;
            Ok(arr.value(row_idx).into_py(py))
        }
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                DataFusionError::Execution("Failed to downcast to Int64Array".to_string())
            })?;
            Ok(arr.value(row_idx).into_py(py))
        }
        DataType::Float32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| {
                    DataFusionError::Execution("Failed to downcast to Float32Array".to_string())
                })?;
            Ok(arr.value(row_idx).into_py(py))
        }
        DataType::Float64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| {
                    DataFusionError::Execution("Failed to downcast to Float64Array".to_string())
                })?;
            Ok(arr.value(row_idx).into_py(py))
        }
        DataType::Utf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution("Failed to downcast to StringArray".to_string())
                })?;
            Ok(arr.value(row_idx).into_py(py))
        }
        DataType::Date32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| {
                    DataFusionError::Execution("Failed to downcast to Date32Array".to_string())
                })?;
            Ok(arr.value(row_idx).into_py(py))
        }
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution(
                        "Failed to downcast to TimestampMicrosecondArray".to_string(),
                    )
                })?;
            Ok(arr.value(row_idx).into_py(py))
        }
        other => Err(DataFusionError::NotImplemented(format!(
            "Data type {:?} not supported in MVP. Available in later PRs.",
            other
        ))),
    }
}

/// Validate that two schemas are compatible.
///
/// Checks field names and types match. Field metadata is ignored.
pub fn validate_schema(expected: &SchemaRef, actual: &SchemaRef) -> Result<()> {
    if expected.fields().len() != actual.fields().len() {
        return Err(DataFusionError::Execution(format!(
            "Schema field count mismatch: expected {} fields, got {}",
            expected.fields().len(),
            actual.fields().len()
        )));
    }

    for (i, (expected_field, actual_field)) in expected
        .fields()
        .iter()
        .zip(actual.fields().iter())
        .enumerate()
    {
        if expected_field.name() != actual_field.name() {
            return Err(DataFusionError::Execution(format!(
                "Schema field name mismatch at position {}: expected '{}', got '{}'",
                i,
                expected_field.name(),
                actual_field.name()
            )));
        }

        if expected_field.data_type() != actual_field.data_type() {
            return Err(DataFusionError::Execution(format!(
                "Schema field type mismatch for '{}': expected {:?}, got {:?}",
                expected_field.name(),
                expected_field.data_type(),
                actual_field.data_type()
            )));
        }
    }

    Ok(())
}

/// Check if a data type is supported in the MVP.
pub fn is_mvp_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Null
            | DataType::Boolean
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64
            | DataType::Utf8
            | DataType::Date32
            | DataType::Timestamp(TimeUnit::Microsecond, None)
    )
}

/// Convert pickled Python rows to a RecordBatch.
///
/// This is used for efficient multi-row batching when Python yields tuples.
pub fn convert_rows_to_batch(schema: &SchemaRef, pickled_rows: &[Vec<u8>]) -> Result<RecordBatch> {
    use pyo3::types::PyBytes;

    if pickled_rows.is_empty() {
        return Ok(RecordBatch::new_empty(schema.clone()));
    }

    Python::attach(|py| {
        let cloudpickle = import_cloudpickle(py)?;

        // Unpickle all rows
        let rows: Vec<Bound<'_, PyAny>> = pickled_rows
            .iter()
            .map(|pickled| {
                let bytes = PyBytes::new(py, pickled);
                cloudpickle.call_method1("loads", (bytes,)).map_err(py_err)
            })
            .collect::<Result<_>>()?;

        // Build arrays for each column
        let arrays: Vec<ArrayRef> = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(col_idx, field)| build_array_from_rows(py, &rows, col_idx, field))
            .collect::<Result<_>>()?;

        RecordBatch::try_new(schema.clone(), arrays).map_err(|e| {
            DataFusionError::External(Box::new(std::io::Error::other(format!(
                "Failed to create RecordBatch: {}",
                e
            ))))
        })
    })
}

/// Macro to reduce boilerplate in build_array_from_rows.
///
/// Generates the common pattern of extracting values from rows and building an array.
macro_rules! build_primitive_array {
    ($rows:expr, $col_idx:expr, $rust_ty:ty, $array_ty:ty) => {{
        let values: Vec<Option<$rust_ty>> = $rows
            .iter()
            .map(|row| extract_value(row, $col_idx))
            .collect::<Result<_>>()?;
        Ok(Arc::new(<$array_ty>::from(values)))
    }};
}

/// Build an Arrow array from Python row values.
fn build_array_from_rows(
    _py: Python<'_>,
    rows: &[Bound<'_, PyAny>],
    col_idx: usize,
    field: &Arc<Field>,
) -> Result<ArrayRef> {
    match field.data_type() {
        DataType::Null => Ok(Arc::new(NullArray::new(rows.len()))),
        DataType::Boolean => build_primitive_array!(rows, col_idx, bool, BooleanArray),
        DataType::Int32 => build_primitive_array!(rows, col_idx, i32, Int32Array),
        DataType::Int64 => build_primitive_array!(rows, col_idx, i64, Int64Array),
        DataType::Float32 => build_primitive_array!(rows, col_idx, f32, Float32Array),
        DataType::Float64 => build_primitive_array!(rows, col_idx, f64, Float64Array),
        DataType::Utf8 => {
            let values: Vec<Option<String>> = rows
                .iter()
                .map(|row| extract_value(row, col_idx))
                .collect::<Result<_>>()?;
            Ok(Arc::new(StringArray::from(
                values.iter().map(|v| v.as_deref()).collect::<Vec<_>>(),
            )))
        }
        DataType::Date32 => build_primitive_array!(rows, col_idx, i32, Date32Array),
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            build_primitive_array!(rows, col_idx, i64, TimestampMicrosecondArray)
        }
        other => Err(DataFusionError::NotImplemented(format!(
            "Data type {:?} not supported in MVP. Available in later PRs.",
            other
        ))),
    }
}

/// Extract a value from a Python row tuple.
fn extract_value<'py, T: pyo3::FromPyObject<'py>>(
    row: &Bound<'py, PyAny>,
    col_idx: usize,
) -> Result<Option<T>> {
    let item = row.get_item(col_idx).map_err(py_err)?;

    if item.is_none() {
        return Ok(None);
    }

    item.extract::<T>().map(Some).map_err(py_err)
}

/// Re-export py_err and import_cloudpickle from error module.
use super::error::{import_cloudpickle, py_err};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_mvp_type() {
        assert!(is_mvp_type(&DataType::Int32));
        assert!(is_mvp_type(&DataType::Int64));
        assert!(is_mvp_type(&DataType::Float32));
        assert!(is_mvp_type(&DataType::Float64));
        assert!(is_mvp_type(&DataType::Utf8));
        assert!(is_mvp_type(&DataType::Boolean));
        assert!(is_mvp_type(&DataType::Date32));
        assert!(is_mvp_type(&DataType::Timestamp(
            TimeUnit::Microsecond,
            None
        )));
        assert!(is_mvp_type(&DataType::Null));

        // Not in MVP
        assert!(!is_mvp_type(&DataType::Binary));
        assert!(!is_mvp_type(&DataType::Int8));
        assert!(!is_mvp_type(&DataType::List(Arc::new(Field::new(
            "item",
            DataType::Int32,
            true
        )))));
    }

    #[test]
    fn test_validate_schema_matching() {
        let schema1 = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let schema2 = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        assert!(validate_schema(&schema1, &schema2).is_ok());
    }

    #[test]
    fn test_validate_schema_field_count_mismatch() {
        let schema1 = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        let schema2 = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        assert!(validate_schema(&schema1, &schema2).is_err());
    }

    #[test]
    fn test_validate_schema_type_mismatch() {
        let schema1 = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let schema2 = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

        assert!(validate_schema(&schema1, &schema2).is_err());
    }

    #[test]
    fn test_rust_record_batch_to_py() {
        pyo3::prepare_freethreaded_python();

        Python::with_gil(|py| {
            // Create a simple RecordBatch
            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("value", DataType::Float64, true),
            ]));

            let id_array = Int32Array::from(vec![1, 2, 3]);
            let value_array = Float64Array::from(vec![Some(1.5), None, Some(3.5)]);

            let batch =
                RecordBatch::try_new(schema, vec![Arc::new(id_array), Arc::new(value_array)])
                    .unwrap();

            // Convert to Python
            let py_batch = rust_record_batch_to_py(py, &batch);
            assert!(py_batch.is_ok());

            // Verify the Python object has the expected properties
            let py_obj = py_batch.unwrap();
            let num_rows: usize = py_obj.getattr(py, "num_rows").unwrap().extract(py).unwrap();
            assert_eq!(num_rows, 3);
        });
    }

    #[test]
    fn test_record_batch_to_py_rows() {
        pyo3::prepare_freethreaded_python();

        Python::with_gil(|py| {
            // Create a simple RecordBatch
            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("name", DataType::Utf8, true),
            ]));

            let id_array = Int32Array::from(vec![1, 2]);
            let name_array = StringArray::from(vec![Some("Alice"), Some("Bob")]);

            let batch =
                RecordBatch::try_new(schema, vec![Arc::new(id_array), Arc::new(name_array)])
                    .unwrap();

            // Convert to Python rows
            let rows = record_batch_to_py_rows(py, &batch);
            assert!(rows.is_ok());

            let rows_vec = rows.unwrap();
            assert_eq!(rows_vec.len(), 2);

            // Verify first row
            let row0 = rows_vec[0].bind(py);
            assert_eq!(row0.len().unwrap(), 2);
            let id0: i32 = row0.get_item(0).unwrap().extract().unwrap();
            let name0: String = row0.get_item(1).unwrap().extract().unwrap();
            assert_eq!(id0, 1);
            assert_eq!(name0, "Alice");

            // Verify second row
            let row1 = rows_vec[1].bind(py);
            let id1: i32 = row1.get_item(0).unwrap().extract().unwrap();
            let name1: String = row1.get_item(1).unwrap().extract().unwrap();
            assert_eq!(id1, 2);
            assert_eq!(name1, "Bob");
        });
    }

    #[test]
    fn test_record_batch_to_py_rows_with_nulls() {
        pyo3::prepare_freethreaded_python();

        Python::with_gil(|py| {
            // Create a RecordBatch with nulls
            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("value", DataType::Float64, true),
            ]));

            let id_array = Int32Array::from(vec![1, 2]);
            let value_array = Float64Array::from(vec![Some(1.5), None]);

            let batch =
                RecordBatch::try_new(schema, vec![Arc::new(id_array), Arc::new(value_array)])
                    .unwrap();

            // Convert to Python rows
            let rows = record_batch_to_py_rows(py, &batch);
            assert!(rows.is_ok());

            let rows_vec = rows.unwrap();
            assert_eq!(rows_vec.len(), 2);

            // Verify second row has null value
            let row1 = rows_vec[1].bind(py);
            let value1 = row1.get_item(1).unwrap();
            assert!(value1.is_none());
        });
    }
}
