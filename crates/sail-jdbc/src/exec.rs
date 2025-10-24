// SPDX-License-Identifier: Apache-2.0

//! JDBC ExecutionPlan implementation for DataFusion.
//!
//! This is where distributed execution happens. DataFusion calls execute()
//! for each partition in parallel, and we read from the database.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use datafusion::arrow::datatypes::Schema;
use datafusion::common::{Result as DFResult, Statistics};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, Partitioning, PlanProperties,
    RecordBatchStream, SendableRecordBatchStream,
};
use datafusion_physical_expr::{EquivalenceProperties, PhysicalSortExpr};
use futures::{stream, Stream, StreamExt};

use crate::options::JDBCOptions;
use crate::partition::JDBCPartition;
use crate::reader;

/// JDBC ExecutionPlan - implements parallel JDBC reads
#[derive(Debug)]
pub struct JDBCExec {
    /// Arrow schema
    schema: SchemaRef,
    /// JDBC options
    options: JDBCOptions,
    /// Partition specifications
    partitions: Vec<JDBCPartition>,
    /// Execution properties
    properties: PlanProperties,
}

impl JDBCExec {
    pub fn new(schema: SchemaRef, options: JDBCOptions, partitions: Vec<JDBCPartition>) -> Self {
        let num_partitions = partitions.len();

        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(num_partitions),
            ExecutionMode::Bounded,
        );

        Self {
            schema,
            options,
            partitions,
            properties,
        }
    }
}

impl DisplayAs for JDBCExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "JDBCExec: url={}, partitions={}",
            self.options.url,
            self.partitions.len()
        )
    }
}

impl ExecutionPlan for JDBCExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        // Leaf node - no children
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(datafusion::common::DataFusionError::Internal(
                "JDBCExec should have no children".to_string(),
            ));
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        // Get the partition spec
        let partition_spec = self.partitions.get(partition).ok_or_else(|| {
            datafusion::common::DataFusionError::Execution(format!(
                "Invalid partition index: {}",
                partition
            ))
        })?;

        // Read partition synchronously (blocking)
        let batches = reader::read_partition(partition_spec, &self.options, &self.schema)
            .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?;

        // Create a stream from the batches
        let schema = self.schema.clone();
        let stream = stream::iter(batches.into_iter().map(Ok));

        Ok(Box::pin(JDBCRecordBatchStream {
            schema,
            inner: Box::pin(stream),
        }))
    }

    fn statistics(&self) -> DFResult<Statistics> {
        // Unknown statistics - could be improved by querying database
        Ok(Statistics::new_unknown(&self.schema))
    }
}

/// RecordBatchStream implementation for JDBC
struct JDBCRecordBatchStream {
    schema: SchemaRef,
    inner: std::pin::Pin<Box<dyn Stream<Item = DFResult<RecordBatch>> + Send>>,
}

impl Stream for JDBCRecordBatchStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.inner.poll_next_unpin(cx)
    }
}

impl RecordBatchStream for JDBCRecordBatchStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
