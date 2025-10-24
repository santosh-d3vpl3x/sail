// SPDX-License-Identifier: Apache-2.0

//! JDBC TableProvider implementation for DataFusion.

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{Session, TableProvider, TableType};
use datafusion::common::{Result as DFResult, ToDFSchema};
use datafusion::datasource::TableProvider as _;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;

use crate::exec::JDBCExec;
use crate::options::JDBCOptions;
use crate::partition::plan_partitions;
use crate::reader;

/// JDBC TableProvider - represents a JDBC table/query
#[derive(Debug)]
pub struct JDBCTableProvider {
    /// Arrow schema
    schema: SchemaRef,
    /// JDBC options
    options: JDBCOptions,
}

impl JDBCTableProvider {
    pub async fn try_new(options: JDBCOptions) -> DFResult<Self> {
        // Validate options
        options
            .validate()
            .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?;

        // Infer schema from database
        let schema = reader::infer_schema(&options)
            .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?;

        Ok(Self { schema, options })
    }
}

#[async_trait]
impl TableProvider for JDBCTableProvider {
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

        // Plan partitions
        let partitions = plan_partitions(&self.options);

        // Create execution plan
        let exec = JDBCExec::new(schema, self.options.clone(), partitions);

        Ok(Arc::new(exec))
    }
}
