// SPDX-License-Identifier: Apache-2.0

//! JDBC TableFormat implementation for Lakesail.
//!
//! This is the entry point for JDBC data source registration with Lakesail.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::Result as DFResult;
use sail_common_datafusion::datasource::{
    DeleteInfo, SinkInfo, SourceInfo, TableFormat,
};

use crate::options::JDBCOptions;
use crate::provider::JDBCTableProvider;

/// JDBC TableFormat - registered with Lakesail's format registry
#[derive(Debug)]
pub struct JDBCFormat;

#[async_trait]
impl TableFormat for JDBCFormat {
    fn name(&self) -> &str {
        "jdbc"
    }

    async fn create_provider(
        &self,
        _ctx: &dyn Session,
        info: SourceInfo,
    ) -> DFResult<Arc<dyn TableProvider>> {
        let SourceInfo {
            paths: _,
            schema: _,
            constraints: _,
            partition_by: _,
            bucket_by: _,
            sort_order: _,
            options,
        } = info;

        // Merge all option sets into a single HashMap
        let merged_options = merge_options(options);

        // Parse JDBC options
        let jdbc_options = JDBCOptions::from_hashmap(&merged_options)
            .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?;

        // Create provider
        let provider = JDBCTableProvider::try_new(jdbc_options).await?;

        Ok(Arc::new(provider))
    }

    async fn create_writer(
        &self,
        _ctx: &dyn Session,
        _info: SinkInfo,
    ) -> DFResult<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        Err(datafusion::common::DataFusionError::NotImplemented(
            "JDBC write is not yet implemented".to_string(),
        ))
    }

    async fn create_deleter(
        &self,
        _ctx: &dyn Session,
        _info: DeleteInfo,
    ) -> DFResult<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        Err(datafusion::common::DataFusionError::NotImplemented(
            "JDBC delete is not yet implemented".to_string(),
        ))
    }
}

/// Merge multiple option sets into a single HashMap
/// Later options override earlier ones
fn merge_options(options_vec: Vec<HashMap<String, String>>) -> HashMap<String, String> {
    let mut merged = HashMap::new();

    for options in options_vec {
        for (key, value) in options {
            // Convert keys to lowercase for case-insensitive matching
            merged.insert(key.to_lowercase(), value);
        }
    }

    merged
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_merge_options() {
        let opts1 = HashMap::from([
            ("url".to_string(), "jdbc:postgresql://localhost/db1".to_string()),
            ("user".to_string(), "admin".to_string()),
        ]);

        let opts2 = HashMap::from([
            ("url".to_string(), "jdbc:postgresql://localhost/db2".to_string()),
            ("password".to_string(), "secret".to_string()),
        ]);

        let merged = merge_options(vec![opts1, opts2]);

        // Later options override earlier ones
        assert_eq!(merged.get("url"), Some(&"jdbc:postgresql://localhost/db2".to_string()));
        assert_eq!(merged.get("user"), Some(&"admin".to_string()));
        assert_eq!(merged.get("password"), Some(&"secret".to_string()));
    }
}
