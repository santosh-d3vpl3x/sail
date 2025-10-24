// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::error::{JDBCError, Result};

/// JDBC connection and read options
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JDBCOptions {
    /// JDBC URL (e.g., "jdbc:postgresql://localhost:5432/mydb")
    pub url: String,

    /// Table name or subquery
    pub dbtable: Option<String>,

    /// Custom SQL query (alternative to dbtable)
    pub query: Option<String>,

    /// Database user
    pub user: Option<String>,

    /// Database password
    pub password: Option<String>,

    /// Backend engine: "connectorx", "adbc", or "fallback"
    pub engine: String,

    /// Partition column for parallel reads
    pub partition_column: Option<String>,

    /// Lower bound for partitioning
    pub lower_bound: Option<i64>,

    /// Upper bound for partitioning
    pub upper_bound: Option<i64>,

    /// Number of partitions
    pub num_partitions: usize,

    /// Fetch size for batching
    pub fetch_size: usize,

    /// Explicit partition predicates (comma-separated)
    pub predicates: Option<String>,
}

impl Default for JDBCOptions {
    fn default() -> Self {
        Self {
            url: String::new(),
            dbtable: None,
            query: None,
            user: None,
            password: None,
            engine: "connectorx".to_string(),
            partition_column: None,
            lower_bound: None,
            upper_bound: None,
            num_partitions: 1,
            fetch_size: 10000,
            predicates: None,
        }
    }
}

impl JDBCOptions {
    /// Parse JDBC options from a hashmap
    pub fn from_hashmap(options: &HashMap<String, String>) -> Result<Self> {
        let mut jdbc_opts = Self::default();

        // Required: URL
        jdbc_opts.url = options
            .get("url")
            .ok_or_else(|| JDBCError::InvalidOptions("missing 'url' option".to_string()))?
            .clone();

        // Either dbtable or query required
        jdbc_opts.dbtable = options.get("dbtable").cloned();
        jdbc_opts.query = options.get("query").cloned();

        if jdbc_opts.dbtable.is_none() && jdbc_opts.query.is_none() {
            return Err(JDBCError::InvalidOptions(
                "either 'dbtable' or 'query' must be specified".to_string(),
            ));
        }

        if jdbc_opts.dbtable.is_some() && jdbc_opts.query.is_some() {
            return Err(JDBCError::InvalidOptions(
                "cannot specify both 'dbtable' and 'query'".to_string(),
            ));
        }

        // Optional credentials
        jdbc_opts.user = options.get("user").cloned();
        jdbc_opts.password = options.get("password").cloned();

        // Optional engine
        if let Some(engine) = options.get("engine") {
            jdbc_opts.engine = engine.clone();
        }

        // Optional partitioning
        jdbc_opts.partition_column = options.get("partitioncolumn").cloned();
        jdbc_opts.lower_bound = options
            .get("lowerbound")
            .and_then(|s| s.parse().ok());
        jdbc_opts.upper_bound = options
            .get("upperbound")
            .and_then(|s| s.parse().ok());

        if let Some(num_parts) = options.get("numpartitions") {
            jdbc_opts.num_partitions = num_parts.parse().map_err(|_| {
                JDBCError::InvalidOptions(format!("invalid numPartitions: {}", num_parts))
            })?;
        }

        if let Some(fetch_size) = options.get("fetchsize") {
            jdbc_opts.fetch_size = fetch_size.parse().map_err(|_| {
                JDBCError::InvalidOptions(format!("invalid fetchSize: {}", fetch_size))
            })?;
        }

        jdbc_opts.predicates = options.get("predicates").cloned();

        Ok(jdbc_opts)
    }

    /// Validate options
    pub fn validate(&self) -> Result<()> {
        if self.url.is_empty() {
            return Err(JDBCError::InvalidOptions("empty URL".to_string()));
        }

        if self.num_partitions == 0 {
            return Err(JDBCError::InvalidOptions(
                "numPartitions must be > 0".to_string(),
            ));
        }

        // If partition column specified, must have bounds
        if self.partition_column.is_some() {
            if self.lower_bound.is_none() || self.upper_bound.is_none() {
                return Err(JDBCError::InvalidOptions(
                    "partitionColumn requires lowerBound and upperBound".to_string(),
                ));
            }
        }

        // Cannot have both partition_column and predicates
        if self.partition_column.is_some() && self.predicates.is_some() {
            return Err(JDBCError::InvalidOptions(
                "cannot specify both partitionColumn and predicates".to_string(),
            ));
        }

        Ok(())
    }
}
