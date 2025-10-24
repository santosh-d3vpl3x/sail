// SPDX-License-Identifier: Apache-2.0

//! JDBC data source for Lakesail using Apache Arrow DataFusion.
//!
//! This crate provides JDBC connectivity for Lakesail's Spark Connect server.
//! It implements DataFusion's TableFormat trait to integrate seamlessly with
//! the query engine.
//!
//! # Architecture
//!
//! ```text
//! Client Query
//!   ↓
//! JDBCFormat (TableFormat)
//!   ↓
//! JDBCTableProvider (TableProvider)
//!   ↓
//! JDBCExec (ExecutionPlan)
//!   ↓
//! Parallel Partition Reads
//!   ↓
//! Arrow RecordBatches
//! ```
//!
//! # Implementation Strategy
//!
//! Phase 1 (Current): Bridge to Python via PyO3
//! - Leverage existing Python JDBCArrowDataSource
//! - Quick to implement, battle-tested backends (ConnectorX, ADBC)
//! - Slight performance overhead from Python interop
//!
//! Phase 2 (Future): Native Rust implementation
//! - Pure Rust JDBC libraries
//! - Maximum performance
//! - No Python dependency

pub mod error;
pub mod exec;
pub mod format;
pub mod options;
pub mod partition;
pub mod provider;
pub mod reader;

// Re-exports
pub use error::{JDBCError, Result};
pub use format::JDBCFormat;
pub use provider::JDBCTableProvider;
