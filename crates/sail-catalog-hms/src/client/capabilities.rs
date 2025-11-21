//! HMS capability detection based on version

use super::version::HmsVersion;
use std::collections::HashSet;

/// HMS capabilities detected from version
#[derive(Debug, Clone)]
pub struct Capabilities {
    /// HMS version
    pub version: HmsVersion,

    /// Supported features
    pub features: HashSet<HmsFeature>,
}

/// HMS features that may or may not be available
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub enum HmsFeature {
    // Core features (all versions)
    /// Basic database operations
    Databases,
    /// Basic table operations
    Tables,
    /// Partition operations
    Partitions,

    // HMS 2.0+ features
    /// Lock service for concurrency control
    LockService,

    // HMS 2.3+ features
    /// Table statistics support
    TableStatistics,
    /// Bulk operations (get_tables_by_name, etc.)
    BulkOperations,
    /// Partition batching
    PartitionBatching,

    // HMS 3.0+ features
    /// ACID transaction support
    AcidTransactions,
    /// Materialized views
    MaterializedViews,
    /// Compaction history
    CompactionHistory,
    /// Schema registry support (Confluent extension)
    SchemaRegistry,

    // HMS 3.1+ features
    /// Table constraints (primary key, foreign key)
    Constraints,
    /// Runtime statistics
    RuntimeStats,

    // HMS 4.0+ features
    /// Native Iceberg table support
    IcebergTables,
    /// Native Delta Lake table support
    DeltaTables,
    /// Enhanced statistics
    EnhancedStatistics,
}

impl Capabilities {
    /// Determine capabilities from HMS version
    pub fn from_version(version: HmsVersion) -> Self {
        let mut features = HashSet::new();

        // Core features (all versions >= 2.0)
        features.insert(HmsFeature::Databases);
        features.insert(HmsFeature::Tables);
        features.insert(HmsFeature::Partitions);

        // HMS 2.0+ features
        if version >= HmsVersion { major: 2, minor: 0, patch: 0 } {
            features.insert(HmsFeature::LockService);
        }

        // HMS 2.3+ features
        if version >= HmsVersion { major: 2, minor: 3, patch: 0 } {
            features.insert(HmsFeature::TableStatistics);
            features.insert(HmsFeature::BulkOperations);
            features.insert(HmsFeature::PartitionBatching);
        }

        // HMS 3.0+ features
        if version >= HmsVersion { major: 3, minor: 0, patch: 0 } {
            features.insert(HmsFeature::AcidTransactions);
            features.insert(HmsFeature::MaterializedViews);
            features.insert(HmsFeature::CompactionHistory);
        }

        // HMS 3.1+ features
        if version >= HmsVersion { major: 3, minor: 1, patch: 0 } {
            features.insert(HmsFeature::Constraints);
            features.insert(HmsFeature::RuntimeStats);
        }

        // HMS 4.0+ features
        if version >= HmsVersion { major: 4, minor: 0, patch: 0 } {
            features.insert(HmsFeature::IcebergTables);
            features.insert(HmsFeature::DeltaTables);
            features.insert(HmsFeature::EnhancedStatistics);
        }

        Self { version, features }
    }

    /// Check if a feature is supported
    pub fn supports(&self, feature: &HmsFeature) -> bool {
        self.features.contains(feature)
    }

    /// Get all supported features
    pub fn supported_features(&self) -> Vec<HmsFeature> {
        self.features.iter().copied().collect()
    }

    /// Get human-readable description
    pub fn description(&self) -> String {
        format!(
            "HMS {} with {} features",
            self.version,
            self.features.len()
        )
    }
}

impl std::fmt::Display for HmsFeature {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            HmsFeature::Databases => "databases",
            HmsFeature::Tables => "tables",
            HmsFeature::Partitions => "partitions",
            HmsFeature::LockService => "lock_service",
            HmsFeature::TableStatistics => "table_statistics",
            HmsFeature::BulkOperations => "bulk_operations",
            HmsFeature::PartitionBatching => "partition_batching",
            HmsFeature::AcidTransactions => "acid_transactions",
            HmsFeature::MaterializedViews => "materialized_views",
            HmsFeature::CompactionHistory => "compaction_history",
            HmsFeature::SchemaRegistry => "schema_registry",
            HmsFeature::Constraints => "constraints",
            HmsFeature::RuntimeStats => "runtime_stats",
            HmsFeature::IcebergTables => "iceberg_tables",
            HmsFeature::DeltaTables => "delta_tables",
            HmsFeature::EnhancedStatistics => "enhanced_statistics",
        };
        write!(f, "{}", name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_capabilities_hms_2_3() {
        let version = HmsVersion { major: 2, minor: 3, patch: 0 };
        let caps = Capabilities::from_version(version);

        assert!(caps.supports(&HmsFeature::Databases));
        assert!(caps.supports(&HmsFeature::Tables));
        assert!(caps.supports(&HmsFeature::LockService));
        assert!(caps.supports(&HmsFeature::TableStatistics));
        assert!(caps.supports(&HmsFeature::BulkOperations));

        assert!(!caps.supports(&HmsFeature::AcidTransactions));
        assert!(!caps.supports(&HmsFeature::Constraints));
    }

    #[test]
    fn test_capabilities_hms_3_1() {
        let version = HmsVersion { major: 3, minor: 1, patch: 3 };
        let caps = Capabilities::from_version(version);

        assert!(caps.supports(&HmsFeature::Databases));
        assert!(caps.supports(&HmsFeature::AcidTransactions));
        assert!(caps.supports(&HmsFeature::Constraints));
        assert!(caps.supports(&HmsFeature::MaterializedViews));

        assert!(!caps.supports(&HmsFeature::IcebergTables));
    }

    #[test]
    fn test_capabilities_hms_4_0() {
        let version = HmsVersion { major: 4, minor: 0, patch: 0 };
        let caps = Capabilities::from_version(version);

        assert!(caps.supports(&HmsFeature::IcebergTables));
        assert!(caps.supports(&HmsFeature::DeltaTables));
        assert!(caps.supports(&HmsFeature::EnhancedStatistics));
        assert!(caps.supports(&HmsFeature::AcidTransactions));
    }

    #[test]
    fn test_capabilities_description() {
        let version = HmsVersion { major: 3, minor: 1, patch: 0 };
        let caps = Capabilities::from_version(version);

        let desc = caps.description();
        assert!(desc.contains("3.1.0"));
        assert!(desc.contains("features"));
    }
}
