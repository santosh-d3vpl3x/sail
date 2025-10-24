// SPDX-License-Identifier: Apache-2.0

use serde::{Deserialize, Serialize};

use crate::options::JDBCOptions;

/// Specification for a JDBC partition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JDBCPartition {
    /// Partition index
    pub index: usize,

    /// SQL WHERE predicate for this partition
    pub predicate: Option<String>,
}

/// Plan partitions for parallel JDBC reads
pub fn plan_partitions(options: &JDBCOptions) -> Vec<JDBCPartition> {
    // If explicit predicates provided, use them
    if let Some(predicates_str) = &options.predicates {
        let predicates: Vec<String> = predicates_str
            .split(',')
            .map(|s| s.trim().to_string())
            .collect();

        return predicates
            .into_iter()
            .enumerate()
            .map(|(index, predicate)| JDBCPartition {
                index,
                predicate: Some(predicate),
            })
            .collect();
    }

    // If partition column provided, use range partitioning
    if let Some(column) = &options.partition_column {
        let lower_bound = options.lower_bound.unwrap_or(0);
        let upper_bound = options.upper_bound.unwrap_or(0);
        let num_partitions = options.num_partitions;

        return range_partitions(column, lower_bound, upper_bound, num_partitions);
    }

    // Default: single partition with no predicate
    vec![JDBCPartition {
        index: 0,
        predicate: None,
    }]
}

/// Create range-based partitions
fn range_partitions(
    column: &str,
    lower_bound: i64,
    upper_bound: i64,
    num_partitions: usize,
) -> Vec<JDBCPartition> {
    if num_partitions <= 1 {
        return vec![JDBCPartition {
            index: 0,
            predicate: None,
        }];
    }

    let range = upper_bound - lower_bound;
    if range <= 0 {
        // Empty range, return single partition
        return vec![JDBCPartition {
            index: 0,
            predicate: Some(format!("{} >= {} AND {} < {}", column, lower_bound, column, upper_bound)),
        }];
    }

    let stride = range / num_partitions as i64;
    if stride == 0 {
        // Range too small for requested partitions, create one per value
        return (lower_bound..upper_bound)
            .map(|val| JDBCPartition {
                index: (val - lower_bound) as usize,
                predicate: Some(format!("{} = {}", column, val)),
            })
            .collect();
    }

    // Create range predicates
    (0..num_partitions)
        .map(|i| {
            let start = lower_bound + (i as i64 * stride);
            let end = if i == num_partitions - 1 {
                upper_bound // Last partition gets remainder
            } else {
                start + stride
            };

            JDBCPartition {
                index: i,
                predicate: Some(format!("{} >= {} AND {} < {}", column, start, column, end)),
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_range_partitions() {
        let partitions = range_partitions("id", 0, 1000, 4);
        assert_eq!(partitions.len(), 4);

        assert_eq!(partitions[0].predicate, Some("id >= 0 AND id < 250".to_string()));
        assert_eq!(partitions[1].predicate, Some("id >= 250 AND id < 500".to_string()));
        assert_eq!(partitions[2].predicate, Some("id >= 500 AND id < 750".to_string()));
        assert_eq!(partitions[3].predicate, Some("id >= 750 AND id < 1000".to_string()));
    }

    #[test]
    fn test_single_partition() {
        let partitions = range_partitions("id", 0, 1000, 1);
        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0].predicate, None);
    }

    #[test]
    fn test_small_range() {
        let partitions = range_partitions("id", 0, 3, 10);
        // Should create one partition per value
        assert_eq!(partitions.len(), 3);
        assert_eq!(partitions[0].predicate, Some("id = 0".to_string()));
        assert_eq!(partitions[1].predicate, Some("id = 1".to_string()));
        assert_eq!(partitions[2].predicate, Some("id = 2".to_string()));
    }
}
