"""Metrics collection and observability for JDBC reads."""

import logging
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any

logger = logging.getLogger("lakesail.jdbc")


@dataclass
class PartitionMetrics:
    """Per-partition read statistics."""

    partition_id: int
    row_count: int = 0
    byte_count: int = 0
    wall_time_ms: float = 0.0
    predicate: Optional[str] = None
    error: Optional[str] = None
    retry_count: int = 0

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "partition_id": self.partition_id,
            "row_count": self.row_count,
            "byte_count": self.byte_count,
            "wall_time_ms": self.wall_time_ms,
            "predicate": self.predicate,
            "error": self.error,
            "retry_count": self.retry_count,
        }


@dataclass
class MetricsCollector:
    """Collect and log per-partition metrics."""

    metrics: List[PartitionMetrics] = field(default_factory=list)

    def record(self, metric: PartitionMetrics) -> None:
        """
        Record partition metrics.

        Args:
            metric: Partition metrics to record
        """
        self.metrics.append(metric)

        # Log to standard logger (can be captured by Spark)
        if metric.error:
            logger.error(
                f"Partition {metric.partition_id} failed: {metric.error} "
                f"(retries: {metric.retry_count})"
            )
        else:
            logger.info(
                f"Partition {metric.partition_id}: "
                f"{metric.row_count:,} rows, {metric.byte_count:,} bytes, "
                f"{metric.wall_time_ms:.1f}ms"
            )

    def summary(self) -> Dict[str, Any]:
        """
        Return aggregated metrics.

        Returns:
            Dictionary with summary statistics
        """
        if not self.metrics:
            return {
                "total_rows": 0,
                "total_bytes": 0,
                "total_time_ms": 0.0,
                "partitions": 0,
                "avg_rows_per_partition": 0,
                "avg_time_per_partition_ms": 0.0,
                "failed_partitions": 0,
            }

        successful_metrics = [m for m in self.metrics if not m.error]
        failed_count = len([m for m in self.metrics if m.error])

        total_rows = sum(m.row_count for m in successful_metrics)
        total_bytes = sum(m.byte_count for m in successful_metrics)
        total_time = sum(m.wall_time_ms for m in successful_metrics)
        partition_count = len(successful_metrics)

        return {
            "total_rows": total_rows,
            "total_bytes": total_bytes,
            "total_time_ms": total_time,
            "partitions": partition_count,
            "avg_rows_per_partition": total_rows // max(1, partition_count),
            "avg_time_per_partition_ms": total_time / max(1, partition_count),
            "failed_partitions": failed_count,
            "total_retries": sum(m.retry_count for m in self.metrics),
        }

    def log_summary(self) -> None:
        """Log summary statistics."""
        summary = self.summary()
        logger.info(
            f"JDBC read completed: {summary['total_rows']:,} rows, "
            f"{summary['total_bytes']:,} bytes, {summary['total_time_ms']:.1f}ms "
            f"across {summary['partitions']} partitions"
        )

        if summary['failed_partitions'] > 0:
            logger.warning(f"{summary['failed_partitions']} partition(s) failed")

        if summary['total_retries'] > 0:
            logger.info(f"Total retries: {summary['total_retries']}")
