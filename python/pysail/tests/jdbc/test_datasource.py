"""Tests for JDBCArrowDataSource behaviors."""

import pytest

from pysail.jdbc.datasource import JDBCArrowDataSource
from pysail.jdbc.exceptions import InvalidOptionsError


class TestJDBCArrowDataSource:
    """Validate JDBCArrowDataSource safeguards."""

    def test_plan_partitions_runs_validation(self):
        """plan_partitions should surface option validation errors."""
        datasource = JDBCArrowDataSource()
        options = {
            "url": "jdbc:postgresql://localhost:5432/mydb",
            "dbtable": "orders",
            "numPartitions": "0",
        }

        with pytest.raises(
            InvalidOptionsError,
            match="numPartitions must be >= 1",
        ):
            datasource.plan_partitions(options)
