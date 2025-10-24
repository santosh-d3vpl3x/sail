"""ADBC (Arrow Database Connectivity) backend implementation."""

import logging
from typing import List

import pyarrow as pa

from .base import DatabaseBackend
from ..exceptions import BackendNotAvailableError, DatabaseError, UnsupportedDatabaseError
from ..utils import mask_credentials

logger = logging.getLogger("lakesail.jdbc")


class ADBCBackend(DatabaseBackend):
    """Backend using ADBC (Arrow DB Connectivity standard)."""

    def __init__(self):
        """Initialize ADBC backend."""
        try:
            import adbc_driver_manager
            self.adbc_dm = adbc_driver_manager
        except ImportError:
            raise BackendNotAvailableError(
                "ADBC driver manager not installed. "
                "Install with: pip install adbc-driver-manager"
            )

    def read_batches(
        self,
        connection_string: str,
        query: str,
        fetch_size: int = 10000,
    ) -> List[pa.RecordBatch]:
        """
        Read and return Arrow RecordBatches.

        Args:
            connection_string: Database connection string
            query: SQL query to execute
            fetch_size: Batch size hint for driver

        Returns:
            List of Arrow RecordBatches

        Raises:
            DatabaseError: If read fails
            UnsupportedDatabaseError: If database driver not supported
        """
        try:
            logger.info(f"ADBC reading from {mask_credentials(connection_string)}")
            logger.debug(f"Query: {query}")

            # Get driver module for this URI
            driver_module = self._get_driver_module(connection_string)

            batches = []
            with driver_module.connect(connection_string) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query)

                    # Fetch Arrow batches
                    # ADBC >= 1.6.0: fetch_arrow_batches() returns iterator
                    # ADBC < 1.6.0: fetch_arrow_table() returns single table
                    if hasattr(cursor, "fetch_arrow_batches"):
                        logger.debug("Using fetch_arrow_batches() (ADBC >= 1.6.0)")
                        for batch in cursor.fetch_arrow_batches():
                            if isinstance(batch, pa.RecordBatch):
                                batches.append(batch)
                            else:  # Might be a Table
                                batches.extend(batch.to_batches())
                    elif hasattr(cursor, "fetch_arrow_table"):
                        logger.debug("Using fetch_arrow_table() (ADBC < 1.6.0)")
                        table = cursor.fetch_arrow_table()
                        batches = table.to_batches()
                    else:
                        raise DatabaseError(
                            "ADBC cursor has neither fetch_arrow_batches() nor fetch_arrow_table(). "
                            "Please upgrade adbc-driver-manager."
                        )

            total_rows = sum(batch.num_rows for batch in batches)
            logger.info(
                f"ADBC read {total_rows} rows in {len(batches)} batches "
                f"from {mask_credentials(connection_string)}"
            )

            return batches

        except Exception as err:
            # Always mask credentials in error messages
            safe_err = str(err).replace(connection_string, mask_credentials(connection_string))
            logger.error(f"ADBC error: {safe_err}")
            raise DatabaseError(f"ADBC error: {safe_err}") from err

    def _get_driver_module(self, connection_string: str):
        """
        Load ADBC driver for URI scheme.

        Args:
            connection_string: Database connection string

        Returns:
            ADBC driver module

        Raises:
            UnsupportedDatabaseError: If scheme not supported
            BackendNotAvailableError: If driver module not installed
        """
        # Extract scheme from connection string
        scheme = connection_string.split("://")[0].lower() if "://" in connection_string else ""

        # Map scheme to ADBC driver module
        module_map = {
            "postgresql": "adbc_driver_postgresql.dbapi",
            "postgres": "adbc_driver_postgresql.dbapi",
            "sqlite": "adbc_driver_sqlite.dbapi",
            "snowflake": "adbc_driver_snowflake.dbapi",
        }

        module_name = module_map.get(scheme)
        if not module_name:
            raise UnsupportedDatabaseError(
                f"ADBC does not support scheme '{scheme}'. "
                f"Supported: {', '.join(module_map.keys())}"
            )

        try:
            # Import the driver module
            parts = module_name.split(".")
            module = __import__(module_name, fromlist=[parts[-1]])
            return module
        except ImportError as err:
            package_name = module_name.split(".")[0].replace("_", "-")
            raise BackendNotAvailableError(
                f"ADBC driver '{package_name}' not installed. "
                f"Install with: pip install {package_name}"
            ) from err

    def get_name(self) -> str:
        """Get backend name."""
        return "ADBC"

    def is_available(self) -> bool:
        """Check if ADBC is available."""
        try:
            import adbc_driver_manager
            return True
        except ImportError:
            return False
