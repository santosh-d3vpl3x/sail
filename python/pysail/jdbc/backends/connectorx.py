"""ConnectorX backend implementation."""

import logging
from typing import List

import pyarrow as pa

from .base import DatabaseBackend
from ..exceptions import BackendNotAvailableError, DatabaseError
from ..utils import mask_credentials

logger = logging.getLogger("lakesail.jdbc")


class ConnectorXBackend(DatabaseBackend):
    """Backend using ConnectorX (distributed, Rust-based)."""

    def __init__(self):
        """Initialize ConnectorX backend."""
        try:
            import connectorx as cx
            self.cx = cx
            # Parse version (e.g., "0.3.2" -> (0, 3))
            try:
                self.version = tuple(map(int, cx.__version__.split(".")[:2]))
            except (ValueError, AttributeError):
                self.version = (0, 0)
                logger.warning(f"Could not parse ConnectorX version: {getattr(cx, '__version__', 'unknown')}")
        except ImportError:
            raise BackendNotAvailableError(
                "ConnectorX backend not installed. "
                "Install with: pip install 'connectorx>=0.3.0'"
            )

        # Verify minimum version
        if self.version < (0, 3):
            logger.warning(
                f"ConnectorX version {'.'.join(map(str, self.version))} is older than recommended 0.3.0. "
                f"Arrow return type may not be available."
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
            fetch_size: Batch size (not used by ConnectorX, kept for API consistency)

        Returns:
            List of Arrow RecordBatches

        Raises:
            DatabaseError: If read fails
        """
        try:
            logger.info(f"ConnectorX reading from {mask_credentials(connection_string)}")
            logger.debug(f"Query: {query}")

            # ConnectorX returns PyArrow Table
            # Use return_type="arrow" for cx >= 0.3.0, otherwise fall back to arrow2
            try:
                arrow_table = self.cx.read_sql(
                    conn=connection_string,
                    query=query,
                    return_type="arrow",  # Requires cx >= 0.3.0
                )
            except Exception as e:
                # Fall back to arrow2 for older versions
                if "return_type" in str(e):
                    logger.warning("Falling back to 'arrow2' return type for older ConnectorX version")
                    arrow_table = self.cx.read_sql(
                        conn=connection_string,
                        query=query,
                        return_type="arrow2",
                    )
                else:
                    raise

            if not isinstance(arrow_table, pa.Table):
                raise DatabaseError(
                    f"ConnectorX returned unexpected type: {type(arrow_table)}. "
                    f"Expected pyarrow.Table"
                )

            # Return as RecordBatches (not monolithic Table)
            batches = arrow_table.to_batches()
            logger.info(
                f"ConnectorX read {arrow_table.num_rows} rows "
                f"in {len(batches)} batches from {mask_credentials(connection_string)}"
            )

            return batches

        except Exception as err:
            # Always mask credentials in error messages
            safe_err = str(err).replace(connection_string, mask_credentials(connection_string))
            logger.error(f"ConnectorX error: {safe_err}")
            raise DatabaseError(f"ConnectorX error: {safe_err}") from err

    def get_name(self) -> str:
        """Get backend name."""
        return "ConnectorX"

    def is_available(self) -> bool:
        """Check if ConnectorX is available."""
        try:
            import connectorx
            return True
        except ImportError:
            return False
