"""Fallback backend using pyodbc (row-wise, slow, for testing only)."""

import logging
from typing import List, Tuple, Any

import pyarrow as pa

from .base import DatabaseBackend
from ..exceptions import BackendNotAvailableError, DatabaseError
from ..utils import mask_credentials

logger = logging.getLogger("lakesail.jdbc")


class FallbackBackend(DatabaseBackend):
    """
    FALLBACK ONLY: Row-wise reading via pyodbc.

    ⚠️ WARNING: This backend:
      - Does NOT support partitioning efficiently
      - Materializes entire result set in memory
      - May fail on very large tables
      - Is 10-100x slower than ConnectorX/ADBC

    Use only for:
      - Testing
      - Small result sets
      - Databases not supported by other backends
    """

    # Safety limit: warn if > 1M rows
    ROW_LIMIT = 1_000_000

    def __init__(self):
        """Initialize fallback backend."""
        try:
            import pyodbc
            self.pyodbc = pyodbc
        except ImportError:
            raise BackendNotAvailableError(
                "Fallback backend requires pyodbc. "
                "Install with: pip install pyodbc"
            )

    def read_batches(
        self,
        connection_string: str,
        query: str,
        fetch_size: int = 10000,
    ) -> List[pa.RecordBatch]:
        """
        Read via ODBC (row-wise; slow).

        Args:
            connection_string: ODBC connection string
            query: SQL query to execute
            fetch_size: Batch size for fetching

        Returns:
            List of Arrow RecordBatches

        Raises:
            DatabaseError: If read fails
        """
        # Build ODBC connection string
        conn_str = self._build_odbc_connection_string(connection_string)

        try:
            logger.warning(
                f"Using fallback backend (slow, row-wise). "
                f"Consider installing ConnectorX for better performance: "
                f"{mask_credentials(conn_str)}"
            )
            logger.debug(f"Query: {query}")

            conn = self.pyodbc.connect(conn_str)
            try:
                cursor = conn.cursor()
                cursor.execute(query)

                # Fetch with safety limit
                rows = []
                row_count = 0

                # Fetch in batches for better memory usage
                while True:
                    batch = cursor.fetchmany(fetch_size)
                    if not batch:
                        break

                    rows.extend(batch)
                    row_count += len(batch)

                    if row_count > self.ROW_LIMIT:
                        raise DatabaseError(
                            f"Result set exceeds {self.ROW_LIMIT:,} rows. "
                            f"Use ConnectorX or ADBC backend for large tables."
                        )

                if row_count == 0:
                    logger.warning("Query returned no rows")
                    # Return empty table with schema from cursor
                    schema = self._cursor_to_arrow_schema(cursor.description)
                    return [pa.RecordBatch.from_pydict({}, schema=schema)]

                # Convert rows to Arrow
                schema, arrays = self._rows_to_arrow(cursor.description, rows)
                table = pa.Table.from_arrays(arrays, schema=schema)

                logger.info(
                    f"Fallback backend read {row_count:,} rows "
                    f"from {mask_credentials(conn_str)}"
                )

                return table.to_batches()

            finally:
                conn.close()

        except Exception as err:
            # Always mask credentials in error messages
            safe_err = str(err).replace(conn_str, mask_credentials(conn_str))
            safe_err = safe_err.replace(connection_string, mask_credentials(connection_string))
            logger.error(f"Fallback backend error: {safe_err}")
            raise DatabaseError(f"Fallback backend error: {safe_err}") from err

    def _build_odbc_connection_string(self, uri: str) -> str:
        """
        Convert URI to ODBC connection string.

        Args:
            uri: Connection URI (e.g., postgresql://user:pass@host/db)

        Returns:
            ODBC connection string
        """
        # For now, return as-is and let pyodbc handle it
        # This is a simplified implementation
        # In production, you'd parse URI and build proper ODBC string
        return uri

    def _cursor_to_arrow_schema(self, description) -> pa.Schema:
        """
        Build Arrow schema from cursor description (for empty results).

        Args:
            description: Cursor description (list of column metadata)

        Returns:
            Arrow schema
        """
        fields = []
        for col in description:
            col_name = col[0]
            # Default to string type for unknown types
            fields.append(pa.field(col_name, pa.string()))

        return pa.schema(fields)

    def _rows_to_arrow(
        self,
        description,
        rows: List[Tuple[Any, ...]],
    ) -> Tuple[pa.Schema, List[pa.Array]]:
        """
        Convert rows to Arrow schema and arrays.

        Args:
            description: Cursor description (column metadata)
            rows: List of row tuples

        Returns:
            Tuple of (schema, arrays)
        """
        if not rows:
            # Empty result
            schema = self._cursor_to_arrow_schema(description)
            arrays = [pa.array([]) for _ in description]
            return schema, arrays

        # Extract column names and infer types
        col_names = [col[0] for col in description]
        col_types = [col[1] for col in description]

        # Transpose rows to columns
        columns = list(zip(*rows))

        # Convert each column to Arrow array
        arrays = []
        fields = []

        for col_name, col_data, col_type in zip(col_names, columns, col_types):
            try:
                # Try to infer Arrow type from data
                arrow_array = pa.array(col_data)
                arrays.append(arrow_array)
                fields.append(pa.field(col_name, arrow_array.type))
            except Exception as e:
                # Fall back to string conversion
                logger.warning(
                    f"Could not infer type for column '{col_name}', "
                    f"falling back to string: {e}"
                )
                arrow_array = pa.array([str(v) if v is not None else None for v in col_data])
                arrays.append(arrow_array)
                fields.append(pa.field(col_name, pa.string()))

        schema = pa.schema(fields)
        return schema, arrays

    def get_name(self) -> str:
        """Get backend name."""
        return "Fallback (pyodbc)"

    def is_available(self) -> bool:
        """Check if pyodbc is available."""
        try:
            import pyodbc
            return True
        except ImportError:
            return False
