"""Utility functions for JDBC reader."""

import re


def mask_credentials(connection_string: str) -> str:
    """
    Mask username:password in connection strings for safe logging.

    Examples:
    - postgresql://user:pass@localhost:5432/db → postgresql://***:***@localhost:5432/db
    - mysql://root:secret@host/mydb → mysql://***:***@host/mydb
    - jdbc:sqlserver://user:pass@host → jdbc:sqlserver://***:***@host

    Args:
        connection_string: The connection string to mask

    Returns:
        Connection string with credentials replaced by ***:***
    """
    # Match scheme://user:pass@ or just user:pass@
    return re.sub(
        r"(?<=://)([^:@]+):([^@]+)@",  # user:pass@ after ://
        "***:***@",
        connection_string
    )
