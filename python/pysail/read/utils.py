"""Utility functions for JDBC reader."""

import re


def mask_credentials(connection_string: str) -> str:
    """
    Mask username:password in connection strings for safe logging.

    Examples:
    - postgresql://user:pass@localhost:5432/db → postgresql://***:***@localhost:5432/db
    - mysql://root:secret@host/mydb → mysql://***:***@host/mydb
    - jdbc:sqlserver://user:pass@host → jdbc:sqlserver://***:***@host
    - postgresql://user:p@ssw0rd!@localhost/db → postgresql://***:***@localhost/db
    - postgresql://user:@localhost/db → postgresql://***:***@localhost/db

    Args:
        connection_string: The connection string to mask

    Returns:
        Connection string with credentials replaced by ***:***
    """
    # Match ://username:password@ where password can be empty or contain special chars like @
    # [^/:]+  matches username (one or more chars, not : or /)
    # [^/]*   matches password (zero or more chars, not /) - allows @ in password and empty passwords
    return re.sub(
        r"://[^/:]+:[^/]*@",
        "://***:***@",
        connection_string
    )
