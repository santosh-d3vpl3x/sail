"""JDBC URL parsing with minimal conversion strategy.

This module extracts the driver name and delegates the rest to backends.
ConnectorX and ADBC already understand their dialects; we don't reinvent it.
"""

import re
from dataclasses import dataclass
from typing import Optional
from urllib.parse import urlparse, parse_qs

from .exceptions import InvalidJDBCUrlError


@dataclass
class ParsedJDBCUrl:
    """Minimal JDBC URL parse result (delegate to backend)."""
    driver: str                          # e.g., 'postgresql', 'mysql', 'oracle'
    connection_string: str               # Full connection string for backend
    user: Optional[str] = None           # Optional override from URL
    password: Optional[str] = None       # Optional override from URL


def parse_jdbc_url(jdbc_url: str, user: Optional[str] = None, password: Optional[str] = None) -> ParsedJDBCUrl:
    """
    Extract driver and delegate rest to backend.

    Examples:
    - jdbc:postgresql://localhost/mydb → driver='postgresql', connection_string='postgresql://localhost/mydb'
    - jdbc:mysql://root@host/db → driver='mysql', connection_string='mysql://root@host/db'
    - jdbc:sqlserver://host;database=db → driver='sqlserver', connection_string='sqlserver://host;database=db'

    Args:
        jdbc_url: JDBC URL string starting with jdbc:
        user: Optional user to override/inject into URL
        password: Optional password to override/inject into URL

    Returns:
        ParsedJDBCUrl with driver, connection_string, and optional credentials

    Raises:
        InvalidJDBCUrlError: If URL format is invalid
    """
    if not jdbc_url.startswith("jdbc:"):
        raise InvalidJDBCUrlError(f"Invalid JDBC URL: must start with 'jdbc:', got: {jdbc_url}")

    # Extract driver (e.g., 'postgresql', 'mysql:thin')
    match = re.match(r"jdbc:([^:]+(?::[^/:]+)?)(.*)", jdbc_url)
    if not match:
        raise InvalidJDBCUrlError(f"Invalid JDBC URL format: {jdbc_url}")

    driver_part = match.group(1)
    rest = match.group(2)

    # Normalize driver to base name (strip subtype like ':thin')
    driver = driver_part.split(":")[0].lower()

    # Build URI by replacing 'jdbc:driver' with just 'driver'
    # For most backends, we can strip the jdbc: prefix
    connection_string = f"{driver}{rest}"

    # Extract credentials from URL if present
    url_user = None
    url_password = None

    # Try to parse as URL to extract credentials
    try:
        # Handle both :// and : formats
        if "://" in connection_string:
            parsed = urlparse(connection_string)
            url_user = parsed.username
            url_password = parsed.password

            # If user/password provided as parameters, inject them into URL
            if user or password:
                final_user = user or url_user or ""
                final_password = password or url_password or ""

                # Rebuild URL with injected credentials
                if final_user or final_password:
                    # Remove existing credentials from netloc
                    netloc = parsed.hostname or ""
                    if parsed.port:
                        netloc = f"{netloc}:{parsed.port}"

                    # Add credentials
                    if final_password:
                        netloc = f"{final_user}:{final_password}@{netloc}"
                    elif final_user:
                        netloc = f"{final_user}@{netloc}"

                    connection_string = f"{parsed.scheme}://{netloc}{parsed.path}"
                    if parsed.query:
                        connection_string += f"?{parsed.query}"
                    if parsed.fragment:
                        connection_string += f"#{parsed.fragment}"
    except Exception:
        # If URL parsing fails, leave connection_string as-is
        # Some JDBC URLs (like SQL Server) use non-standard formats
        pass

    # Use provided credentials or extracted ones
    final_user = user or url_user
    final_password = password or url_password

    return ParsedJDBCUrl(
        driver=driver,
        connection_string=connection_string,
        user=final_user,
        password=final_password
    )


def validate_driver_supported(driver: str) -> None:
    """
    Warn if driver may not be supported by any enabled backend.

    Note: This is minimal validation; backends will fail explicitly if unsupported.

    Args:
        driver: Database driver name (e.g., 'postgresql', 'mysql')
    """
    # Common supported drivers
    supported_drivers = {
        'postgresql', 'postgres', 'mysql', 'sqlite', 'sqlserver',
        'mssql', 'oracle', 'snowflake', 'redshift', 'clickhouse'
    }

    if driver.lower() not in supported_drivers:
        import logging
        logger = logging.getLogger("lakesail.jdbc")
        logger.warning(
            f"Driver '{driver}' may not be supported by ConnectorX or ADBC backends. "
            f"Supported drivers: {', '.join(sorted(supported_drivers))}"
        )
