"""JDBC options normalization and validation."""

import logging
from dataclasses import dataclass
from typing import Optional, Dict, List

from .exceptions import InvalidOptionsError

logger = logging.getLogger("lakesail.jdbc")


def normalize_session_init(session_init_statement: Optional[str]) -> Optional[str]:
    """
    Validate sessionInitStatement is DDL-safe.

    Allow: SET, CREATE TEMPORARY, etc.
    Deny: INSERT, UPDATE, DELETE (no DML allowed)

    Args:
        session_init_statement: SQL statement to execute before reading

    Returns:
        Normalized statement or None

    Raises:
        InvalidOptionsError: If statement contains dangerous keywords
    """
    if not session_init_statement:
        return None

    dangerous_keywords = ["INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "TRUNCATE"]
    stmt_upper = session_init_statement.upper().strip()

    for keyword in dangerous_keywords:
        if stmt_upper.startswith(keyword):
            raise InvalidOptionsError(
                f"sessionInitStatement cannot start with {keyword}. "
                f"Only SET and read-only configuration allowed."
            )

    return session_init_statement


@dataclass
class NormalizedJDBCOptions:
    """Normalized & validated JDBC options."""

    url: str
    dbtable: Optional[str] = None
    query: Optional[str] = None
    user: Optional[str] = None
    password: Optional[str] = None
    engine: str = "connectorx"
    partition_column: Optional[str] = None
    lower_bound: Optional[int] = None
    upper_bound: Optional[int] = None
    num_partitions: int = 1
    predicates: Optional[List[str]] = None
    fetch_size: int = 10000
    session_init_statement: Optional[str] = None
    isolation_level: str = "READ_COMMITTED"

    @classmethod
    def from_spark_options(cls, options: Dict[str, str]) -> "NormalizedJDBCOptions":
        """
        Convert Spark option dict → normalized options.

        Handles case-insensitive option names and validates consistency.

        Args:
            options: Dictionary of JDBC options from Spark

        Returns:
            NormalizedJDBCOptions instance

        Raises:
            InvalidOptionsError: If options are missing or inconsistent
        """
        # Normalize keys to lowercase (Spark passes camelCase, handle both)
        norm_opts = {k.lower(): v for k, v in options.items()}

        # Extract required
        url = norm_opts.get("url")
        if not url:
            raise InvalidOptionsError("'url' option is required")

        # Extract dbtable/query (exactly one required)
        dbtable = norm_opts.get("dbtable")
        query = norm_opts.get("query")

        if not dbtable and not query:
            raise InvalidOptionsError("Either 'dbtable' or 'query' option must be specified")
        if dbtable and query:
            raise InvalidOptionsError("Cannot specify both 'dbtable' and 'query'")

        # Extract user/password (may be in URL or as options)
        user = norm_opts.get("user")
        password = norm_opts.get("password")

        # Engine selection
        engine = norm_opts.get("engine", "connectorx").lower()
        if engine not in ("connectorx", "adbc", "fallback"):
            raise InvalidOptionsError(
                f"Invalid engine: {engine}. Must be one of: connectorx, adbc, fallback"
            )

        # Partitioning
        partition_column = norm_opts.get("partitioncolumn")
        lower_bound = None
        upper_bound = None
        num_partitions = 1

        if "lowerbound" in norm_opts:
            try:
                lower_bound = int(norm_opts["lowerbound"])
            except ValueError:
                raise InvalidOptionsError(f"lowerBound must be an integer, got: {norm_opts['lowerbound']}")

        if "upperbound" in norm_opts:
            try:
                upper_bound = int(norm_opts["upperbound"])
            except ValueError:
                raise InvalidOptionsError(f"upperBound must be an integer, got: {norm_opts['upperbound']}")

        if "numpartitions" in norm_opts:
            try:
                num_partitions = int(norm_opts["numpartitions"])
            except ValueError:
                raise InvalidOptionsError(f"numPartitions must be an integer, got: {norm_opts['numpartitions']}")

        # Predicates
        predicates = None
        if "predicates" in norm_opts:
            predicates = [p.strip() for p in norm_opts["predicates"].split(",") if p.strip()]

        # Fetch size
        fetch_size = 10000
        if "fetchsize" in norm_opts:
            try:
                fetch_size = int(norm_opts["fetchsize"])
            except ValueError:
                raise InvalidOptionsError(f"fetchsize must be an integer, got: {norm_opts['fetchsize']}")

        # Session init
        session_init = norm_opts.get("sessioninitstatement")
        if session_init:
            session_init = normalize_session_init(session_init)

        # Isolation level
        isolation_level = norm_opts.get("isolationlevel", "READ_COMMITTED").upper()

        return cls(
            url=url,
            dbtable=dbtable,
            query=query,
            user=user,
            password=password,
            engine=engine,
            partition_column=partition_column,
            lower_bound=lower_bound,
            upper_bound=upper_bound,
            num_partitions=num_partitions,
            predicates=predicates,
            fetch_size=fetch_size,
            session_init_statement=session_init,
            isolation_level=isolation_level,
        )

    def validate(self) -> None:
        """
        Validate option consistency.

        Raises:
            InvalidOptionsError: If options are inconsistent
        """
        # Partitioning validation
        if self.partition_column:
            if self.lower_bound is None or self.upper_bound is None:
                raise InvalidOptionsError(
                    "partitionColumn requires lowerBound and upperBound"
                )
            if self.lower_bound >= self.upper_bound:
                raise InvalidOptionsError(
                    f"lowerBound ({self.lower_bound}) must be less than upperBound ({self.upper_bound})"
                )
            if self.num_partitions < 1:
                raise InvalidOptionsError("numPartitions must be >= 1")

        # Predicate validation
        if self.predicates and self.partition_column:
            raise InvalidOptionsError(
                "Cannot specify both predicates list and partitionColumn"
            )

        # Num partitions validation
        if self.num_partitions < 1:
            raise InvalidOptionsError(f"numPartitions must be >= 1, got: {self.num_partitions}")

        # Fetch size validation
        if self.fetch_size < 1:
            raise InvalidOptionsError(f"fetchsize must be >= 1, got: {self.fetch_size}")
