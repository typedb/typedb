import re
from dataclasses import dataclass
from typing import Any, Optional, Tuple, Type

from dbt.adapters.contracts.connection import Connection
from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.postgres.connections import PostgresConnectionManager, PostgresCredentials
from dbt_common.exceptions import DbtRuntimeError


logger = AdapterLogger("TypeDB")

_LEADING_SQL_DECORATION = re.compile(r"(?is)\A(?:\s+|/\*.*?\*/\s*|--[^\n]*(?:\n|$))*")
_LEADING_KEYWORD = re.compile(r"(?i)([a-z_]+)")
_READ_ONLY_STATEMENTS = {
    "begin",
    "commit",
    "deallocate",
    "discard",
    "explain",
    "reset",
    "rollback",
    "select",
    "set",
    "show",
    "values",
    "with",
}


def leading_sql_keyword(sql: str) -> Optional[str]:
    body = _LEADING_SQL_DECORATION.sub("", sql, count=1)
    match = _LEADING_KEYWORD.match(body)
    if match is None:
        return None
    return match.group(1).lower()


@dataclass
class TypedbCredentials(PostgresCredentials):
    @property
    def type(self):
        return "typedb"


class TypedbConnectionManager(PostgresConnectionManager):
    TYPE = "typedb"

    def cancel(self, connection: Connection):
        logger.debug("TypeDB pgwire does not support backend cancellation; skipping cancel request")

    def add_query(
        self,
        sql: str,
        auto_begin: bool = True,
        bindings: Optional[Any] = None,
        abridge_sql_log: bool = False,
        retryable_exceptions: Tuple[Type[Exception], ...] = tuple(),
        retry_limit: int = 1,
    ) -> Tuple[Connection, Any]:
        keyword = leading_sql_keyword(sql)
        connection = self.get_thread_connection()
        conn_name = connection.name if connection is not None and connection.name else "<None>"

        if keyword is None:
            raise DbtRuntimeError(f"Tried to run invalid SQL: {sql} on {conn_name}")

        if keyword not in _READ_ONLY_STATEMENTS:
            raise DbtRuntimeError(
                "dbt-typedb only supports read-only pgwire workflows. "
                f"The statement '{keyword}' is not supported."
            )

        return super().add_query(
            sql,
            auto_begin=auto_begin,
            bindings=bindings,
            abridge_sql_log=abridge_sql_log,
            retryable_exceptions=retryable_exceptions,
            retry_limit=retry_limit,
        )