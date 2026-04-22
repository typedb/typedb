from dbt.adapters.base import AdapterPlugin

from dbt.adapters.typedb.connections import TypedbConnectionManager, TypedbCredentials
from dbt.adapters.typedb.impl import TypedbAdapter
from dbt.include import typedb


Plugin = AdapterPlugin(
    adapter=TypedbAdapter,  # type: ignore[arg-type]
    credentials=TypedbCredentials,
    include_path=typedb.PACKAGE_PATH,
    dependencies=["postgres"],
    project_name="dbt_typedb",
)

__all__ = [
    "Plugin",
    "TypedbAdapter",
    "TypedbConnectionManager",
    "TypedbCredentials",
]