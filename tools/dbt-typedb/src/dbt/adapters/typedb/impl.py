from dbt.adapters.base import ConstraintSupport
from dbt.adapters.postgres import PostgresAdapter
from dbt_common.contracts.constraints import ConstraintType

from dbt.adapters.typedb.connections import TypedbConnectionManager


class TypedbAdapter(PostgresAdapter):
    ConnectionManager = TypedbConnectionManager

    CONSTRAINT_SUPPORT = {
        ConstraintType.check: ConstraintSupport.NOT_SUPPORTED,
        ConstraintType.not_null: ConstraintSupport.NOT_SUPPORTED,
        ConstraintType.unique: ConstraintSupport.NOT_SUPPORTED,
        ConstraintType.primary_key: ConstraintSupport.NOT_SUPPORTED,
        ConstraintType.foreign_key: ConstraintSupport.NOT_SUPPORTED,
    }

    def valid_incremental_strategies(self):
        return []