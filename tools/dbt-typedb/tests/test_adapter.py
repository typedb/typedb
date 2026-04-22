import unittest

from dbt.adapters.base import ConstraintSupport

from dbt.adapters.typedb import Plugin, TypedbAdapter, TypedbConnectionManager, TypedbCredentials
from dbt.adapters.typedb.connections import leading_sql_keyword


class TypedbAdapterTests(unittest.TestCase):
    def test_plugin_registers_typedb_adapter(self):
        self.assertEqual(Plugin.adapter.type(), "typedb")
        self.assertEqual(Plugin.credentials, TypedbCredentials)
        self.assertEqual(Plugin.dependencies, ["postgres"])

    def test_credentials_report_typedb_type(self):
        credentials = TypedbCredentials(
            host="localhost",
            user="admin",
            port=5432,
            password="password",
            database="typedb",
            schema="public",
        )
        self.assertEqual(credentials.type, "typedb")

    def test_connection_manager_uses_typedb_type(self):
        self.assertEqual(TypedbConnectionManager.TYPE, "typedb")

    def test_adapter_declares_constraints_unsupported(self):
        self.assertTrue(TypedbAdapter.CONSTRAINT_SUPPORT)
        for support in TypedbAdapter.CONSTRAINT_SUPPORT.values():
            self.assertEqual(support, ConstraintSupport.NOT_SUPPORTED)

    def test_leading_sql_keyword_ignores_comments(self):
        self.assertEqual(leading_sql_keyword("/* dbt */\nSELECT 1"), "select")
        self.assertEqual(leading_sql_keyword("-- comment\nWITH cte AS (SELECT 1) SELECT * FROM cte"), "with")
        self.assertIsNone(leading_sql_keyword("   \n  "))


if __name__ == "__main__":
    unittest.main()