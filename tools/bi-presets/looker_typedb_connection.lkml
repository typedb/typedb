# Looker connection preset for TypeDB pgwire endpoint.
# Import this into your Looker project or use as reference.
#
# File: looker_typedb_connection.lkml
# Default local credentials for the connection are admin/password unless changed.

connection: "typedb"

# Example explore referencing a TypeDB projection table
# explore: my_projection {
#   sql_table_name: public.my_projection ;;
# }
