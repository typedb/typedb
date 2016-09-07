package io.mindmaps.migration.sql;

import io.mindmaps.core.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * Class to hold metadata of a SQL table.
 */
public class SQLModel implements Iterable<SQLModel.SQLTable> {

    private static final Logger logger = LoggerFactory.getLogger(SQLModel.class);

    private Connection connection;
    private List<SQLTable> tables;

    /**
     * Initialize data structures containing SQL data
     * @param connection Connection containing data to migrate
     */
    public SQLModel(Connection connection){
        this.connection = connection;
        this.tables = new ArrayList<>();

        ResultSet results = null;
        try {
            results = connection.getMetaData().getTables(null, null, null, new String[]{"TABLE"});

            while (results.next()) {
                String tableName = results.getString("TABLE_NAME");
                tables.add(new SQLTable(tableName, connection));
            }
        }
        catch (SQLException e){
            throw new RuntimeException(e);
        }
        finally {
            closeQuietly(results);
        }
    }

    /**
     * Close the given JDBC statement, logging any errors, but not interrupting the calling process.
     * @param closeable jdbc object that extends AutoCloseable
     */
    public static void closeQuietly(AutoCloseable closeable) {
        try {
            if (closeable!= null) {
                closeable.close();
            }
        }
        catch (Exception e) {
            logger.error("An error occurred closing statement.", e);
        }
    }

    @Override
    public Iterator<SQLTable> iterator() {
        return new Iterator<SQLTable>() {
            int currentTableIndex = 0;

            @Override
            public boolean hasNext() {
                return currentTableIndex < tables.size();
            }

            @Override
            public SQLTable next() {
                return tables.get(currentTableIndex++);
            }
        };
    }

    class SQLTable {
        private String type;
        private List<String> primaryKeyColumns;
        private Map<String, String> foreignKeyColumns;
        private Map<String, Data> columnTypes;

         SQLTable(String type, Connection connection) {
            this.type = type;
            columnTypes = new HashMap<>();
            primaryKeyColumns = new ArrayList<>();
            foreignKeyColumns = new HashMap<>();

            // migrate the metadata
            try {
                getColumns(connection);
                getPrimaryKeyColumns(connection);
                getForeignKeyColumns(connection);

            } catch (SQLException e) {
                logger.error("Error migrating metadata of table " + type);
                throw new RuntimeException(e);
            }
        }

        public String getEntityType() {
            return type;
        }

        public Connection getConnection() {
            return connection;
        }

        public List<String> getPrimaryKeyColumns() {
            return primaryKeyColumns;
        }

        public Map<String, String> getForeignKeyColumns() {
            return foreignKeyColumns;
        }

        public Map<String, Data> getColumns() {
            return columnTypes;
        }

        private void getColumns(Connection connection) throws SQLException {
            ResultSet results = connection.getMetaData().getColumns(null, "public", type, null);
            if(!results.first()){
                results = connection.getMetaData().getColumns(null, "PUBLIC", type, null);
            } else {
                results = connection.getMetaData().getColumns(null, "public", type, null);
            }

            while(results.next()){
                String name = results.getString("COLUMN_NAME");
                Data type = SQLType.getDatatype(results.getInt("DATA_TYPE"));

                columnTypes.put(name, type);
            }

            closeQuietly(results);
        }

        private void getPrimaryKeyColumns(Connection connection) throws SQLException {
            ResultSet results = connection.getMetaData().getPrimaryKeys(null, null, type);

            while(results.next()){
                if(results.getString("COLUMN_NAME") != null){
                    primaryKeyColumns.add(results.getString("COLUMN_NAME"));
                }
            }

            closeQuietly(results);
        }

        private void getForeignKeyColumns(Connection connection) throws SQLException{
            ResultSet results = connection.getMetaData().getImportedKeys(null, null, type);

            while (results.next()){
                String foreignKeyName = results.getString("FKCOLUMN_NAME");
                if(foreignKeyName != null){
                    String pkName = results.getString("PKTABLE_NAME");

                    foreignKeyColumns.put(foreignKeyName, pkName);
                }
            }

            closeQuietly(results);
        }


        /**
        * Get the primary key of a given row (the concatenation of the values of the primary key columns)
        */
        public Collection<String> getPrimaryKeyValues(ResultSet row) throws SQLException {
            Collection<String> values = new ArrayList<>();
            for(String key:primaryKeyColumns){
                if(row.getObject(key) != null){
                    values.add(row.getObject(key).toString());
                }
            }

            return values;
        }
    }
}
