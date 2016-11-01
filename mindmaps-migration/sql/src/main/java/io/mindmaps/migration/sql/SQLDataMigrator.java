/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.migration.sql;

import com.google.common.base.Throwables;
import io.mindmaps.MindmapsGraph;
import io.mindmaps.concept.ResourceType;
import io.mindmaps.engine.loader.Loader;
import io.mindmaps.graql.Var;
import io.mindmaps.migration.sql.SQLModel.SQLTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static io.mindmaps.graql.Graql.var;

public class SQLDataMigrator implements Iterable<Collection<Var>>, Closeable {

    private final Logger logger = LoggerFactory.getLogger(SQLDataMigrator.class);

    private Namer namer;
    private MindmapsGraph graph;
    private Connection connection;
    private SQLModel metadata;

    public SQLDataMigrator(){
        this.namer = new Namer() {};
    }

    /**
     * Configure the data migrator with a JDBC connection
     * @param connection the connection containing schema to be migrated
     */
    public SQLDataMigrator configure(Connection connection){
        this.connection = connection;
        this.metadata = new SQLModel(connection);
        return this;
    }

    /**
     * Set a graph from which to get information
     * @param graph a Mindmaps graph
     */
    public SQLDataMigrator graph(MindmapsGraph graph){
        this.graph = graph;
        return this;
    }

    /**
     * Migrate a CSV schema into a Mindmaps ontology
     * @return var patterns representing the migrated Mindmaps ontology
     */
    public Collection<Var> migrate(){
        Collection<Var> collection = new HashSet<>();

        for (Collection<Var> vars : this) {
            collection.addAll(vars);
        }
        return collection;
    }

    /**
     * Migrate the entire data file into the given loader.
     * @param loader Loader to migrate into.
     */
    public SQLDataMigrator migrate(Loader loader){
        for (Collection<Var> vars : this) {
            loader.addToQueue(vars);
        }

        loader.flush();
        loader.waitToFinish();
        return this;
    }

    /**
     * Iterate through the migration
     */
    public Iterator<Collection<Var>> iterator(){
        return new Iterator<Collection<Var>>() {
            Iterator<SQLTable> tables = metadata.iterator();
            PreparedStatement currentStatement = null;
            SQLTable currentTable = null;
            ResultSet currentRow = null;

            @Override
            public boolean hasNext() {

                try {
                    if(currentRow != null && !currentRow.isLast()) {
                        return true;
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }

                boolean tableExists = tables.hasNext();

                if(!tableExists){
                    closeRows();
                }
                return tableExists;
            }

            @Override
            public Collection<Var> next() {
                try {
                    moveReferences();
                    return migrateRow(currentTable, currentRow);
                }
                catch (Throwable e){
                    logger.error("Error migrating row " + currentRow + " from table " + currentTable);
                    logger.error(Throwables.getStackTraceAsString(e));
                }

                return Collections.EMPTY_SET;
            }


            /**
             * Update the global references.
             * If table is complete, move on to next table, else move to next row.
             * @throws SQLException error updating to next row or table
             */
            public void moveReferences() throws SQLException {
                if (currentRow == null || currentRow.isLast()) {
                    nextTable();
                } else{
                    nextRow();
                }
            }

            /**
             * Move reference within current result set up by one
             * @throws SQLException error in updating the reference
             */
            public void nextRow() throws SQLException{
                currentRow.next();
            }

            /**
             * Move reference of the current table up by one
             * @throws SQLException error in opening the connection to the next table
             */
            public void nextTable() throws SQLException {
                if(tables.hasNext()){
                    currentTable = tables.next();
                    openRows();
                }
            }

            /**
             * Open connection to the current table. Query for table rows and maintain the
             * statement and result set in memory.
             */
            private void openRows(){
                String statement = "select * from " + currentTable.getEntityType();
                try {
                    currentStatement = connection.prepareStatement(statement,
                            ResultSet.TYPE_SCROLL_INSENSITIVE,
                            ResultSet.CONCUR_UPDATABLE);
                    currentRow = currentStatement.executeQuery();

                    if(!currentRow.first()) {
                        nextTable();
                    }
                }
                catch (SQLException e){
                    closeRows();
                    logger.error("Error in statement " + statement);
                    throw new RuntimeException(e);
                }
            }

            /**
             * Close the current row and statement.
             */
            public void closeRows(){
                SQLModel.closeQuietly(currentRow);
                SQLModel.closeQuietly(currentStatement);
            }
        };
    }

    /**
     * Close the JDBC connection
     */
    @Override
    public void close() {
        try {
            if(connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * Loop through the rows, migrating the columns as resources or relations. Each row is an entity.
     */
    private Collection<Var> migrateRow(SQLTable currentTable, ResultSet row) throws SQLException {
        String tableType = currentTable.getEntityType();

        // create an instance and insert
        Collection<String> primaryKeyValues = currentTable.getPrimaryKeyValues(row);
        Var instance = var().isa(tableType).id(namer.primaryKey(tableType, primaryKeyValues));

        // migrate each of the columns of that row
        Collection<Var> vars = migrateColumns(currentTable, row, instance);
        vars.add(instance);
        return vars;
    }

    /**
     * Loop through each of the columns in the row, migrating each as a resource or relation.
     */
    private Collection<Var> migrateColumns(SQLTable table, ResultSet row, Var instance) throws SQLException{
        String tableType = table.getEntityType();
        Map<String, ResourceType.DataType> columns = table.getColumns();
        Map<String, String> foreign = table.getForeignKeyColumns();

        ResultSetMetaData metadata = row.getMetaData();
        List<Var> vars = new ArrayList<>();
        for(int i = 1; i <= metadata.getColumnCount(); i++){

            String columnName = metadata.getColumnName(i);
            Object columnValue = row.getObject(columnName);
            ResourceType.DataType dataType = columns.get(columnName);

            String foreignKey = foreign.get(columnName);

            if(foreignKey != null){
                vars.addAll(migrateColumnValueAsRelation(instance, columnName, foreignKey, columnValue));
            }
            else{
                vars.addAll(migrateColumnValueAsResource(instance, columnName, cast(dataType, columnName, row), tableType));
            }
        }

        return vars;
    }

    /**
     * Migrate a column value as a resource
     *
     * @param instance owning instance
     * @param columnName type of the resource
     * @param columnValue value of the resource
     * @param tableName the table that owns the resource
     * @return var patterns representing the resource
     */
    private Collection<Var> migrateColumnValueAsResource(Var instance, String columnName, Object columnValue, String tableName){
        if(columnValue == null){ return Collections.emptyList(); }

        return Collections.singleton(var().id(id(instance))
                .has(namer.resourceName(tableName, columnName), columnValue));
    }

    /**
     * Migrate a column value as a relation.
     * This creates a target node with the ID of the foreign key and a
     * relation from the given instance to this node.
     * @return var patterns representing the relation
     */
    private Collection<Var> migrateColumnValueAsRelation(Var parent, String fkName, String childType, Object childId){
        if(childId == null){ return Collections.emptyList(); }

        String relationType = namer.relationName(fkName);
        String childRole = namer.roleChildName(fkName);
        String parentRole = namer.roleParentName(fkName);

        // create/insert the "owner". We need to make sure that this is already there.
        String foreignPrimaryKey = namer.primaryKey(childType, Collections.singleton(childId.toString()));
        Var child = var().isa(childType).id(foreignPrimaryKey);

        // create the relation to insert
        Var relation = var()
                .rel(childRole, var().id(id(child)))
                .rel(parentRole, var().id(id(parent)))
                .isa(relationType);

        // perform insertion
        return Arrays.asList(child, relation);
    }

    /**
     * Call the correct getter on the row based on the datatype of the column being retrieved.
     *
     * @param type datatype of the column
     * @param column name o the column to be retrieved
     * @param row row from which to get the column
     * @return value of column in row
     * @throws SQLException
     */
    private Object cast(ResourceType.DataType type, String column, ResultSet row) throws SQLException {
        if(ResourceType.DataType.BOOLEAN == type){
            return row.getBoolean(column);
        } else if (ResourceType.DataType.STRING == type){
            return row.getString(column);
        } else if (ResourceType.DataType.LONG == type){
            return row.getLong(column);
        } else if (ResourceType.DataType.DOUBLE == type){
            return row.getDouble(column);
        }
        return null;
    }

    /**
     * Returns the id of a Var
     * @param instance var instance
     * @return the id of the instance
     */
    private String id(Var instance){
        return instance.admin().getId().get();
    }
}
