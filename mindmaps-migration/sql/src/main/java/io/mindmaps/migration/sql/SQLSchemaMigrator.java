package io.mindmaps.migration.sql;

import com.google.common.collect.Lists;
import io.mindmaps.MindmapsGraph;
import io.mindmaps.core.Data;
import io.mindmaps.engine.loader.Loader;
import io.mindmaps.graql.Var;
import io.mindmaps.migration.sql.SQLModel.SQLTable;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static io.mindmaps.graql.Graql.var;

public class SQLSchemaMigrator implements Closeable {

    private Namer namer;
    private Connection connection;
    private SQLModel metadata;
    private MindmapsGraph graph;

    public SQLSchemaMigrator(){
        this.namer = new Namer(){};
    }

    /**
     * Configure the schema migrator with a JDBC connection
     * @param connection the connection containing schema to be migrated
     */
    public SQLSchemaMigrator configure(Connection connection){
        this.connection = connection;
        this.metadata = new SQLModel(connection);
        return this;
    }

    /**
     * Set a graph from which to get information
     * @param graph a Mindmaps graph
     */
    public SQLSchemaMigrator graph(MindmapsGraph graph){
        this.graph = graph;
        return this;
    }

    public Collection<Var> migrate(){
        Collection<Var> vars = new HashSet<>();
        for (SQLTable aMetadata : metadata) {
            vars.addAll(migrateAsEntity(aMetadata));
            vars.addAll(migrateColumns(aMetadata));
        }
        return vars;
    }

    /**
     * Migrate the entire schema into the given loader.
     * @param loader Loader to migrate into.
     */
    public SQLSchemaMigrator migrate(Loader loader){
        loader.addToQueue(migrate());
        loader.flush();
        loader.waitToFinish();
        return this;
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
     * Loop through each of the columns in the given table, migrating each as a resource or relation.
     * @param currentTable Table from which to obtain columns
     * @return Collection of var patterns for the migrated columns
     */
    private Collection<Var> migrateColumns(SQLTable currentTable) {
        List<Var> vars = new ArrayList<>();

        String tableType = currentTable.getEntityType();
        Map<String, Data> columns = currentTable.getColumns();
        Map<String, String> foreignColumns = currentTable.getForeignKeyColumns();

        for(String column: columns.keySet()){

            Data columnType = columns.get(column);

            if(foreignColumns.containsKey(column)){
                vars.addAll(migrateAsRelation(tableType, column, foreignColumns.get(column)));
            }
            else{
                vars.addAll(migrateAsResource(tableType, columnType, column));
            }
        }

        return vars;
    }

    /**
     * Migrate a table as an entity type
     * @param currentTable name of the table (that becomes name of the entity)
     * @return var patterns representing the entity
     */
    private Collection<Var> migrateAsEntity(SQLTable currentTable){
        Var type = var().isa("entity-type").id(currentTable.getEntityType());

        return Lists.newArrayList(type);
    }

    /**
     * Migrate a column as a resource type
     * @param ownerType id of the owning type
     * @param columnType datatype of the column
     * @param columnName name of the column
     * @return var patterns representing the resource type
     */
    private Collection<Var> migrateAsResource(String ownerType, Data columnType, String columnName){
        String resourceName = namer.resourceName(ownerType, columnName);

        // create the vars
        Var resourceType = var().id(resourceName)
                .datatype(columnType)
                .isa("resource-type");

        Var hasResource = var().id(ownerType).hasResource(resourceName);
        return Lists.newArrayList(resourceType, hasResource);
    }

    /**
     * Migrate a foreign key column as a relation
     * @param entityTypeParent name of the parent table
     * @param columnType name of the foreign key column (becomes the relation type)
     * @param childType name of the target table (becomes the target entity type)
     * @return var patterns representing the relation type
     */
    private Collection<Var> migrateAsRelation(String entityTypeParent, String columnType, String childType){
        String roleParentName = namer.roleParentName(columnType);
        String roleChildName = namer.roleChildName(columnType);

        // create the vars
        Var entityTypeChild = var().id(childType).isa("entity-type");
        Var roleTypeParent = var().id(roleParentName).isa("role-type");
        Var roleTypeChild = var().id(roleChildName).isa("role-type");

        Var relationType = var()
                .id(namer.relationName(columnType)).isa("relation-type")
                .hasRole(roleParentName)
                .hasRole(roleChildName);

        Var entityTypeParentPlayingRole = var().id(entityTypeParent).playsRole(roleParentName);
        entityTypeChild.playsRole(roleChildName);

        return Lists.newArrayList(entityTypeChild, roleTypeParent, roleTypeChild, relationType, entityTypeParentPlayingRole);
    }
}
