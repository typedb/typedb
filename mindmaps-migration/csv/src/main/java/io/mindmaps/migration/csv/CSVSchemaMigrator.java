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

package io.mindmaps.migration.csv;


import com.google.common.collect.Lists;
import io.mindmaps.MindmapsGraph;
import io.mindmaps.concept.ResourceType;
import io.mindmaps.engine.loader.Loader;
import io.mindmaps.graql.Var;
import org.apache.commons.csv.CSVParser;

import java.util.Collection;
import java.util.Map;

import static io.mindmaps.graql.Graql.var;

/**
 * Migrator for migrating a CSV schema into a Mindmaps Ontology
 */
public class CSVSchemaMigrator {

    private Namer namer;
    private String entityName;
    private CSVParser parser;
    private MindmapsGraph graph;

    /**
     * Create a CSVSchemaMigrator to migrate the schemas in the given parsers
     */
    public CSVSchemaMigrator(){
        this.namer = new Namer() {};
    }

    /**
     * @param entityName name to be given to the migrated entity
     * @param parser CSVparser of the file to migrate
     */
    public CSVSchemaMigrator configure(String entityName, CSVParser parser){
        this.entityName = entityName;
        this.parser = parser;
        return this;
    }

    /**
     * Set a graph from which to get information
     * @param graph a Mindmaps graph
     */
    public CSVSchemaMigrator graph(MindmapsGraph graph){
        this.graph = graph;
        return this;
    }

    /**
     * Migrate a CSV schema into a Mindmaps ontology
     * @return var patterns representing the migrated Mindmaps ontology
     */
    public Collection<Var> migrate(){
        return migrateEntitySchema(entityName, parser);
    }

    /**
     * Migrate the entire schema into the given loader.
     * @param loader Loader to migrate into.
     */
    public void migrate(Loader loader){
        loader.addToQueue(migrate());
        loader.flush();
        loader.waitToFinish();
    }

    /**
     * Migrate a CSV parser as an entity type
     * @param entityType user-provided type of the entity representing the table
     * @param parser the parser to migrate
     * @return var patterns representing the resource and the entity
     */
    public Collection<Var> migrateEntitySchema(String entityType, CSVParser parser){
        Var type = var().isa("entity-type").id(entityType);

        Collection<Var> collection = Lists.newArrayList(type);

        Map<String, Integer> headers = parser.getHeaderMap();
        headers.keySet().stream()
                .map(header -> migrateAsResource(entityType, header))
                .forEach(collection::addAll);

        return collection;
    }

    /**
     * Migrate a column as a resource type
     * @param ownerType id of the owning type
     * @param otherType  name of the column (becomes the id of the resource type)
     * @return
     */
    public Collection<Var> migrateAsResource(String ownerType, String otherType){
        // create the vars
        Var resourceType = var().id(namer.resourceName(otherType))
                .datatype(ResourceType.DataType.STRING)
                .isa("resource-type");

        Var hasResource = var().id(ownerType).hasResource(namer.resourceName(otherType));

        // insert
        return Lists.newArrayList(resourceType, hasResource);
    }
}
