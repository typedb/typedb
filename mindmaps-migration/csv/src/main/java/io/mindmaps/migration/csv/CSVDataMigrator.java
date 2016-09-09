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

import io.mindmaps.MindmapsGraph;
import io.mindmaps.engine.loader.Loader;
import io.mindmaps.graql.Var;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static io.mindmaps.graql.Graql.var;

/**
 * The CSV data migrator will migrate all of the data in a CSV file into Mindmaps Graql var patters, to be
 * imported into a graph as the user sees fit.
 *
 * This class implements Iterator.
 */
public class CSVDataMigrator implements Iterable<Collection<Var>> {

    private Namer namer;
    private MindmapsGraph graph;

    private String entityName;
    private Iterator<CSVRecord> records;
    private Map<String, Integer> headers;

    public CSVDataMigrator(){
        this.namer = new io.mindmaps.migration.csv.Namer() {};
    }

    /**
     * @param entityName name to be given to the migrated entity
     * @param parser CSVparser of the file to migrate
     */
    public CSVDataMigrator configure(String entityName, CSVParser parser){
        this.entityName = entityName;
        this.records = parser.iterator();
        this.headers = parser.getHeaderMap();
        return this;
    }

    /**
     * Set a graph from which to get information
     * @param graph a Mindmaps graph
     */
    public CSVDataMigrator graph(MindmapsGraph graph){
        this.graph = graph;
        return this;
    }

    /**
     * Migrate a CSV schema into a Mindmaps ontology
     * @return var patterns representing the migrated Mindmaps ontology
     */
    public Collection<Var> migrate(){
        Collection<Var> collection = new HashSet<>();

        Iterator<Collection<Var>> iterator = iterator();

        while(iterator.hasNext()){
            collection.addAll(iterator.next());
        }
        return collection;
    }

    /**
     * Migrate the entire data file into the given loader.
     * @param loader Loader to migrate into.
     */
    public void migrate(Loader loader){
        Iterator<Collection<Var>> iterator = iterator();
        while(iterator.hasNext()){
            loader.addToQueue(iterator.next());
        }

        loader.flush();
        loader.waitToFinish();
    }

    public Iterator<Collection<Var>> iterator(){

        return new Iterator<Collection<Var>>() {
            /**
             * Returns true if the CSVParser has more rows to process and the last CSVParser has not been completed.
             * @return true if the iteration has more elements
             */
            @Override
            public boolean hasNext() {
                return records.hasNext();
            }

            /**
             * Returns the migrated result of the next iteration (i.e. row)
             * @return collection of vars representing a migrated row of the CSVParser
             */
            @Override
            public Collection<Var> next() {
                return migrateEntity(entityName, records.next());
            }

            /**
             * This operation is not supported.
             */
            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    /**
     * Migrate one CSVRecord (row) as an entity (creates the entity). Migrates all of its resources.
     * @param type the entity type of the row
     * @param record the CSVRecord (row) to migrate
     * @return collection of var patterns representing the migrated entity, its relations and resources
     */
    private Collection<Var> migrateEntity(String type, CSVRecord record){
        Var instance = var().isa(type).id(UUID.randomUUID().toString());

        Collection<Var> vars = migrateResources(instance, record);
        vars.add(instance);

        return vars;
    }

    /**
     * Migrate all the resources of one Var instance.
     * @param instance var instance the resources belong to
     * @param record the CSVRecord (row) to migrate
     * @return collection of var patterns representing the migrated resources
     */
    private Collection<Var> migrateResources(Var instance, CSVRecord record){
        List<Var> vars = new ArrayList<>();
        for(String resourceType:headers.keySet()){
            String resourceValue = record.get(resourceType);

            vars.addAll(migrateAsResource(instance, resourceType, resourceValue));
        }
        return vars;
    }

    /**
     * Migrate one resource to vars.
     * @param instance var instance the resource belongs to
     * @param resourceType the type of the resource to be created, as specified by the CSVHeader and CSVMapper
     * @param resourceValue the value of the resource to be created
     * @return collection of var representing the migrated resource
     */
    private Collection<Var> migrateAsResource(Var instance, String resourceType, Object resourceValue){
        if(resourceValue instanceof String && resourceValue.toString().isEmpty()){
            return Collections.emptyList();
        }

        // create the relation to insert
        Var has = var().id(id(instance))
                .has(namer.resourceName(resourceType), resourceValue);

        return Collections.singleton(has);
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
