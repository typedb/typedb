/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.test.graql.reasoner.graphs;

import ai.grakn.Grakn;
import ai.grakn.GraknGraph;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Instance;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.graql.QueryBuilder;

import ai.grakn.migration.base.io.MigrationLoader;
import ai.grakn.migration.csv.CSVMigrator;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.joining;

public class TestGraph {
    protected GraknGraph graknGraph;
    protected final static String filePath = "src/test/graql/";
    private final static String defaultKey = "name";

    protected static ResourceType<String> key;

    public TestGraph(){
        // EmbeddedCassandra has issues dropping keyspaces that start with numbers
        graknGraph = Grakn.factory(Grakn.DEFAULT_URI, "a"+UUID.randomUUID().toString().replaceAll("-", "a")).getGraph();
        graknGraph.showImplicitConcepts(true);
        buildGraph();
        commit();
    }

    public TestGraph(String primaryKeyId, String... files) {
        // EmbeddedCassandra has issues dropping keyspaces that start with numbers
        graknGraph = Grakn.factory(Grakn.DEFAULT_URI, "a"+UUID.randomUUID().toString().replaceAll("-", "a")).getGraph();
        graknGraph.showImplicitConcepts(true);
        if (primaryKeyId != null) addPrimaryKey(primaryKeyId);
        for( String graqlFile : files) loadGraqlFile(graqlFile);
        graknGraph = graph();
        commit();
    }

    public void loadFiles(String... files){
        for( String graqlFile : files) loadGraqlFile(graqlFile);
    }

    public GraknGraph graph(){
        GraknGraph graph = Grakn.factory(Grakn.DEFAULT_URI, graknGraph.getKeyspace()).getGraph();
        graph.showImplicitConcepts(true);
        return graph;
    }

    public static GraknGraph getGraph() {
        return new TestGraph().graph();
    }
    public static GraknGraph getGraph(String primaryKeyId, String... files) {
        return new TestGraph(primaryKeyId, files).graph();
    }

    public void migrateCSV(String templatePath, String dataPath){ migrateCSV(templatePath, dataPath, ',');}
    public void migrateCSV(String templatePath, String dataPath, char separator){
        System.out.println("Migrating " + dataPath);
        try {
            String hsaMatureTemplate = getResourceAsString(templatePath);
            File hsaMatureFile = new File(dataPath);
            MigrationLoader.load(graph().getKeyspace(), new CSVMigrator(hsaMatureTemplate, hsaMatureFile).setSeparator(separator));
        } catch (IOException e){
            throw new RuntimeException(e);
        }
    }

    protected void loadGraqlFile(String fileName) {
        System.out.println("Loading " + fileName);
        if (fileName.isEmpty()) return;

        try (GraknGraph graph = graph()) {
            QueryBuilder qb = graph.graql();
            List<String> lines = Files.readAllLines(Paths.get(filePath + fileName), StandardCharsets.UTF_8);
            String query = lines.stream().reduce("", (s1, s2) -> s1 + "\n" + s2);
            qb.parse(query).execute();
            graph.commit();
        }
        catch (IOException|GraknValidationException e){
            e.printStackTrace();
        }
    }

    protected void commit(){
        try {
            graph().commit();
        } catch (GraknValidationException e) {
            System.out.println(e.getMessage());
        }
    }

    protected void buildGraph() {
        addPrimaryKey(defaultKey);
        buildOntology();
        buildInstances();
        buildRelations();
        buildRules();
    }

    private void addPrimaryKey(String keyName){
        key = graknGraph.putResourceType(keyName, ResourceType.DataType.STRING);
    }

    protected void buildOntology(){}
    protected void buildInstances(){}
    protected void buildRelations(){}
    protected void buildRules(){}

    protected Instance getInstance(String id){
        Set<Instance> instances = graknGraph.getResourcesByValue(id)
                .stream().flatMap(res -> res.ownerInstances().stream()).collect(Collectors.toSet());
        if (instances.size() != 1)
            throw new IllegalStateException("Something wrong, multiple instances with given res value");
        return instances.iterator().next();
    }

    protected Instance putEntity(String id, EntityType type) {
        Instance inst = type.addEntity();
        putResource(inst, graknGraph.getResourceType(key.getName()), id);
        return inst;
    }

    protected <T> void putResource(Instance instance, ResourceType<T> resourceType, T resource) {
        Resource resourceInstance = resourceType.putResource(resource);
        instance.hasResource(resourceInstance);
    }

    public static Path getResource(String resourceName){
        return Paths.get(resourceName);
    }

    public static String getResourceAsString(String resourceName) throws IOException {
        return Files.readAllLines(getResource(resourceName)).stream().collect(joining("\n"));
    }
}
