/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs
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

import ai.grakn.Mindmaps;
import ai.grakn.MindmapsGraph;
import ai.grakn.concept.Instance;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.Mindmaps;
import ai.grakn.MindmapsGraph;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Instance;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.RoleType;
import ai.grakn.exception.MindmapsValidationException;
import ai.grakn.graql.QueryBuilder;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

public class TestGraph {
    protected MindmapsGraph mindmaps;
    private final static String filePath = "src/test/graql/";
    private final static String defaultKey = "name";

    protected static RoleType hasKeyTarget;
    private static RelationType hasKeyRelation;
    private static ResourceType<String> key;
    private static RoleType hasKeyValue;

    public TestGraph(){
        mindmaps = Mindmaps.factory(Mindmaps.IN_MEMORY, UUID.randomUUID().toString().replaceAll("-", "a")).getGraph();
        buildGraph();
        commit();
    }

    public TestGraph(String primaryKeyId, String... files) {
        mindmaps = Mindmaps.factory(Mindmaps.IN_MEMORY, UUID.randomUUID().toString().replaceAll("-", "a")).getGraph();
        addPrimaryKey(primaryKeyId);
        for( String graqlFile : files) loadGraqlFile(graqlFile);
        commit();
    }

    public MindmapsGraph graph(){ return mindmaps;}

    public static MindmapsGraph getGraph() {
        return new TestGraph().graph();
    }
    public static MindmapsGraph getGraph(String primaryKeyId, String... files) {
        return new TestGraph(primaryKeyId, files).graph();
    }

    protected void loadGraqlFile(String fileName) {
        System.out.println("Loading " + fileName);
        if (fileName.isEmpty()) return;

        QueryBuilder qb = mindmaps.graql();
        try {
            List<String> lines = Files.readAllLines(Paths.get(filePath + fileName), StandardCharsets.UTF_8);
            String query = lines.stream().reduce("", (s1, s2) -> s1 + "\n" + s2);
            qb.parse(query).execute();
        }
        catch (IOException e){
            e.printStackTrace();
        }
    }

    protected void commit(){
        try {
            mindmaps.commit();
        } catch (MindmapsValidationException e) {
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
        hasKeyTarget = mindmaps.putRoleType("has-" + keyName + "-owner");
        hasKeyValue = mindmaps.putRoleType("has-" + keyName + "-value");
        hasKeyRelation = mindmaps.putRelationType("has-" + keyName)
                .hasRole(hasKeyTarget).hasRole(hasKeyValue);
        key = mindmaps.putResourceType(keyName, ResourceType.DataType.STRING).playsRole(hasKeyValue);
    }

    protected void buildOntology(){}
    protected void buildInstances(){}
    protected void buildRelations(){}
    protected void buildRules(){}

    protected Instance getInstance(String id){
        Set<Instance> instances = mindmaps.getResourcesByValue(id)
                .stream().flatMap(res -> res.ownerInstances().stream()).collect(Collectors.toSet());
        if (instances.size() != 1)
            throw new IllegalStateException("Something wrong, multiple instances with given res value");
        return instances.iterator().next();
    }

    protected Instance putEntity(String id, EntityType type) {
        Instance inst = mindmaps.addEntity(type);
        putResource(inst, key, id, hasKeyRelation, hasKeyTarget, hasKeyValue);
        return inst;
    }

    protected <T> void putResource(Instance instance, ResourceType<T> resourceType, T resource, RelationType relationType,
                                        RoleType targetRole, RoleType valueRole) {
        Resource resourceInstance = mindmaps.putResource(resource, resourceType);
        mindmaps.addRelation(relationType)
                .putRolePlayer(targetRole, instance)
                .putRolePlayer(valueRole, resourceInstance);
    }
}
