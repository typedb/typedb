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

package io.mindmaps.graql.internal.analytics;

import com.google.common.collect.Sets;
import io.mindmaps.MindmapsComputer;
import io.mindmaps.MindmapsGraph;
import io.mindmaps.concept.Concept;
import io.mindmaps.concept.Instance;
import io.mindmaps.concept.Relation;
import io.mindmaps.concept.RelationType;
import io.mindmaps.concept.Resource;
import io.mindmaps.concept.ResourceType;
import io.mindmaps.concept.RoleType;
import io.mindmaps.concept.Type;
import io.mindmaps.exception.MindmapsValidationException;
import io.mindmaps.factory.MindmapsClient;
import io.mindmaps.graql.internal.util.GraqlType;
import io.mindmaps.util.ErrorMessage;
import io.mindmaps.util.Schema;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static io.mindmaps.util.Schema.ConceptProperty.ITEM_IDENTIFIER;

/**
 * OLAP computations that can be applied to a Mindmaps Graph. The current implementation uses the SparkGraphComputer
 * with a Hadoop graph that connects directly to cassandra and de-serialises vertices.
 */

public class Analytics {

    public final String keySpace;
    public static final String TYPE = Schema.MetaType.TYPE.getId();

    public static final String degree = "degree";
    private static Set<String> analyticsElements =
            Sets.newHashSet(degree, GraqlType.HAS_RESOURCE.getId(degree));

    /**
     * The concept type ids that define which instances appear in the subgraph.
     */
    private final Set<String> allTypes = new HashSet<>();

    /**
     * Create a graph computer from a Mindmaps Graph. The computer operates on all instances in the graph.
     */
    public Analytics(String keySpace) {
        this.keySpace = keySpace;
        MindmapsGraph graph = MindmapsClient.getGraph(this.keySpace);

        // collect meta-types to exclude them as they do not have instances
        Set<Concept> excludedTypes = new HashSet<>();
        excludedTypes.add(graph.getMetaType());
        excludedTypes.add(graph.getMetaEntityType());
        excludedTypes.add(graph.getMetaRelationType());
        excludedTypes.add(graph.getMetaResourceType());
        excludedTypes.add(graph.getMetaRoleType());
        excludedTypes.add(graph.getMetaRuleType());

        // collect role-types to exclude them because the user does not see castings
        excludedTypes.addAll(graph.getMetaRoleType().instances());
        excludedTypes.addAll(graph.getMetaRuleType().instances());

        // collect analytics resource type to exclude
        analyticsElements.stream()
                .filter(element -> graph.getType(element) != null)
                .map(graph::getType)
                .forEach(excludedTypes::add);

        // fetch all types
        graph.getMetaType().instances().stream()
                .filter(concept -> !excludedTypes.contains(concept))
                .map(Concept::asType)
                .collect(Collectors.toList()).forEach(type -> allTypes.add(type.getId()));

        graph.rollback();
    }

    /**
     * Create a graph computer from a Mindmaps Graph. The computer operates on the instances of the types provided in
     * the <code>types</code> argument. All subtypes of the given types are included when deciding whether to include an
     * instance.
     *
     * @param types the set of types the computer will use to filter instances
     */
    public Analytics(String keySpace, Set<Type> types) {
        this.keySpace = keySpace;

        // use ako relations to add subtypes of the provided types
        for (Type t : types) {
            t.subTypes().forEach(subtype -> allTypes.add(subtype.getId()));
        }
    }

    /**
     * Count the number of instances in the graph.
     *
     * @return the number of instances
     */
    public long count() {
        MindmapsComputer computer = MindmapsClient.getGraphComputer(keySpace);
        ComputerResult result = computer.compute(new CountMapReduce(allTypes));
        Map<String, Long> count = result.memory().get(MindmapsMapReduce.MAP_REDUCE_MEMORY_KEY);
        return count.getOrDefault(CountMapReduce.MEMORY_KEY,0L);
    }

    /**
     * Compute the number of relations that each instance takes part in.
     *
     * @return a map from each instance to its degree
     */
    public Map<Instance, Long> degrees() {
        Map<Instance, Long> allDegrees = new HashMap<>();
        MindmapsComputer computer = MindmapsClient.getGraphComputer(keySpace);
        ComputerResult result = computer.compute(new DegreeVertexProgram(allTypes));
        MindmapsGraph graph = MindmapsClient.getGraph(keySpace);
        result.graph().traversal().V().forEachRemaining(v -> {
            if (v.keys().contains(DegreeVertexProgram.DEGREE)) {
                Instance instance = graph.getInstance(v.value(ITEM_IDENTIFIER.name()));
                allDegrees.put(instance, v.value(DegreeVertexProgram.DEGREE));
            }
        });
        return allDegrees;
    }

    /**
     * Compute the number of relations that each instance takes part in and persist this information in the graph. The
     * degree is stored as a resource attached to the relevant instance.
     *
     * @param resourceType the type of the resource that will contain the degree
     */
    private void degreesAndPersist(String resourceType) {
        insertOntology(resourceType, ResourceType.DataType.LONG);
        MindmapsComputer computer = MindmapsClient.getGraphComputer(keySpace);
        computer.compute(new DegreeAndPersistVertexProgram(keySpace, allTypes));
    }

    /**
     * Compute the number of relations that each instance takes part in and persist this information in the graph. The
     * degree is stored as a resource of type "degree" attached to the relevant instance.
     */
    public void degreesAndPersist() throws ExecutionException, InterruptedException {
        degreesAndPersist(degree);
    }

    private void insertOntology(String resourceTypeId, ResourceType.DataType resourceDataType) {
        MindmapsGraph graph = MindmapsClient.getGraph(keySpace);
        ResourceType resource = graph.putResourceType(resourceTypeId, resourceDataType);
        RoleType degreeOwner = graph.putRoleType(GraqlType.HAS_RESOURCE_OWNER.getId(resourceTypeId));
        RoleType degreeValue = graph.putRoleType(GraqlType.HAS_RESOURCE_VALUE.getId(resourceTypeId));
        graph.putRelationType(GraqlType.HAS_RESOURCE.getId(resourceTypeId))
                .hasRole(degreeOwner)
                .hasRole(degreeValue);

        for (String type : allTypes) {
            graph.getType(type).playsRole(degreeOwner);
        }
        resource.playsRole(degreeValue);

        try {
            graph.commit();
        } catch (MindmapsValidationException e) {
            throw new RuntimeException(ErrorMessage.ONTOLOGY_MUTATION.getMessage(e.getMessage()),e);
        }

        //TODO: need a proper way to store this information
        addAnalyticsElements(resourceTypeId);
    }

    private static void addAnalyticsElements(String resourceTypeId) {
        if (analyticsElements == null) {
            analyticsElements = new HashSet<>();
        }
        analyticsElements.add(resourceTypeId);
        analyticsElements.add(GraqlType.HAS_RESOURCE.getId(resourceTypeId));
    }

    public static boolean isAnalyticsElement(Vertex vertex) {
        return Analytics.analyticsElements.contains(getVertexType(vertex));
    }

    public static String persistResource(String keySpace, Vertex vertex,
                                         String resourceName, long value) {
        MindmapsGraph mindmapsGraph = MindmapsClient.getGraphBatchLoading(keySpace);
        ResourceType<Long> resourceType = mindmapsGraph.getResourceType(resourceName);
        RoleType resourceOwner = mindmapsGraph.getRoleType(GraqlType.HAS_RESOURCE_OWNER.getId(resourceName));
        RoleType resourceValue = mindmapsGraph.getRoleType(GraqlType.HAS_RESOURCE_VALUE.getId(resourceName));
        RelationType relationType = mindmapsGraph.getRelationType(GraqlType.HAS_RESOURCE.getId(resourceName));

        Instance instance =
                mindmapsGraph.getInstance(vertex.value(Schema.ConceptProperty.ITEM_IDENTIFIER.name()));

        List<Relation> relations = instance.relations(resourceOwner).stream()
                .filter(relation -> relation.rolePlayers().size() == 2)
                .filter(relation -> relation.rolePlayers().containsKey(resourceValue) &&
                        relation.rolePlayers().get(resourceValue).type().getId().equals(resourceName))
                .collect(Collectors.toList());

        if (relations.isEmpty()) {
            Resource<Long> resource = mindmapsGraph.putResource(value, resourceType);

            mindmapsGraph.addRelation(relationType)
                    .putRolePlayer(resourceOwner, instance)
                    .putRolePlayer(resourceValue, resource);

            try {
                mindmapsGraph.commit();
            } catch (Exception e) {
                throw new RuntimeException(ErrorMessage.BULK_PERSIST.getMessage(resourceType,e.getMessage()),e);
            }
            return null;
        }

        relations = relations.stream()
                .filter(relation ->
                        (long) relation.rolePlayers().get(resourceValue).asResource().getValue() != value)
                .collect(Collectors.toList());

        if (!relations.isEmpty()) {
            String oldAssertionId = relations.get(0).getId();

            Resource<Long> resource = mindmapsGraph.putResource(value, resourceType);

            mindmapsGraph.addRelation(relationType)
                    .putRolePlayer(resourceOwner, instance)
                    .putRolePlayer(resourceValue, resource);
            try {
                mindmapsGraph.commit();
            } catch (Exception e) {
                throw new RuntimeException(ErrorMessage.BULK_PERSIST.getMessage(resourceType,e.getMessage()),e);
            }

            return oldAssertionId;
        } else {
            return null;
        }
    }

    public static String getVertexType(Vertex vertex) {
        return vertex.value(Schema.ConceptProperty.TYPE.name());
    }
}
