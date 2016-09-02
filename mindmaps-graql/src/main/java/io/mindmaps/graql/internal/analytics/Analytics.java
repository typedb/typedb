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
import io.mindmaps.MindmapsTransaction;
import io.mindmaps.constants.DataType;
import io.mindmaps.constants.ErrorMessage;
import io.mindmaps.core.Data;
import io.mindmaps.core.MindmapsGraph;

import io.mindmaps.core.implementation.exception.MindmapsValidationException;
import io.mindmaps.core.model.*;
import io.mindmaps.factory.MindmapsClient;
import io.mindmaps.graql.internal.GraqlType;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static io.mindmaps.constants.DataType.ConceptPropertyUnique.ITEM_IDENTIFIER;

/**
 * OLAP computations that can be applied to a Mindmaps Graph. The current implementation uses the SparkGraphComputer
 * with a Hadoop graph that connects directly to cassandra and de-serialises vertices.
 */

public class Analytics {

    public final String keySpace;
    public static final String TYPE = DataType.ConceptMeta.TYPE.getId();

    public static final String degree = "degree";
    private static Set<String> analyticsElements =
            Sets.newHashSet(degree, GraqlType.HAS_RESOURCE.getId(degree));

    /**
     * A reference to the Mindmaps Graph that this subgraph belongs to.
     */
    private final MindmapsGraph graph;

    /**
     * The graph computer.
     */
    private final MindmapsComputer computer;

    /**
     * The concept types that define which instances appear in the subgraph.
     */
    private final Set<Type> allTypes = new HashSet<>();

    /**
     * Create a graph computer from a Mindmaps Graph. The computer operates on all instances in the graph.
     */
    public Analytics(String keySpace) {
        this.keySpace = keySpace;
        graph = MindmapsClient.getGraph(this.keySpace);
        MindmapsTransaction transaction = graph.getTransaction();
        computer = MindmapsClient.getGraphComputer(this.keySpace);

        // collect meta-types to exclude them as they do not have instances
        Set<Concept> excludedTypes = new HashSet<>();
        excludedTypes.add(transaction.getMetaType());
        excludedTypes.add(transaction.getMetaEntityType());
        excludedTypes.add(transaction.getMetaRelationType());
        excludedTypes.add(transaction.getMetaResourceType());
        excludedTypes.add(transaction.getMetaRoleType());
        excludedTypes.add(transaction.getMetaRuleType());

        // collect role-types to exclude them because the user does not see castings
        excludedTypes.addAll(transaction.getMetaRoleType().instances());
        excludedTypes.addAll(transaction.getMetaRuleType().instances());

        // collect analytics resource type to exclude
        analyticsElements.stream()
                .filter(element -> transaction.getType(element) != null)
                .map(transaction::getType)
                .forEach(excludedTypes::add);

        // fetch all types
        allTypes.addAll(transaction.getMetaType().instances().stream()
                .filter(concept -> !excludedTypes.contains(concept))
                .map(Concept::asType)
                .collect(Collectors.toList()));
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
        graph = MindmapsClient.getGraph(this.keySpace);
        computer = MindmapsClient.getGraphComputer(this.keySpace);

        // use ako relations to add subtypes of the provided types
        for (Type t : types) {
            this.allTypes.addAll(t.subTypes());
        }
    }

    /**
     * Count the number of instances in the graph.
     *
     * @return the number of instances
     */
    public long count() {
        ComputerResult result = computer.compute(new CountMapReduce(allTypes));
        Map<String, Long> count = result.memory().get(CountMapReduce.DEFAULT_MEMORY_KEY);
        return count.containsKey(CountMapReduce.DEFAULT_MEMORY_KEY) ? count.get(CountMapReduce.DEFAULT_MEMORY_KEY) : 0L;
    }

    /**
     * Compute the number of relations that each instance takes part in.
     *
     * @return a map from each instance to its degree
     */
    public Map<Instance, Long> degrees() {
        Map<Instance, Long> allDegrees = new HashMap<>();
        ComputerResult result = computer.compute(new DegreeVertexProgram(allTypes));
        result.graph().traversal().V().forEachRemaining(v -> {
            if (v.keys().contains(DegreeVertexProgram.DEGREE)) {
                Instance instance = graph.getTransaction().getInstance(v.value(ITEM_IDENTIFIER.name()));
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
        MindmapsTransaction transaction= graph.getTransaction();
        insertOntology(resourceType, Data.LONG);
        ComputerResult result = computer.compute(new DegreeAndPersistVertexProgram(keySpace,allTypes));
        result.graph().traversal().V().forEachRemaining(v -> {
//            System.out.println("v.keys() = " + v.keys());

            if (v.keys().contains(DegreeAndPersistVertexProgram.OLD_ASSERTION_ID)) {
                System.out.println("v.value(DegreeAndPersistVertexProgram.OLD_ASSERTION_ID) = " +
                        v.value(DegreeAndPersistVertexProgram.OLD_ASSERTION_ID));

                transaction.getTinkerTraversal().V().has(ITEM_IDENTIFIER.name(),
                        v.value(DegreeAndPersistVertexProgram.OLD_ASSERTION_ID).toString()).bothE()
                        .forEachRemaining(edge -> edge.remove());
                transaction.getRelation(v.value(DegreeAndPersistVertexProgram.OLD_ASSERTION_ID)).delete();


            }
        });

        try {
            transaction.commit();
        } catch (MindmapsValidationException e) {
            e.printStackTrace();
        }
    }

    public void degreesAndPersist() throws ExecutionException, InterruptedException {
        degreesAndPersist(degree);
    }

    private void insertOntology(String resourceTypeId, Data resourceDataType) {
        MindmapsTransaction transaction = graph.getTransaction();
        ResourceType resource = transaction.putResourceType(resourceTypeId, resourceDataType);
        RoleType degreeOwner = transaction.putRoleType(GraqlType.HAS_RESOURCE_OWNER.getId(resourceTypeId));
        RoleType degreeValue = transaction.putRoleType(GraqlType.HAS_RESOURCE_VALUE.getId(resourceTypeId));
        transaction.putRelationType(GraqlType.HAS_RESOURCE.getId(resourceTypeId))
                .hasRole(degreeOwner)
                .hasRole(degreeValue);

        for (Type type : allTypes) {
            type.playsRole(degreeOwner);
        }
        resource.playsRole(degreeValue);

        try {
            transaction.commit();
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
        return Analytics.analyticsElements.contains(getVertextType(vertex));
    }

    public static String persistResource(MindmapsGraph mindmapsGraph, Vertex vertex,
                                         String resourceName, long value) {
        MindmapsTransaction transaction = mindmapsGraph.getTransaction();

        ResourceType<Long> resourceType = transaction.getResourceType(resourceName);
        RoleType resourceOwner = transaction.getRoleType(GraqlType.HAS_RESOURCE_OWNER.getId(resourceName));
        RoleType resourceValue = transaction.getRoleType(GraqlType.HAS_RESOURCE_VALUE.getId(resourceName));
        RelationType relationType = transaction.getRelationType(GraqlType.HAS_RESOURCE.getId(resourceName));

        Instance instance =
                transaction.getInstance(vertex.value(DataType.ConceptPropertyUnique.ITEM_IDENTIFIER.name()));

        //TODO: remove the deletion of resource. This should be done by core.
        List<Relation> relations = instance.relations(resourceOwner).stream()
                .filter(relation -> relation.rolePlayers().size() == 2)
                .filter(relation -> relation.rolePlayers().containsKey(resourceValue) &&
                        relation.rolePlayers().get(resourceValue).type().getId().equals(resourceName))
                .collect(Collectors.toList());

        if (relations.isEmpty()) {
            Resource<Long> resource = transaction.putResource(value, resourceType);

            transaction.addRelation(relationType)
                    .putRolePlayer(resourceOwner, instance)
                    .putRolePlayer(resourceValue, resource);

            while (true) {
                try {
                    transaction.commit();
                    break;
                } catch (MindmapsValidationException e) {
                    throw new RuntimeException(e);
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println("RETRYING");
                }
            }
            return null;
        }

        relations = relations.stream()
                .filter(relation ->
                        (long) relation.rolePlayers().get(resourceValue).asResource().getValue() != value)
                .collect(Collectors.toList());

        if (!relations.isEmpty()) {
            String oldAssertionId = relations.get(0).getId();

            while (true) {
                Resource<Long> resource = transaction.putResource(value, resourceType);

                transaction.addRelation(relationType)
                        .putRolePlayer(resourceOwner, instance)
                        .putRolePlayer(resourceValue, resource);
                try {
                    transaction.commit();
                    break;
                } catch (MindmapsValidationException e) {
                    throw new RuntimeException(e);
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println("RETRYING");
                }
            }

            return oldAssertionId;
        } else {
            return null;
        }
    }

    public static void deleteOldResourceAssertion(MindmapsGraph mindmapsGraph, Vertex vertex,
                                                  String resourceName, long value) throws MindmapsValidationException {

        MindmapsTransaction transaction = mindmapsGraph.getTransaction();

        Instance instance =
                transaction.getInstance(vertex.value(DataType.ConceptPropertyUnique.ITEM_IDENTIFIER.name()));

        RoleType resourceOwner = transaction.getRoleType(GraqlType.HAS_RESOURCE_OWNER.getId(resourceName));
        RoleType resourceValue = transaction.getRoleType(GraqlType.HAS_RESOURCE_VALUE.getId(resourceName));

        instance.relations(resourceOwner).stream()
                .filter(relation -> relation.rolePlayers().size() == 2)
                .filter(relation -> relation.rolePlayers().containsKey(resourceValue) &&
                        relation.rolePlayers().get(resourceValue).type().getId().equals(resourceName) &&
                        (long) relation.rolePlayers().get(resourceValue).asResource().getValue() == value)
                .forEach(Concept::delete);

        transaction.commit();
    }

    public static String getVertextType(Vertex vertex) {
        return vertex.value(DataType.ConceptProperty.TYPE.name());
    }
}
