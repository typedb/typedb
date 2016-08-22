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
import io.mindmaps.core.Data;
import io.mindmaps.core.MindmapsGraph;

import io.mindmaps.core.implementation.exception.MindmapsValidationException;
import io.mindmaps.core.model.*;
import io.mindmaps.factory.MindmapsClient;
import io.mindmaps.graql.internal.GraqlType;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static io.mindmaps.constants.DataType.ConceptPropertyUnique.ITEM_IDENTIFIER;

/**
 *
 */

public class Analytics {

    public static String keySpace = "mindmapsanalyticstest";
    private String uri = "lxd-cluster2-cassandra3";

    //TODO: get the following names from core
    public static final String DEGREE = "degree";
    public static Set<String> analyticsResourceTypes = Sets.newHashSet(DEGREE);
    public static Set<String> analyticsRelationTypes = Sets.newHashSet(DEGREE);

    /**
     * A reference to the Mindmaps Graph that this subgraph belongs to.
     */
    private final MindmapsGraph graph;

    /**
     * The current transaction in use.
     */
    private MindmapsTransaction transaction;

    /**
     *
     */
    private MindmapsComputer computer;

    /**
     * The concept types that define which instances appear in the subgraph.
     */
    private final Set<Type> types = new HashSet<>();

    /**
     * Create a graph computer from a Mindmaps Graph. The computer operates on all instances in the graph.
     */
    public Analytics() {
        graph = MindmapsClient.getGraph(keySpace);
        transaction = graph.getTransaction();
        computer = MindmapsClient.getGraphComputer();

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

        // fetch all types
        types.addAll(transaction.getMetaType().instances().stream()
                .filter(concept -> !excludedTypes.contains(concept))
                .map(Concept::asType)
                .collect(Collectors.toList()));
    }

    /**
     * Create a graph computer from a Mindmaps Graph. The computer operates on the instances of the types provided in
     * the <code>types</code> argument. All subtypes of the given types are included when deciding whether to include an
     * instance.
     *
     * @param mindmapsGraph the graph that the computer operates on
     * @param types         the set of types the computer will use to filter instances
     */
    public Analytics(MindmapsGraph mindmapsGraph, Set<Type> types) {
        graph = mindmapsGraph;
        transaction = mindmapsGraph.getTransaction();

        // use ako relations to add subtypes of the provided types
        for (Type t : types) {
            this.types.addAll(t.subTypes());
        }
    }

    /**
     * Count the number of instances in the graph.
     *
     * @return the number of instances
     */
    public long count() {
        ComputerResult result = computer.compute(new CountMapReduce());
        Map<String, Long> count = result.memory().get(CountMapReduce.DEFAULT_KEY);
        return count.containsKey(CountMapReduce.DEFAULT_KEY) ? count.get(CountMapReduce.DEFAULT_KEY) : 0L;
    }

    /**
     * Compute the number of relations that each instance takes part in.
     *
     * @return a map from each instance to its degree
     */
    public Map<Instance, Long> degrees() throws ExecutionException, InterruptedException {
        Map<Instance, Long> allDegrees = new HashMap<>();
        ComputerResult result = computer.compute(new DegreeVertexProgram());
        result.graph().traversal().V().forEachRemaining(v -> {
            if (v.keys().contains(DegreeVertexProgram.DEGREE_VALUE_TYPE)) {
                Instance instance = transaction.getInstance(v.value(ITEM_IDENTIFIER.name()));
                allDegrees.put(instance, v.value(DegreeVertexProgram.DEGREE_VALUE_TYPE));
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
    private void degreesAndPersist(String resourceType) throws ExecutionException, InterruptedException {
        insertOntology(resourceType, Data.LONG);
        computer.compute(new DegreeAndPersistVertexProgram());
    }

    public void degreesAndPersist() throws ExecutionException, InterruptedException {
        degreesAndPersist(DEGREE);
    }

    private void insertOntology(String resourceName, Data resourceDataType) {
        ResourceType resource = transaction.putResourceType(resourceName, resourceDataType);
        RoleType degreeOwner = transaction.putRoleType(GraqlType.HAS_RESOURCE_OWNER.getId(resourceName));
        RoleType degreeValue = transaction.putRoleType(GraqlType.HAS_RESOURCE_VALUE.getId(resourceName));
        transaction.putRelationType(GraqlType.HAS_RESOURCE.getId(resourceName))
                .hasRole(degreeOwner)
                .hasRole(degreeValue);

        for (Type type : types) {
            type.playsRole(degreeOwner);
        }
        resource.playsRole(degreeValue);

        try {
            transaction.commit();
        } catch (MindmapsValidationException e) {
            e.printStackTrace();
        }
    }


}
