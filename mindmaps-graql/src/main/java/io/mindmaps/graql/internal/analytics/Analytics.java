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
import io.mindmaps.concept.ResourceType;
import io.mindmaps.concept.RoleType;
import io.mindmaps.concept.Type;
import io.mindmaps.exception.MindmapsValidationException;
import io.mindmaps.Mindmaps;
import io.mindmaps.graql.Pattern;
import io.mindmaps.graql.internal.util.GraqlType;
import io.mindmaps.util.ErrorMessage;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;

import java.util.*;
import java.util.stream.Collectors;

import static io.mindmaps.graql.Graql.or;
import static io.mindmaps.graql.Graql.var;
import static io.mindmaps.graql.Graql.withGraph;
import static io.mindmaps.util.Schema.ConceptProperty.ITEM_IDENTIFIER;

/**
 * OLAP computations that can be applied to a Mindmaps Graph. The current implementation uses the SparkGraphComputer
 * with a Hadoop graph that connects directly to cassandra and de-serialises vertices.
 */

public class Analytics {

    private final String keySpace;
    // TODO: allow user specified resources
    public static final String degree = "degree";

    /**
     * The concept type ids that define which instances appear in the subgraph.
     */
    private final Set<String> subtypes = new HashSet<>();
    private final Map<String, String> resourceTypes = new HashMap<>();
    private final Set<String> statisticsResourceTypes = new HashSet<>();

    /**
     * Create a graph computer from a Mindmaps Graph. The computer operates on all instances in the graph.
     */
    @Deprecated
    public Analytics(String keySpace) {
        this.keySpace = keySpace;

        MindmapsGraph graph = Mindmaps.factory(Mindmaps.DEFAULT_URI, this.keySpace).getGraph();

        // collect resource-types for statistics
        graph.getMetaResourceType().instances()
                .forEach(type ->
                        resourceTypes.put(type.getId(), type.asResourceType().getDataType().getName()));

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

        // collect analytics resource types to exclude
        HashSet<String> analyticsElements =
                Sets.newHashSet(Analytics.degree, GraqlType.HAS_RESOURCE.getId(Analytics.degree));
        analyticsElements.stream()
                .filter(element -> graph.getType(element) != null)
                .map(graph::getType)
                .forEach(excludedTypes::add);

        // fetch all types
        graph.getMetaType().instances().stream()
                .filter(concept -> !excludedTypes.contains(concept))
                .map(Concept::asType)
                .forEach(type -> subtypes.add(type.getId()));

        graph.rollback();

        // add analytics ontology - hard coded for now
        mutateResourceOntology(degree, ResourceType.DataType.LONG);
    }

    /**
     * Create a graph computer from a Mindmaps Graph. The computer operates on the instances of the types provided in
     * the <code>subtypes</code> argument. All subtypes of the given types are included when deciding whether to
     * include an instance.
     *
     * @param subtypes the set of types the computer will use to filter instances
     */
    @Deprecated
    public Analytics(String keySpace, Set<Type> subtypes) {
        this.keySpace = keySpace;
        MindmapsGraph graph = Mindmaps.factory(Mindmaps.DEFAULT_URI, this.keySpace).getGraph();

        // collect resource-types for statistics
        graph.getMetaResourceType().instances()
                .forEach(type ->
                        resourceTypes.put(type.getId(), type.asResourceType().getDataType().getName()));

        // use ako relations to add subtypes of the provided types
        for (Type t : subtypes) {
            t.subTypes().forEach(subtype -> this.subtypes.add(subtype.getId()));
        }

        // add analytics ontology - hard coded for now
        mutateResourceOntology(degree, ResourceType.DataType.LONG);
    }

    /**
     * Create a graph computer from a Mindmaps Graph. The computer operates on the instances of the types provided in
     * the <code>subtypes</code> argument. All subtypes of the given types are included when deciding whether to
     * include an instance.
     *
     * @param subTypeIds                the set of type ids the computer will use to filter instances
     * @param statisticsResourceTypeIds the set of resource type ids statistics will be working on
     */
    public Analytics(String keySpace, Set<String> subTypeIds, Set<String> statisticsResourceTypeIds) {
        this.keySpace = keySpace;
        MindmapsGraph graph = Mindmaps.factory(Mindmaps.DEFAULT_URI, this.keySpace).getGraph();

        // make sure we don't accidentally commit anything
        graph.rollback();

        // fetch all the types
        Set<Type> subtypes = subTypeIds.stream().map((id) -> {
            Type type = graph.getType(id);
            if (type == null) throw new IllegalArgumentException(ErrorMessage.ID_NOT_FOUND.getMessage(id));
            return type;
        }).collect(Collectors.toSet());
        Set<Type> statisticsResourceTypes = statisticsResourceTypeIds.stream().map((id) -> {
            Type type = graph.getType(id);
            if (type == null) throw new IllegalArgumentException(ErrorMessage.ID_NOT_FOUND.getMessage(id));
            return type;
        }).collect(Collectors.toSet());

        // collect resource-types for statistics
        graph.getMetaResourceType().instances()
                .forEach(type -> resourceTypes.put(type.getId(), type.asResourceType().getDataType().getName()));

        if (subtypes.isEmpty()) {

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

            // collect analytics resource types to exclude
            HashSet<String> analyticsElements =
                    Sets.newHashSet(Analytics.degree, GraqlType.HAS_RESOURCE.getId(Analytics.degree));
            analyticsElements.stream()
                    .filter(element -> graph.getType(element) != null)
                    .map(graph::getType)
                    .forEach(excludedTypes::add);

            // fetch all types
            graph.getMetaType().instances().stream()
                    .filter(concept -> !excludedTypes.contains(concept))
                    .map(Concept::asType)
                    .forEach(type -> this.subtypes.add(type.getId()));
        } else {
            for (Type t : subtypes) {
                t.subTypes().forEach(subtype -> this.subtypes.add(subtype.getId()));
            }
        }

        if (!statisticsResourceTypes.isEmpty()) {
            for (Type t : statisticsResourceTypes) {
                t.subTypes().forEach(subtype -> this.statisticsResourceTypes.add(subtype.getId()));
            }
        }

        // add analytics ontology - hard coded for now
        mutateResourceOntology(degree, ResourceType.DataType.LONG);
    }

    /**
     * Count the number of instances in the graph.
     *
     * @return the number of instances
     */
    public long count() {
        MindmapsComputer computer = Mindmaps.factory(Mindmaps.DEFAULT_URI, keySpace).getGraphComputer();
        ComputerResult result = computer.compute(new CountMapReduce(subtypes));
        Map<String, Long> count = result.memory().get(MindmapsMapReduce.MAP_REDUCE_MEMORY_KEY);
        return count.getOrDefault(CountMapReduce.MEMORY_KEY, 0L);
    }

    /**
     * Minimum value of the selected resource-type.
     *
     * @return min
     */
    public Optional<Number> min() {
        String dataType = checkSelectedResourceTypesHaveCorrectDataType(statisticsResourceTypes);
        if (!selectedTypesHaveInstanceInSubgraph(statisticsResourceTypes, subtypes)) return Optional.empty();

        Set<String> allSubtypes = statisticsResourceTypes.stream()
                .map(GraqlType.HAS_RESOURCE::getId).collect(Collectors.toSet());
        allSubtypes.addAll(subtypes);
        allSubtypes.addAll(statisticsResourceTypes);

        MindmapsComputer computer = Mindmaps.factory(Mindmaps.DEFAULT_URI, keySpace).getGraphComputer();
        ComputerResult result = computer.compute(new DegreeVertexProgram(allSubtypes),
                new MinMapReduce(statisticsResourceTypes, dataType));
        Map<String, Number> min = result.memory().get(MindmapsMapReduce.MAP_REDUCE_MEMORY_KEY);
        return Optional.of(min.get(MinMapReduce.MEMORY_KEY));
    }

    /**
     * Maximum value of the selected resource-type.
     *
     * @return max
     */
    public Optional<Number> max() {
        String dataType = checkSelectedResourceTypesHaveCorrectDataType(statisticsResourceTypes);
        if (!selectedTypesHaveInstanceInSubgraph(statisticsResourceTypes, subtypes)) return Optional.empty();

        Set<String> allSubtypes = statisticsResourceTypes.stream()
                .map(GraqlType.HAS_RESOURCE::getId).collect(Collectors.toSet());
        allSubtypes.addAll(subtypes);
        allSubtypes.addAll(statisticsResourceTypes);

        MindmapsComputer computer = Mindmaps.factory(Mindmaps.DEFAULT_URI, keySpace).getGraphComputer();
        ComputerResult result = computer.compute(new DegreeVertexProgram(allSubtypes),
                new MaxMapReduce(statisticsResourceTypes, dataType));
        Map<String, Number> max = result.memory().get(MindmapsMapReduce.MAP_REDUCE_MEMORY_KEY);
        return Optional.of(max.get(MaxMapReduce.MEMORY_KEY));
    }

    /**
     * Sum of values of the selected resource-type.
     *
     * @return sum
     */
    public Optional<Number> sum() {
        String dataType = checkSelectedResourceTypesHaveCorrectDataType(statisticsResourceTypes);
        if (!selectedTypesHaveInstanceInSubgraph(statisticsResourceTypes, subtypes)) return Optional.empty();

        Set<String> allSubtypes = statisticsResourceTypes.stream()
                .map(GraqlType.HAS_RESOURCE::getId).collect(Collectors.toSet());
        allSubtypes.addAll(subtypes);
        allSubtypes.addAll(statisticsResourceTypes);

        MindmapsComputer computer = Mindmaps.factory(Mindmaps.DEFAULT_URI, keySpace).getGraphComputer();
        ComputerResult result = computer.compute(new DegreeVertexProgram(allSubtypes),
                new SumMapReduce(statisticsResourceTypes, dataType));
        Map<String, Number> max = result.memory().get(MindmapsMapReduce.MAP_REDUCE_MEMORY_KEY);
        return Optional.of(max.get(SumMapReduce.MEMORY_KEY));
    }

    /**
     * Compute the mean of instances of the selected resource-type.
     *
     * @return mean
     */
    public Optional<Double> mean() {
        String dataType = checkSelectedResourceTypesHaveCorrectDataType(statisticsResourceTypes);
        if (!selectedTypesHaveInstanceInSubgraph(statisticsResourceTypes, subtypes)) return Optional.empty();

        Set<String> allSubtypes = statisticsResourceTypes.stream()
                .map(GraqlType.HAS_RESOURCE::getId).collect(Collectors.toSet());
        allSubtypes.addAll(subtypes);
        allSubtypes.addAll(statisticsResourceTypes);

        MindmapsComputer computer = Mindmaps.factory(Mindmaps.DEFAULT_URI, keySpace).getGraphComputer();
        ComputerResult result = computer.compute(new DegreeVertexProgram(allSubtypes),
                new MeanMapReduce(statisticsResourceTypes, dataType));
        Map<String, Map<String, Double>> mean = result.memory().get(MindmapsMapReduce.MAP_REDUCE_MEMORY_KEY);
        Map<String, Double> meanPair = mean.get(MeanMapReduce.MEMORY_KEY);
        return Optional.of(meanPair.get(MeanMapReduce.SUM) / meanPair.get(MeanMapReduce.COUNT));
    }

    /**
     * Compute the standard deviation of instances of the selected resource-type.
     *
     * @return standard deviation
     */
    public Optional<Double> std() {
        String dataType = checkSelectedResourceTypesHaveCorrectDataType(statisticsResourceTypes);
        if (!selectedTypesHaveInstanceInSubgraph(statisticsResourceTypes, subtypes)) return Optional.empty();

        Set<String> allSubtypes = statisticsResourceTypes.stream()
                .map(GraqlType.HAS_RESOURCE::getId).collect(Collectors.toSet());
        allSubtypes.addAll(subtypes);
        allSubtypes.addAll(statisticsResourceTypes);

        MindmapsComputer computer = Mindmaps.factory(Mindmaps.DEFAULT_URI, keySpace).getGraphComputer();
        ComputerResult result = computer.compute(new DegreeVertexProgram(allSubtypes),
                new StdMapReduce(statisticsResourceTypes, dataType));
        Map<String, Map<String, Double>> std = result.memory().get(MindmapsMapReduce.MAP_REDUCE_MEMORY_KEY);
        Map<String, Double> stdTuple = std.get(StdMapReduce.MEMORY_KEY);
        double squareSum = stdTuple.get(StdMapReduce.SQUARE_SUM);
        double sum = stdTuple.get(StdMapReduce.SUM);
        double count = stdTuple.get(StdMapReduce.COUNT);
        return Optional.of(Math.sqrt(squareSum / count - (sum / count) * (sum / count)));
    }

    /**
     * Compute the number of relations that each instance takes part in.
     *
     * @return a map from each instance to its degree
     */
    public Map<Instance, Long> degrees() {
        Map<Instance, Long> allDegrees = new HashMap<>();
        MindmapsComputer computer = Mindmaps.factory(Mindmaps.DEFAULT_URI, keySpace).getGraphComputer();
        ComputerResult result = computer.compute(new DegreeVertexProgram(subtypes));
        MindmapsGraph graph = Mindmaps.factory(Mindmaps.DEFAULT_URI, keySpace).getGraph();
        result.graph().traversal().V().forEachRemaining(v -> {
            if (v.keys().contains(DegreeVertexProgram.MEMORY_KEY)) {
                Instance instance = graph.getInstance(v.value(ITEM_IDENTIFIER.name()));
                allDegrees.put(instance, v.value(DegreeVertexProgram.MEMORY_KEY));
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
        if (!Sets.intersection(subtypes, CommonOLAP.analyticsElements).isEmpty()) {
            throw new IllegalStateException(ErrorMessage.ILLEGAL_ARGUMENT_EXCEPTION
                    .getMessage(this.getClass().toString()));
        }
        MindmapsComputer computer = Mindmaps.factory(Mindmaps.DEFAULT_URI, keySpace).getGraphComputer();
        computer.compute(new DegreeAndPersistVertexProgram(keySpace, subtypes));
    }

    /**
     * Compute the number of relations that each instance takes part in and persist this information in the graph. The
     * degree is stored as a resource of type "degree" attached to the relevant instance.
     */
    public void degreesAndPersist() {
        degreesAndPersist(degree);
    }

    /**
     * Add the analytics elements to the ontology of the graph specified in <code>keySpace</code>. The ontology elements
     * are related to the resource type <code>resourceTypeId</code> used to persist data computed by analytics.
     *
     * @param resourceTypeId   the ID of a resource type used to persist information
     * @param resourceDataType the datatype of the resource type
     */
    private void mutateResourceOntology(String resourceTypeId, ResourceType.DataType resourceDataType) {
        MindmapsGraph graph = Mindmaps.factory(Mindmaps.DEFAULT_URI, keySpace).getGraph();

        ResourceType resource = graph.putResourceType(resourceTypeId, resourceDataType);
        RoleType degreeOwner = graph.putRoleType(GraqlType.HAS_RESOURCE_OWNER.getId(resourceTypeId));
        RoleType degreeValue = graph.putRoleType(GraqlType.HAS_RESOURCE_VALUE.getId(resourceTypeId));
        graph.putRelationType(GraqlType.HAS_RESOURCE.getId(resourceTypeId))
                .hasRole(degreeOwner)
                .hasRole(degreeValue);

        for (String type : subtypes) {
            graph.getType(type).playsRole(degreeOwner);
        }
        resource.playsRole(degreeValue);

        try {
            graph.commit();
        } catch (MindmapsValidationException e) {
            throw new RuntimeException(ErrorMessage.ONTOLOGY_MUTATION.getMessage(e.getMessage()), e);
        }

    }

    private String checkSelectedResourceTypesHaveCorrectDataType(Set<String> types) {
        if (types == null || types.isEmpty())
            throw new IllegalStateException(ErrorMessage.ILLEGAL_ARGUMENT_EXCEPTION
                    .getMessage(this.getClass().toString()));

        String dataType = null;
        for (String type : types) {
            // check if the selected type is a resource-type
            if (!resourceTypes.containsKey(type))
                throw new IllegalStateException(ErrorMessage.ILLEGAL_ARGUMENT_EXCEPTION
                        .getMessage(this.getClass().toString()));

            if (dataType == null) {
                // check if the resource-type has data-type LONG or DOUBLE
                dataType = resourceTypes.get(type);

                if (!dataType.equals(ResourceType.DataType.LONG.getName()) &&
                        !dataType.equals(ResourceType.DataType.DOUBLE.getName()))
                    throw new IllegalStateException(ErrorMessage.ILLEGAL_ARGUMENT_EXCEPTION
                            .getMessage(this.getClass().toString()));

            } else {
                // check if all the resource-types have the same data-type
                if (!dataType.equals(resourceTypes.get(type)))
                    throw new IllegalStateException(ErrorMessage.ILLEGAL_ARGUMENT_EXCEPTION
                            .getMessage(this.getClass().toString()));
            }
        }
        return dataType;
    }

    private boolean selectedTypesHaveInstanceInSubgraph(Set<String> statisticsResourceTypes,
                                                        Set<String> subtypes) {

        MindmapsGraph graph = Mindmaps.factory(Mindmaps.DEFAULT_URI, this.keySpace).getGraph();

        List<Pattern> checkResourceTypes = statisticsResourceTypes.stream()
                .map(type -> var("x").has(type)).collect(Collectors.toList());
        List<Pattern> checkSubtypes = subtypes.stream()
                .map(type -> var("x").isa(type)).collect(Collectors.toList());

        return withGraph(graph).match(or(checkResourceTypes), or(checkSubtypes)).ask().execute();
    }
}
