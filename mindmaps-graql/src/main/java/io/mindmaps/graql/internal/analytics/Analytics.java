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
import io.mindmaps.Mindmaps;
import io.mindmaps.MindmapsComputer;
import io.mindmaps.MindmapsGraph;
import io.mindmaps.concept.Concept;
import io.mindmaps.concept.RelationType;
import io.mindmaps.concept.ResourceType;
import io.mindmaps.concept.RoleType;
import io.mindmaps.concept.Type;
import io.mindmaps.exception.MindmapsValidationException;
import io.mindmaps.graql.Pattern;
import io.mindmaps.util.ErrorMessage;
import io.mindmaps.util.Schema;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static io.mindmaps.graql.Graql.or;
import static io.mindmaps.graql.Graql.var;
import static io.mindmaps.graql.Graql.withGraph;

/**
 * OLAP computations that can be applied to a Mindmaps Graph. The current implementation uses the SparkGraphComputer
 * with a Hadoop graph that connects directly to cassandra and de-serialises vertices.
 */

public class Analytics {

    // TODO: allow user specified resources
    public static final String degree = "degree";
    public static final String connectedComponent = "connectedComponent";
    private static final int numberOfOntologyChecks = 10;

    private final String keySpace;

    /**
     * The concept type ids that define which instances appear in the subgraph.
     */
    private final Set<String> subtypes = new HashSet<>();
    private final Map<String, String> resourceTypes = new HashMap<>();
    private final Set<String> statisticsResourceTypes = new HashSet<>();

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

        // make sure we don't accidentally commit anythin
        // TODO: Fix this properly. I.E. Don't run TinkerGraph Tests which hit this line.
        try {
            graph.rollback();
        } catch (UnsupportedOperationException ignored){}

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
            CommonOLAP.analyticsElements.stream()
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
    }

    /**
     * Count the number of instances in the graph.
     *
     * @return the number of instances
     */
    public long count() {
        MindmapsComputer computer = getGraphComputer();
        if (!selectedTypesHaveInstance()) return 0L;
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
        if (!selectedResourceTypesHaveInstance(statisticsResourceTypes)) return Optional.empty();

        Set<String> allSubtypes = statisticsResourceTypes.stream()
                .map(Schema.Resource.HAS_RESOURCE::getId).collect(Collectors.toSet());
        allSubtypes.addAll(subtypes);
        allSubtypes.addAll(statisticsResourceTypes);

        MindmapsComputer computer = getGraphComputer();
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
        if (!selectedResourceTypesHaveInstance(statisticsResourceTypes)) return Optional.empty();

        Set<String> allSubtypes = statisticsResourceTypes.stream()
                .map(Schema.Resource.HAS_RESOURCE::getId).collect(Collectors.toSet());
        allSubtypes.addAll(subtypes);
        allSubtypes.addAll(statisticsResourceTypes);

        MindmapsComputer computer = getGraphComputer();
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
        if (!selectedResourceTypesHaveInstance(statisticsResourceTypes)) return Optional.empty();

        Set<String> allSubtypes = statisticsResourceTypes.stream()
                .map(Schema.Resource.HAS_RESOURCE::getId).collect(Collectors.toSet());
        allSubtypes.addAll(subtypes);
        allSubtypes.addAll(statisticsResourceTypes);

        MindmapsComputer computer = getGraphComputer();
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
        if (!selectedResourceTypesHaveInstance(statisticsResourceTypes)) return Optional.empty();

        Set<String> allSubtypes = statisticsResourceTypes.stream()
                .map(Schema.Resource.HAS_RESOURCE::getId).collect(Collectors.toSet());
        allSubtypes.addAll(subtypes);
        allSubtypes.addAll(statisticsResourceTypes);

        MindmapsComputer computer = getGraphComputer();
        ComputerResult result = computer.compute(new DegreeVertexProgram(allSubtypes),
                new MeanMapReduce(statisticsResourceTypes, dataType));
        Map<String, Map<String, Double>> mean = result.memory().get(MindmapsMapReduce.MAP_REDUCE_MEMORY_KEY);
        Map<String, Double> meanPair = mean.get(MeanMapReduce.MEMORY_KEY);
        return Optional.of(meanPair.get(MeanMapReduce.SUM) / meanPair.get(MeanMapReduce.COUNT));
    }

    /**
     * Compute the median of instances of the selected resource-type.
     *
     * @return median
     */
    public Optional<Number> median() {
        String dataType = checkSelectedResourceTypesHaveCorrectDataType(statisticsResourceTypes);
        if (!selectedResourceTypesHaveInstance(statisticsResourceTypes)) return Optional.empty();

        Set<String> allSubtypes = statisticsResourceTypes.stream()
                .map(Schema.Resource.HAS_RESOURCE::getId).collect(Collectors.toSet());
        allSubtypes.addAll(subtypes);
        allSubtypes.addAll(statisticsResourceTypes);

        MindmapsComputer computer = getGraphComputer();
        ComputerResult result = computer.compute(
                new MedianVertexProgram(allSubtypes, statisticsResourceTypes, dataType));
        return Optional.of(result.memory().get(MedianVertexProgram.MEDIAN));
    }

    /**
     * Compute the standard deviation of instances of the selected resource-type.
     *
     * @return standard deviation
     */
    public Optional<Double> std() {
        String dataType = checkSelectedResourceTypesHaveCorrectDataType(statisticsResourceTypes);
        if (!selectedResourceTypesHaveInstance(statisticsResourceTypes)) return Optional.empty();

        Set<String> allSubtypes = statisticsResourceTypes.stream()
                .map(Schema.Resource.HAS_RESOURCE::getId).collect(Collectors.toSet());
        allSubtypes.addAll(subtypes);
        allSubtypes.addAll(statisticsResourceTypes);

        MindmapsComputer computer = getGraphComputer();
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
     * Compute the median of instances of the selected resource-type.
     *
     * @return median
     */
    public Map<Integer, Set<String>> shortestPath(String startId, String endId) {
        if (!selectedTypesHaveInstance()) return Collections.emptyMap();
        MindmapsComputer computer = getGraphComputer();
        ComputerResult result = computer.compute(new ShortestPathVertexProgram(subtypes, startId, endId),
                new ClusterMemberMapReduce(subtypes, ShortestPathVertexProgram.FOUND_IN_ITERATION));
        Map<Integer, Set<String>> map = result.memory().get(MindmapsMapReduce.MAP_REDUCE_MEMORY_KEY);

        return result.memory().get(MindmapsMapReduce.MAP_REDUCE_MEMORY_KEY);
    }

    /**
     * Compute the number of connected components.
     *
     * @return a map of set, each set contains all the vertex ids belonging to one connected component
     */
    public Map<String, Set<String>> connectedComponents() {
        if (!selectedTypesHaveInstance()) return Collections.emptyMap();
        MindmapsComputer computer = getGraphComputer();
        ComputerResult result = computer.compute(new ConnectedComponentVertexProgram(subtypes),
                new ClusterMemberMapReduce(subtypes, ConnectedComponentVertexProgram.CLUSTER_LABEL));
        return result.memory().get(MindmapsMapReduce.MAP_REDUCE_MEMORY_KEY);
    }

    /**
     * Compute the number of connected components.
     *
     * @return a map of component size
     */
    public Map<String, Long> connectedComponentsSize() {
        if (!selectedTypesHaveInstance()) return Collections.emptyMap();
        MindmapsComputer computer = getGraphComputer();
        ComputerResult result = computer.compute(new ConnectedComponentVertexProgram(subtypes),
                new ClusterSizeMapReduce(subtypes, ConnectedComponentVertexProgram.CLUSTER_LABEL));
        return result.memory().get(MindmapsMapReduce.MAP_REDUCE_MEMORY_KEY);
    }

    /**
     * Compute the connected components and persist the component labels
     */
    public Map<String, Long> connectedComponentsAndPersist() {
        if (!Sets.intersection(subtypes, CommonOLAP.analyticsElements).isEmpty()) {
            throw new IllegalStateException(ErrorMessage.ILLEGAL_ARGUMENT_EXCEPTION
                    .getMessage(this.getClass().toString()));
        }
        if (selectedTypesHaveInstance()) {
            mutateResourceOntology(connectedComponent, ResourceType.DataType.STRING);
            waitOnMutateResourceOntology(connectedComponent);

            MindmapsComputer computer = getGraphComputer();
            ComputerResult result = computer.compute(new ConnectedComponentVertexProgram(subtypes, keySpace),
                    new ClusterSizeMapReduce(subtypes, ConnectedComponentVertexProgram.CLUSTER_LABEL));
            return result.memory().get(MindmapsMapReduce.MAP_REDUCE_MEMORY_KEY);
        }
        return Collections.emptyMap();
    }

    /**
     * Compute the number of relations that each instance takes part in.
     *
     * @return a map from each instance to its degree
     */
    public Map<Long, Set<String>> degrees() {
        MindmapsComputer computer = getGraphComputer();
        ComputerResult result = computer.compute(new DegreeVertexProgram(subtypes),
                new DegreeDistributionMapReduce(subtypes));
        return result.memory().get(MindmapsMapReduce.MAP_REDUCE_MEMORY_KEY);
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

        mutateResourceOntology(resourceType, ResourceType.DataType.LONG);
        waitOnMutateResourceOntology(resourceType);

        MindmapsComputer computer = getGraphComputer();
        computer.compute(new DegreeAndPersistVertexProgram(subtypes, keySpace));
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

        for (String type : subtypes) {
            graph.getType(type).hasResource(resource);
        }

        try {
            graph.commit();
        } catch (MindmapsValidationException e) {
            throw new RuntimeException(ErrorMessage.ONTOLOGY_MUTATION.getMessage(e.getMessage()), e);
        }

    }

    /**
     * Ensures that the ontology mutation performed by analytics is persisted before proceeding. In rare cases, such as
     * underpowered machines, or ones with high lag to engine, the ontology mutation is not persisted in time for the
     * OLAP task to proceed. This method forces analytics to wait and ensure the ontology is persisted before
     * proceeding.
     *
     * @param resourceTypeId the resource the plays role edges must point to
     */
    private void waitOnMutateResourceOntology(String resourceTypeId) {
        MindmapsGraph graph = Mindmaps.factory(Mindmaps.DEFAULT_URI, keySpace).getGraph();

        for (int i = 0; i < numberOfOntologyChecks; i++) {
            boolean isOntologyComplete = true;
            // TODO: Fix this properly. I.E. Don't run TinkerGraph Tests which hit this line.
            try {
                graph.rollback();
            } catch (UnsupportedOperationException ignored){}

            ResourceType resource = graph.getResourceType(resourceTypeId);
            if (resource == null) continue;
            RoleType degreeOwner = graph.getRoleType(Schema.Resource.HAS_RESOURCE_OWNER.getId(resourceTypeId));
            if (degreeOwner == null) continue;
            RoleType degreeValue = graph.getRoleType(Schema.Resource.HAS_RESOURCE_VALUE.getId(resourceTypeId));
            if (degreeValue == null) continue;
            RelationType relationType = graph.getRelationType(Schema.Resource.HAS_RESOURCE.getId(resourceTypeId));
            if (relationType == null) continue;

            for (String type : subtypes) {
                Collection<RoleType> roles = graph.getType(type).playsRoles();
                if (!roles.contains(degreeOwner)) {
                    isOntologyComplete = false;
                    break;
                }
            }

            if (isOntologyComplete) return;
        }
        throw new RuntimeException(
                ErrorMessage.ONTOLOGY_MUTATION
                        .getMessage("Failed to confirm ontology is present after mutation."));
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

    private boolean selectedResourceTypesHaveInstance(Set<String> statisticsResourceTypes) {

        MindmapsGraph graph = Mindmaps.factory(Mindmaps.DEFAULT_URI, this.keySpace).getGraph();

        List<Pattern> checkResourceTypes = statisticsResourceTypes.stream()
                .map(type -> var("x").has(type)).collect(Collectors.toList());
        List<Pattern> checkSubtypes = subtypes.stream()
                .map(type -> var("x").isa(type)).collect(Collectors.toList());

        return withGraph(graph).match(or(checkResourceTypes), or(checkSubtypes)).ask().execute();
    }

    private boolean selectedTypesHaveInstance() {
        if (subtypes.isEmpty()) return false;

        MindmapsGraph graph = Mindmaps.factory(Mindmaps.DEFAULT_URI, this.keySpace).getGraph();

        List<Pattern> checkSubtypes = subtypes.stream()
                .map(type -> var("x").isa(type)).collect(Collectors.toList());

        return withGraph(graph).match(or(checkSubtypes)).ask().execute();
    }

    protected MindmapsComputer getGraphComputer() {
        return Mindmaps.factory(Mindmaps.DEFAULT_URI, keySpace).getGraphComputer();
    }
}
