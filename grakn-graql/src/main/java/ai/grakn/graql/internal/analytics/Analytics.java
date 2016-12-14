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

package ai.grakn.graql.internal.analytics;

import ai.grakn.Grakn;
import ai.grakn.GraknComputer;
import ai.grakn.GraknGraph;
import ai.grakn.concept.Concept;
import ai.grakn.concept.Instance;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.Type;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.RelationType;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.graql.Pattern;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.Schema;
import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static ai.grakn.graql.Graql.or;
import static ai.grakn.graql.Graql.var;
import static ai.grakn.graql.internal.analytics.CommonOLAP.analyticsElements;

/**
 * OLAP computations that can be applied to a Grakn Graph. The current implementation uses the SparkGraphComputer
 * with a Hadoop graph that connects directly to cassandra and de-serialises vertices.
 */
@Deprecated
public class Analytics {

    // TODO: allow user specified resources
    public static final String degree = "degree";
    public static final String connectedComponent = "connectedComponent";
    private static final int numberOfOntologyChecks = 10;
    private static final Logger LOGGER = LoggerFactory.getLogger(Analytics.class);

    private final String keySpace;

    /**
     * The concept type ids that define which instances appear in the subgraph.
     */
    private final Set<String> subtypeNames = new HashSet<>();
    private final Map<String, String> resourceTypeNames = new HashMap<>();
    private final Set<String> statisticsResourceTypeNames = new HashSet<>();

    /**
     * Create a graph computer from a Grakn Graph. The computer operates on the instances of the types provided in
     * the <code>subtypeNames</code> argument. All subtypeNames of the given types are included when deciding whether to
     * include an instance.
     *
     * @param subTypeNames                the set of type names the computer will use to filter instances
     * @param statisticsResourceTypeNames the set of resource type ids statistics will be working on
     */

    public Analytics(String keySpace, Set<String> subTypeNames, Set<String> statisticsResourceTypeNames) {
        this.keySpace = keySpace;
        GraknGraph graph = Grakn.factory(Grakn.DEFAULT_URI, this.keySpace).getGraph();

        // make sure we don't accidentally commit anything
        // TODO: Fix this properly. I.E. Don't run TinkerGraph Tests which hit this line.
        try {
            graph.rollback();
        } catch (UnsupportedOperationException ignored) {
        }

        // fetch all the types
        Set<Type> subtypes = subTypeNames.stream().map((name) -> {
            Type type = graph.getType(name);
            if (type == null) throw new IllegalArgumentException(ErrorMessage.NAME_NOT_FOUND.getMessage(name));
            return type;
        }).collect(Collectors.toSet());
        Set<Type> statisticsResourceTypes = statisticsResourceTypeNames.stream().map((name) -> {
            Type type = graph.getType(name);
            if (type == null) throw new IllegalArgumentException(ErrorMessage.NAME_NOT_FOUND.getMessage(name));
            return type;
        }).collect(Collectors.toSet());

        // collect resource-types for statistics
        ResourceType<?> metaResourceType = graph.admin().getMetaResourceType();
        metaResourceType.instances().stream()
                .map(Concept::asResourceType)
                .forEach(type -> resourceTypeNames.put(type.getName(), type.getDataType().getName()));

        if (subtypes.isEmpty()) {
            graph.admin().getMetaEntityType().instances().forEach(type -> this.subtypeNames.add(type.asType().getName()));
            metaResourceType.instances().forEach(type -> this.subtypeNames.add(type.asType().getName()));
            graph.admin().getMetaRelationType().instances().forEach(type -> this.subtypeNames.add(type.asType().getName()));
            this.subtypeNames.removeAll(analyticsElements);
        } else {
            for (Type t : subtypes) {
                t.subTypes().forEach(subtype -> this.subtypeNames.add(subtype.getName()));
            }
        }

        if (!statisticsResourceTypes.isEmpty()) {
            for (Type t : statisticsResourceTypes) {
                t.subTypes().forEach(subtype -> this.statisticsResourceTypeNames.add(subtype.getName()));
            }
        }
    }

    /**
     * Count the number of instances in the graph.
     *
     * @return the number of instances
     */
    public long count() {
        LOGGER.info("CountMapReduce is called");
        GraknComputer computer = getGraphComputer();
        if (!selectedTypesHaveInstance()) return 0L;
        ComputerResult result = computer.compute(new CountMapReduce(subtypeNames));
        Map<String, Long> count = result.memory().get(GraknMapReduce.MAP_REDUCE_MEMORY_KEY);
        LOGGER.info("CountMapReduce is done");
        return count.getOrDefault(CountMapReduce.MEMORY_KEY, 0L);
    }

    /**
     * Minimum value of the selected resource-type.
     *
     * @return min
     */
    public Optional<Number> min() {
        LOGGER.info("MinMapReduce is called");
        String dataType = checkSelectedResourceTypesHaveCorrectDataType(statisticsResourceTypeNames);
        if (!selectedResourceTypesHaveInstance(statisticsResourceTypeNames)) return Optional.empty();

        Set<String> allSubtypes = statisticsResourceTypeNames.stream()
                .map(Schema.Resource.HAS_RESOURCE::getName).collect(Collectors.toSet());
        allSubtypes.addAll(subtypeNames);
        allSubtypes.addAll(statisticsResourceTypeNames);

        GraknComputer computer = getGraphComputer();
        ComputerResult result = computer.compute(new DegreeVertexProgram(allSubtypes),
                new MinMapReduce(statisticsResourceTypeNames, dataType));
        Map<String, Number> min = result.memory().get(GraknMapReduce.MAP_REDUCE_MEMORY_KEY);
        LOGGER.info("MinMapReduce is done");
        return Optional.of(min.get(MinMapReduce.MEMORY_KEY));
    }

    /**
     * Maximum value of the selected resource-type.
     *
     * @return max
     */
    public Optional<Number> max() {
        LOGGER.info("MaxMapReduce is called");
        String dataType = checkSelectedResourceTypesHaveCorrectDataType(statisticsResourceTypeNames);
        if (!selectedResourceTypesHaveInstance(statisticsResourceTypeNames)) return Optional.empty();

        Set<String> allSubtypes = statisticsResourceTypeNames.stream()
                .map(Schema.Resource.HAS_RESOURCE::getName).collect(Collectors.toSet());
        allSubtypes.addAll(subtypeNames);
        allSubtypes.addAll(statisticsResourceTypeNames);

        GraknComputer computer = getGraphComputer();
        ComputerResult result = computer.compute(new DegreeVertexProgram(allSubtypes),
                new MaxMapReduce(statisticsResourceTypeNames, dataType));
        Map<String, Number> max = result.memory().get(GraknMapReduce.MAP_REDUCE_MEMORY_KEY);
        LOGGER.info("MaxMapReduce is done");
        return Optional.of(max.get(MaxMapReduce.MEMORY_KEY));
    }

    /**
     * Sum of values of the selected resource-type.
     *
     * @return sum
     */
    public Optional<Number> sum() {
        LOGGER.info("SumMapReduce is called");
        String dataType = checkSelectedResourceTypesHaveCorrectDataType(statisticsResourceTypeNames);
        if (!selectedResourceTypesHaveInstance(statisticsResourceTypeNames)) return Optional.empty();

        Set<String> allSubtypes = statisticsResourceTypeNames.stream()
                .map(Schema.Resource.HAS_RESOURCE::getName).collect(Collectors.toSet());
        allSubtypes.addAll(subtypeNames);
        allSubtypes.addAll(statisticsResourceTypeNames);

        GraknComputer computer = getGraphComputer();
        ComputerResult result = computer.compute(new DegreeVertexProgram(allSubtypes),
                new SumMapReduce(statisticsResourceTypeNames, dataType));
        Map<String, Number> max = result.memory().get(GraknMapReduce.MAP_REDUCE_MEMORY_KEY);
        LOGGER.info("SumMapReduce is done");
        return Optional.of(max.get(SumMapReduce.MEMORY_KEY));
    }

    /**
     * Compute the mean of instances of the selected resource-type.
     *
     * @return mean
     */
    public Optional<Double> mean() {
        LOGGER.info("MeanMapReduce is called");
        String dataType = checkSelectedResourceTypesHaveCorrectDataType(statisticsResourceTypeNames);
        if (!selectedResourceTypesHaveInstance(statisticsResourceTypeNames)) return Optional.empty();

        Set<String> allSubtypes = statisticsResourceTypeNames.stream()
                .map(Schema.Resource.HAS_RESOURCE::getName).collect(Collectors.toSet());
        allSubtypes.addAll(subtypeNames);
        allSubtypes.addAll(statisticsResourceTypeNames);

        GraknComputer computer = getGraphComputer();
        ComputerResult result = computer.compute(new DegreeVertexProgram(allSubtypes),
                new MeanMapReduce(statisticsResourceTypeNames, dataType));
        Map<String, Map<String, Double>> mean = result.memory().get(GraknMapReduce.MAP_REDUCE_MEMORY_KEY);
        Map<String, Double> meanPair = mean.get(MeanMapReduce.MEMORY_KEY);
        LOGGER.info("MeanMapReduce is done");
        return Optional.of(meanPair.get(MeanMapReduce.SUM) / meanPair.get(MeanMapReduce.COUNT));
    }

    /**
     * Compute the median of instances of the selected resource-type.
     *
     * @return median
     */
    public Optional<Number> median() {
        LOGGER.info("MedianVertexProgram is called");
        String dataType = checkSelectedResourceTypesHaveCorrectDataType(statisticsResourceTypeNames);
        if (!selectedResourceTypesHaveInstance(statisticsResourceTypeNames)) return Optional.empty();

        Set<String> allSubtypes = statisticsResourceTypeNames.stream()
                .map(Schema.Resource.HAS_RESOURCE::getName).collect(Collectors.toSet());
        allSubtypes.addAll(subtypeNames);
        allSubtypes.addAll(statisticsResourceTypeNames);

        GraknComputer computer = getGraphComputer();
        ComputerResult result = computer.compute(
                new MedianVertexProgram(allSubtypes, statisticsResourceTypeNames, dataType));
        LOGGER.info("MedianMapReduce is done");
        return Optional.of(result.memory().get(MedianVertexProgram.MEDIAN));
    }

    /**
     * Compute the standard deviation of instances of the selected resource-type.
     *
     * @return standard deviation
     */
    public Optional<Double> std() {
        LOGGER.info("StdMapReduce is called");
        String dataType = checkSelectedResourceTypesHaveCorrectDataType(statisticsResourceTypeNames);
        if (!selectedResourceTypesHaveInstance(statisticsResourceTypeNames)) return Optional.empty();

        Set<String> allSubtypes = statisticsResourceTypeNames.stream()
                .map(Schema.Resource.HAS_RESOURCE::getName).collect(Collectors.toSet());
        allSubtypes.addAll(subtypeNames);
        allSubtypes.addAll(statisticsResourceTypeNames);

        GraknComputer computer = getGraphComputer();
        ComputerResult result = computer.compute(new DegreeVertexProgram(allSubtypes),
                new StdMapReduce(statisticsResourceTypeNames, dataType));
        Map<String, Map<String, Double>> std = result.memory().get(GraknMapReduce.MAP_REDUCE_MEMORY_KEY);
        Map<String, Double> stdTuple = std.get(StdMapReduce.MEMORY_KEY);
        double squareSum = stdTuple.get(StdMapReduce.SQUARE_SUM);
        double sum = stdTuple.get(StdMapReduce.SUM);
        double count = stdTuple.get(StdMapReduce.COUNT);
        LOGGER.info("StdMapReduce is done");
        return Optional.of(Math.sqrt(squareSum / count - (sum / count) * (sum / count)));
    }

    /**
     * Compute the shortest path between two vertices. If there are more than one shortest path, an arbitrary path
     * is returned.
     *
     * @return a shortest path: a list of vertex ids along the path (including the two given vertices)
     */
    public List<Concept> shortestPath(String sourceId, String destinationId) {
        LOGGER.info("ShortestPathVertexProgram is called");
        if (!verticesExistInSubgraph(sourceId, destinationId))
            throw new IllegalStateException(ErrorMessage.INSTANCE_DOES_NOT_EXIST.getMessage());
        GraknGraph graph = Grakn.factory(Grakn.DEFAULT_URI, this.keySpace).getGraph();
        if (sourceId.equals(destinationId))
            return Collections.singletonList(graph.getConcept(sourceId));
        GraknComputer computer = getGraphComputer();
        ComputerResult result = computer.compute(new ShortestPathVertexProgram(subtypeNames, sourceId, destinationId),
                new ClusterMemberMapReduce(subtypeNames, ShortestPathVertexProgram.FOUND_IN_ITERATION));
        Map<Integer, Set<String>> map = result.memory().get(GraknMapReduce.MAP_REDUCE_MEMORY_KEY);

        List<String> path = new ArrayList<>();
        path.add(sourceId);
        path.addAll(map.entrySet().stream()
                .sorted(Comparator.comparingInt(Map.Entry::getKey))
                .map(pair -> pair.getValue().iterator().next())
                .collect(Collectors.toList()));
        path.add(destinationId);
        LOGGER.info("ShortestPathVertexProgram is done");
        graph = Grakn.factory(Grakn.DEFAULT_URI, this.keySpace).getGraph();
        return path.stream().map(graph::<Instance>getConcept).collect(Collectors.toList());
    }

    /**
     * Compute the number of connected components.
     *
     * @return a map of set, each set contains all the vertex ids belonging to one connected component
     */
    public Map<String, Set<String>> connectedComponents() {
        LOGGER.info("ConnectedComponentsVertexProgram is called");
        if (!selectedTypesHaveInstance()) return Collections.emptyMap();
        GraknComputer computer = getGraphComputer();
        ComputerResult result = computer.compute(new ConnectedComponentVertexProgram(subtypeNames),
                new ClusterMemberMapReduce(subtypeNames, ConnectedComponentVertexProgram.CLUSTER_LABEL));
        LOGGER.info("ConnectedComponentsVertexProgram is done");
        return result.memory().get(GraknMapReduce.MAP_REDUCE_MEMORY_KEY);
    }

    /**
     * Compute the number of connected components.
     *
     * @return a map of component size
     */
    public Map<String, Long> connectedComponentsSize() {
        LOGGER.info("ConnectedComponentsVertexProgram is called");
        if (!selectedTypesHaveInstance()) return Collections.emptyMap();
        GraknComputer computer = getGraphComputer();
        ComputerResult result = computer.compute(new ConnectedComponentVertexProgram(subtypeNames),
                new ClusterSizeMapReduce(subtypeNames, ConnectedComponentVertexProgram.CLUSTER_LABEL));
        LOGGER.info("ConnectedComponentsVertexProgram is done");
        return result.memory().get(GraknMapReduce.MAP_REDUCE_MEMORY_KEY);
    }

    /**
     * Compute the connected components and persist the component labels
     */
    public Map<String, Long> connectedComponentsAndPersist() {
        LOGGER.info("ConnectedComponentsVertexProgram is called");
        if (!Sets.intersection(subtypeNames, analyticsElements).isEmpty()) {
            throw new IllegalStateException(ErrorMessage.ILLEGAL_ARGUMENT_EXCEPTION
                    .getMessage(this.getClass().toString()));
        }
        if (selectedTypesHaveInstance()) {
            mutateResourceOntology(connectedComponent, ResourceType.DataType.STRING);
            waitOnMutateResourceOntology(connectedComponent);

            GraknComputer computer = getGraphComputer();
            ComputerResult result = computer.compute(new ConnectedComponentVertexProgram(subtypeNames, keySpace),
                    new ClusterSizeMapReduce(subtypeNames, ConnectedComponentVertexProgram.CLUSTER_LABEL));
            LOGGER.info("ConnectedComponentsVertexProgram is done");
            return result.memory().get(GraknMapReduce.MAP_REDUCE_MEMORY_KEY);
        }
        return Collections.emptyMap();
    }

    /**
     * Compute the number of relations that each instance takes part in.
     *
     * @return a map from each instance to its degree
     */
    public Map<Long, Set<String>> degrees() {
        LOGGER.info("DegreeVertexProgram is called");
        GraknComputer computer = getGraphComputer();
        ComputerResult result = computer.compute(new DegreeVertexProgram(subtypeNames),
                new DegreeDistributionMapReduce(subtypeNames));
        LOGGER.info("DegreeVertexProgram is done");
        return result.memory().get(GraknMapReduce.MAP_REDUCE_MEMORY_KEY);
    }

    /**
     * Compute the number of relations that each instance takes part in and persist this information in the graph. The
     * degree is stored as a resource attached to the relevant instance.
     *
     * @param resourceType the type of the resource that will contain the degree
     */
    private void degreesAndPersist(String resourceType) {
        LOGGER.info("DegreeVertexProgram is called");
        if (!Sets.intersection(subtypeNames, analyticsElements).isEmpty()) {
            throw new IllegalStateException(ErrorMessage.ILLEGAL_ARGUMENT_EXCEPTION
                    .getMessage(this.getClass().toString()));
        }

        mutateResourceOntology(resourceType, ResourceType.DataType.LONG);
        waitOnMutateResourceOntology(resourceType);

        GraknComputer computer = getGraphComputer();
        computer.compute(new DegreeAndPersistVertexProgram(subtypeNames, keySpace));
        LOGGER.info("DegreeVertexProgram is done");
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
     * are related to the resource type <code>resourceTypeName</code> used to persist data computed by analytics.
     *
     * @param resourceTypeName the ID of a resource type used to persist information
     * @param resourceDataType the datatype of the resource type
     */
    private void mutateResourceOntology(String resourceTypeName, ResourceType.DataType resourceDataType) {
        GraknGraph graph = Grakn.factory(Grakn.DEFAULT_URI, keySpace).getGraph();

        ResourceType resource = graph.putResourceType(resourceTypeName, resourceDataType);

        for (String type : subtypeNames) {
            graph.getType(type).hasResource(resource);
        }

        try {
            graph.commit();
        } catch (GraknValidationException e) {
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
        GraknGraph graph = Grakn.factory(Grakn.DEFAULT_URI, keySpace).getGraph();
        graph.showImplicitConcepts(true);

        for (int i = 0; i < numberOfOntologyChecks; i++) {
            boolean isOntologyComplete = true;
            // TODO: Fix this properly. I.E. Don't run TinkerGraph Tests which hit this line.
            try {
                graph.rollback();
            } catch (UnsupportedOperationException ignored) {
            }

            ResourceType resource = graph.getResourceType(resourceTypeId);
            if (resource == null) continue;
            RoleType degreeOwner = graph.getRoleType(Schema.Resource.HAS_RESOURCE_OWNER.getName(resourceTypeId));
            if (degreeOwner == null) continue;
            RoleType degreeValue = graph.getRoleType(Schema.Resource.HAS_RESOURCE_VALUE.getName(resourceTypeId));
            if (degreeValue == null) continue;
            RelationType relationType = graph.getRelationType(Schema.Resource.HAS_RESOURCE.getName(resourceTypeId));
            if (relationType == null) continue;

            for (String type : subtypeNames) {
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
            if (!resourceTypeNames.containsKey(type))
                throw new IllegalStateException(ErrorMessage.ILLEGAL_ARGUMENT_EXCEPTION
                        .getMessage(this.getClass().toString()));

            if (dataType == null) {
                // check if the resource-type has data-type LONG or DOUBLE
                dataType = resourceTypeNames.get(type);

                if (!dataType.equals(ResourceType.DataType.LONG.getName()) &&
                        !dataType.equals(ResourceType.DataType.DOUBLE.getName()))
                    throw new IllegalStateException(ErrorMessage.ILLEGAL_ARGUMENT_EXCEPTION
                            .getMessage(this.getClass().toString()));

            } else {
                // check if all the resource-types have the same data-type
                if (!dataType.equals(resourceTypeNames.get(type)))
                    throw new IllegalStateException(ErrorMessage.ILLEGAL_ARGUMENT_EXCEPTION
                            .getMessage(this.getClass().toString()));
            }
        }
        return dataType;
    }

    private boolean selectedResourceTypesHaveInstance(Set<String> statisticsResourceTypes) {

        GraknGraph graph = Grakn.factory(Grakn.DEFAULT_URI, this.keySpace).getGraph();

        List<Pattern> checkResourceTypes = statisticsResourceTypes.stream()
                .map(type -> var("x").has(type)).collect(Collectors.toList());
        List<Pattern> checkSubtypes = subtypeNames.stream()
                .map(type -> var("x").isa(type)).collect(Collectors.toList());

        return graph.graql().infer(false).match(or(checkResourceTypes), or(checkSubtypes)).ask().execute();
    }

    private boolean selectedTypesHaveInstance() {
        if (subtypeNames.isEmpty()) return false;

        GraknGraph graph = Grakn.factory(Grakn.DEFAULT_URI, this.keySpace).getGraph();

        List<Pattern> checkSubtypes = subtypeNames.stream()
                .map(type -> var("x").isa(type)).collect(Collectors.toList());

        return graph.graql().infer(false).match(or(checkSubtypes)).ask().execute();
    }

    protected GraknComputer getGraphComputer() {
        return Grakn.factory(Grakn.DEFAULT_URI, keySpace).getGraphComputer();
    }

    protected boolean verticesExistInSubgraph(String... ids) {
        GraknGraph graph = Grakn.factory(Grakn.DEFAULT_URI, this.keySpace).getGraph();
        for (String id : ids) {
            Concept concept = graph.getConcept(id);
            if (concept == null || !subtypeNames.contains(concept.type().getName())) return false;
        }
        return true;
    }
}
