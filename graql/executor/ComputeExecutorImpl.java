/*
 * Copyright (C) 2020 Grakn Labs
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 */

package grakn.core.graql.executor;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import grakn.core.concept.answer.ConceptList;
import grakn.core.concept.answer.ConceptSet;
import grakn.core.concept.answer.ConceptSetMeasure;
import grakn.core.concept.answer.Numeric;
import grakn.core.core.Schema;
import grakn.core.graql.analytics.ClusterMemberMapReduce;
import grakn.core.graql.analytics.ConnectedComponentVertexProgram;
import grakn.core.graql.analytics.ConnectedComponentsVertexProgram;
import grakn.core.graql.analytics.CorenessVertexProgram;
import grakn.core.graql.analytics.DegreeDistributionMapReduce;
import grakn.core.graql.analytics.DegreeStatisticsVertexProgram;
import grakn.core.graql.analytics.DegreeVertexProgram;
import grakn.core.graql.analytics.GraknMapReduce;
import grakn.core.graql.analytics.GraknVertexProgram;
import grakn.core.graql.analytics.KCoreVertexProgram;
import grakn.core.graql.analytics.MaxMapReduce;
import grakn.core.graql.analytics.MeanMapReduce;
import grakn.core.graql.analytics.MedianVertexProgram;
import grakn.core.graql.analytics.MinMapReduce;
import grakn.core.graql.analytics.NoResultException;
import grakn.core.graql.analytics.ShortestPathVertexProgram;
import grakn.core.graql.analytics.StatisticsMapReduce;
import grakn.core.graql.analytics.StdMapReduce;
import grakn.core.graql.analytics.SumMapReduce;
import grakn.core.graql.analytics.Utility;
import grakn.core.kb.concept.api.AttributeType;
import grakn.core.kb.concept.api.Concept;
import grakn.core.kb.concept.api.ConceptId;
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.concept.api.LabelId;
import grakn.core.kb.concept.api.RelationType;
import grakn.core.kb.concept.api.Role;
import grakn.core.kb.concept.api.SchemaConcept;
import grakn.core.kb.concept.api.Thing;
import grakn.core.kb.concept.api.Type;
import grakn.core.kb.concept.manager.ConceptManager;
import grakn.core.kb.graql.exception.GraqlSemanticException;
import grakn.core.kb.graql.executor.ComputeExecutor;
import grakn.core.kb.graql.executor.ExecutorFactory;
import grakn.core.kb.graql.executor.TraversalExecutor;
import grakn.core.kb.keyspace.KeyspaceStatistics;
import graql.lang.Graql;
import graql.lang.pattern.Pattern;
import graql.lang.query.GraqlCompute;
import graql.lang.query.builder.Computable;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import static graql.lang.Graql.Token.Compute.Algorithm.CONNECTED_COMPONENT;
import static graql.lang.Graql.Token.Compute.Algorithm.DEGREE;
import static graql.lang.Graql.Token.Compute.Algorithm.K_CORE;
import static graql.lang.Graql.Token.Compute.Method.COUNT;
import static graql.lang.Graql.Token.Compute.Method.MAX;
import static graql.lang.Graql.Token.Compute.Method.MEAN;
import static graql.lang.Graql.Token.Compute.Method.MEDIAN;
import static graql.lang.Graql.Token.Compute.Method.MIN;
import static graql.lang.Graql.Token.Compute.Method.STD;
import static graql.lang.Graql.Token.Compute.Method.SUM;
import static graql.lang.Graql.var;
import static java.util.stream.Collectors.toSet;

/**
 * A Graql Compute query executor
 */
public class ComputeExecutorImpl implements ComputeExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(ComputeExecutorImpl.class);
    private ConceptManager conceptManager;
    private ExecutorFactory executorFactory;
    private TraversalExecutor traversalExecutor;
    private HadoopGraph hadoopGraph;
    private KeyspaceStatistics keyspaceStatistics;

    ComputeExecutorImpl(ConceptManager conceptManager, ExecutorFactory executorFactory, TraversalExecutor traversalExecutor, HadoopGraph hadoopGraph, KeyspaceStatistics keyspaceStatistics) {
        this.conceptManager = conceptManager;
        this.executorFactory = executorFactory;
        this.traversalExecutor = traversalExecutor;
        this.hadoopGraph = hadoopGraph;
        this.keyspaceStatistics = keyspaceStatistics;
    }

    @Override
    public Stream<Numeric> stream(GraqlCompute.Statistics query) {
        Graql.Token.Compute.Method method = query.method();
        if (method.equals(MIN) || method.equals(MAX) || method.equals(MEDIAN) || method.equals(SUM)) {
            return runComputeMinMaxMedianOrSum(query.asValue());
        } else if (method.equals(MEAN)) {
            return runComputeMean(query.asValue());
        } else if (method.equals(STD)) {
            return runComputeStd(query.asValue());
        } else if (method.equals(COUNT)) {
            return runComputeCount(query.asCount());
        } else {
            throw new UnsupportedOperationException("Unsupported Graql Compute Statistics: " + query);
        }
    }

    @Override
    public Stream<ConceptList> stream(GraqlCompute.Path query) {
        return runComputePath(query);
    }

    @Override
    public Stream<ConceptSetMeasure> stream(GraqlCompute.Centrality query) {
        return runComputeCentrality(query);
    }

    @Override
    public Stream<ConceptSet> stream(GraqlCompute.Cluster query) {
        return runComputeCluster(query);
    }

    @Override
    public final ComputerResult compute(@Nullable VertexProgram<?> program,
                                        @Nullable MapReduce<?, ?, ?, ?, ?> mapReduce,
                                        @Nullable Set<LabelId> scope,
                                        Boolean includesRolePlayerEdges) {

        return new OLAPOperation(hadoopGraph).compute(program, mapReduce, scope, includesRolePlayerEdges);
    }

    @Override
    public final ComputerResult compute(@Nullable VertexProgram<?> program,
                                        @Nullable MapReduce<?, ?, ?, ?, ?> mapReduce,
                                        @Nullable Set<LabelId> scope) {

        return new OLAPOperation(hadoopGraph).compute(program, mapReduce, scope);
    }

    /**
     * The Graql compute min, max, median, or sum query run method
     *
     * @return a Answer object containing a Number that represents the answer
     */
    private Stream<Numeric> runComputeMinMaxMedianOrSum(GraqlCompute.Statistics.Value query) {
        Number number = runComputeStatistics(query);
        if (number == null) return Stream.empty();
        else return Stream.of(new Numeric(number));
    }

    /**
     * The Graql compute mean query run method
     *
     * @return a Answer object containing a Number that represents the answer
     */
    private Stream<Numeric> runComputeMean(GraqlCompute.Statistics.Value query) {
        Map<String, Double> meanPair = runComputeStatistics(query);
        if (meanPair == null) return Stream.empty();

        Double mean = meanPair.get(MeanMapReduce.SUM) / meanPair.get(MeanMapReduce.COUNT);
        return Stream.of(new Numeric(mean));
    }

    /**
     * The Graql compute std query run method
     *
     * @return a Answer object containing a Number that represents the answer
     */
    private Stream<Numeric> runComputeStd(GraqlCompute.Statistics.Value query) {
        Map<String, Double> stdTuple = runComputeStatistics(query);
        if (stdTuple == null) return Stream.empty();

        double squareSum = stdTuple.get(StdMapReduce.SQUARE_SUM);
        double sum = stdTuple.get(StdMapReduce.SUM);
        double count = stdTuple.get(StdMapReduce.COUNT);
        Double std = Math.sqrt(squareSum / count - (sum / count) * (sum / count));

        return Stream.of(new Numeric(std));
    }

    /**
     * The compute statistics base algorithm that is called in every compute statistics query
     *
     * @param <S> The return type of StatisticsMapReduce
     * @return result of compute statistics algorithm, which will be of type S
     */
    @Nullable
    private <S> S runComputeStatistics(GraqlCompute.Statistics.Value query) {
        AttributeType.DataType<?> targetDataType = validateAndGetTargetDataType(query);
        if (!targetContainsInstance(query)) return null;

        Set<LabelId> extendedScopeTypes = convertLabelsToIds(extendedScopeTypeLabels(query));
        Set<LabelId> targetTypes = convertLabelsToIds(targetTypeLabels(query));

        VertexProgram program = initStatisticsVertexProgram(query, targetTypes, targetDataType);
        StatisticsMapReduce<?> mapReduce = initStatisticsMapReduce(query, targetTypes, targetDataType);
        ComputerResult computerResult = compute(program, mapReduce, extendedScopeTypes);

        if (query.method().equals(MEDIAN)) {
            Number result = computerResult.memory().get(MedianVertexProgram.MEDIAN);
            LOG.debug("Median = {}", result);
            return (S) result;
        }

        Map<Serializable, S> resultMap = computerResult.memory().get(mapReduce.getClass().getName());
        LOG.debug("Result = {}", resultMap.get(MapReduce.NullObject.instance()));
        return resultMap.get(MapReduce.NullObject.instance());
    }

    /**
     * Helper method to validate that the target types are of one data type, and get that data type
     *
     * @return the DataType of the target types
     */
    @Nullable
    private AttributeType.DataType<?> validateAndGetTargetDataType(GraqlCompute.Statistics.Value query) {
        AttributeType.DataType<?> dataType = null;
        for (Type type : targetTypes(query)) {
            // check if the selected type is a attribute type
            if (!type.isAttributeType()) throw GraqlSemanticException.mustBeAttributeType(type.label());
            AttributeType<?> attributeType = type.asAttributeType();
            if (dataType == null) {
                // check if the attribute type has data-type LONG or DOUBLE
                dataType = attributeType.dataType();
                if (!dataType.equals(AttributeType.DataType.LONG) && !dataType.equals(AttributeType.DataType.DOUBLE)) {
                    throw GraqlSemanticException.attributeMustBeANumber(dataType, attributeType.label());
                }
            } else {
                // check if all the attribute types have the same data-type
                if (!dataType.equals(attributeType.dataType())) {
                    throw GraqlSemanticException.attributesWithDifferentDataTypes(query.of());
                }
            }
        }
        return dataType;
    }

    /**
     * Helper method to intialise the vertex program for compute statistics queries
     *
     * @param query          representing the compute query
     * @param targetTypes    representing the attribute types in which the statistics computation is targeted for
     * @param targetDataType representing the data type of the target attribute types
     * @return an object which is a subclass of VertexProgram
     */
    private VertexProgram initStatisticsVertexProgram(GraqlCompute query, Set<LabelId> targetTypes, AttributeType.DataType<?> targetDataType) {
        if (query.method().equals(MEDIAN)) return new MedianVertexProgram(targetTypes, targetDataType);
        else return new DegreeStatisticsVertexProgram(targetTypes);
    }

    /**
     * Helper method to initialise the MapReduce algorithm for compute statistics queries
     *
     * @param targetTypes    representing the attribute types in which the statistics computation is targeted for
     * @param targetDataType representing the data type of the target attribute types
     * @return an object which is a subclass of StatisticsMapReduce
     */
    private StatisticsMapReduce<?> initStatisticsMapReduce(GraqlCompute.Statistics.Value query,
                                                           Set<LabelId> targetTypes,
                                                           AttributeType.DataType<?> targetDataType) {
        Graql.Token.Compute.Method method = query.method();
        if (method.equals(MIN)) {
            return new MinMapReduce(targetTypes, targetDataType, DegreeVertexProgram.DEGREE);
        } else if (method.equals(MAX)) {
            return new MaxMapReduce(targetTypes, targetDataType, DegreeVertexProgram.DEGREE);
        } else if (method.equals(MEAN)) {
            return new MeanMapReduce(targetTypes, targetDataType, DegreeVertexProgram.DEGREE);
        } else if (method.equals(STD)) {
            return new StdMapReduce(targetTypes, targetDataType, DegreeVertexProgram.DEGREE);
        } else if (method.equals(SUM)) {
            return new SumMapReduce(targetTypes, targetDataType, DegreeVertexProgram.DEGREE);
        }

        return null;
    }

    /**
     * Run Graql compute count query
     *
     * @return a Answer object containing the count value
     */
    private Stream<Numeric> runComputeCount(GraqlCompute.Statistics.Count query) {
        //TODO: simplify this when we update statistics to also contain ENTITY, RELATION and ATTRIBUTE
        return retrieveCachedCount(query);

        // TODO we can re-add this when we move the cached count behavior out of `compute` and into `aggregate` instead
        /*
        Set<LabelId> scopeTypeLabelIDs = convertLabelsToIds(scopeTypeLabels(query));
        Set<LabelId> scopeTypeAndImpliedPlayersLabelIDs = convertLabelsToIds(scopeTypeLabelsImplicitPlayers(query));
        scopeTypeAndImpliedPlayersLabelIDs.addAll(scopeTypeLabelIDs);

        Map<Integer, Long> count;

        ComputerResult result = compute(
                new CountVertexProgram(),
                new CountMapReduceWithAttribute(),
                scopeTypeAndImpliedPlayersLabelIDs, false);
        count = result.memory().get(CountMapReduceWithAttribute.class.getName());

        long finalCount = count.keySet().stream()
                .filter(id -> scopeTypeLabelIDs.contains(LabelId.of(id)))
                .mapToLong(count::get).sum();
        if (count.containsKey(GraknMapReduce.RESERVED_TYPE_LABEL_KEY)) {
            finalCount += count.get(GraknMapReduce.RESERVED_TYPE_LABEL_KEY);
        }

        LOG.debug("Count = {}", finalCount);
        return Stream.of(new Numeric(finalCount));
         */
    }

    /**
     * @param query compute count entry query
     * @return aggregate instance counts fetched from keyspace statistics
     */

    private Stream<Numeric> retrieveCachedCount(GraqlCompute.Statistics.Count query) {

        // enforce that query has attributes set true
        query.attributes(true);

        // retrieve all types that must be counted
        Set<Type> types = scopeTypes(query).collect(toSet());
        if (types.contains(conceptManager.getMetaConcept())) {
            types = types.stream().filter(type -> type.equals(conceptManager.getMetaConcept())).collect(toSet());
        }
        if (types.contains(conceptManager.getMetaEntityType())) {
            types = types.stream()
                    // discard all entity types except the meta entity type
                    .filter(type -> !type.isEntityType() || type.equals(conceptManager.getMetaEntityType()))
                    .collect(toSet());
        }
        if (types.contains(conceptManager.getMetaRelationType())) {
            types = types.stream()
                    // discard all relation types except the meta relation type
                    .filter(type -> !type.isRelationType() || type.equals(conceptManager.getMetaRelationType()))
                    .collect(toSet());
        }

        if (types.contains(conceptManager.getMetaAttributeType())) {
            types = types.stream()
                    // discard all attribute types except the meta attribute type
                    .filter(type -> !type.isAttributeType() || type.equals(conceptManager.getMetaAttributeType()))
                    .collect(toSet());
        }

        // the final set of types should only include the types for whom we should perform counts
        long totalCount = types.stream().mapToLong(type -> keyspaceStatistics.count(conceptManager, type.label())).sum();
        return Stream.of(new Numeric(totalCount));
    }

    /**
     * The Graql compute path query run method
     *
     * @return a Answer containing the list of shortest paths
     */
    private Stream<ConceptList> runComputePath(GraqlCompute.Path query) {
        ConceptId fromID = ConceptId.of(query.from());
        ConceptId toID = ConceptId.of(query.to());

        if (!scopeContainsInstances(query, fromID, toID)) throw GraqlSemanticException.instanceDoesNotExist();
        if (fromID.equals(toID)) return Stream.of(new ConceptList(ImmutableList.of(fromID)));

        Set<LabelId> scopedLabelIds = convertLabelsToIds(scopeTypeLabels(query));

        ComputerResult result = compute(new ShortestPathVertexProgram(fromID, toID), null, scopedLabelIds);

        Multimap<ConceptId, ConceptId> pathsAsEdgeList = HashMultimap.create();
        Map<String, Set<String>> resultFromMemory = result.memory().get(ShortestPathVertexProgram.SHORTEST_PATH);
        resultFromMemory.forEach((id, idSet) -> idSet.forEach(id2 -> {
            pathsAsEdgeList.put(Schema.conceptIdFromVertexId(id), Schema.conceptIdFromVertexId(id2));
        }));

        List<List<ConceptId>> paths;
        if (!resultFromMemory.isEmpty()) {
            paths = getComputePathResultList(pathsAsEdgeList, fromID);
            if (scopeIncludesAttributes(query)) {
                paths = getComputePathResultListIncludingImplicitRelations(paths);
            }
        } else {
            paths = Collections.emptyList();
        }

        return paths.stream().map(ConceptList::new);
    }

    /**
     * The Graql compute centrality query run method
     *
     * @return a Answer containing the centrality count map
     */
    private Stream<ConceptSetMeasure> runComputeCentrality(GraqlCompute.Centrality query) {
        if (query.using().equals(DEGREE)) return runComputeDegree(query);
        if (query.using().equals(K_CORE)) return runComputeCoreness(query);

        throw new IllegalArgumentException("Unrecognised Graql Compute Centrality algorithm: " + query.method());
    }

    /**
     * The Graql compute centrality using degree query run method
     *
     * @return a Answer containing the centrality count map
     */
    private Stream<ConceptSetMeasure> runComputeDegree(GraqlCompute.Centrality query) {
        Set<Label> targetTypeLabels;

        // Check if ofType is valid before returning emptyMap
        if (query.of().isEmpty()) {
            targetTypeLabels = scopeTypeLabels(query);
        } else {
            targetTypeLabels = query.of().stream()
                    .flatMap(t -> {
                        Label typeLabel = Label.of(t);
                        Type type = conceptManager.getSchemaConcept(typeLabel);
                        if (type == null) throw GraqlSemanticException.labelNotFound(typeLabel);
                        return type.subs();
                    })
                    .map(SchemaConcept::label)
                    .collect(toSet());
        }

        Set<Label> scopeTypeLabels = Sets.union(scopeTypeLabels(query), targetTypeLabels);

        if (!scopeContainsInstance(query)) {
            return Stream.empty();
        }

        Set<LabelId> scopeTypeLabelIDs = convertLabelsToIds(scopeTypeLabels);
        Set<LabelId> targetTypeLabelIDs = convertLabelsToIds(targetTypeLabels);

        ComputerResult computerResult = compute(new DegreeVertexProgram(targetTypeLabelIDs),
                new DegreeDistributionMapReduce(targetTypeLabelIDs, DegreeVertexProgram.DEGREE),
                scopeTypeLabelIDs);

        Map<Long, Set<ConceptId>> centralityMap = computerResult.memory().get(DegreeDistributionMapReduce.class.getName());

        return centralityMap.entrySet().stream()
                .map(centrality -> new ConceptSetMeasure(centrality.getValue(), centrality.getKey()));
    }

    /**
     * The Graql compute centrality using k-core query run method
     *
     * @return a Answer containing the centrality count map
     */
    private Stream<ConceptSetMeasure> runComputeCoreness(GraqlCompute.Centrality query) {
        long k = query.where().minK().get();

        if (k < 2L) throw GraqlSemanticException.kValueSmallerThanTwo();

        Set<Label> targetTypeLabels;

        // Check if ofType is valid before returning emptyMap
        if (query.of().isEmpty()) {
            targetTypeLabels = scopeTypeLabels(query);
        } else {
            targetTypeLabels = query.of().stream()
                    .flatMap(t -> {
                        Label typeLabel = Label.of(t);
                        Type type = conceptManager.getSchemaConcept(typeLabel);
                        if (type == null) throw GraqlSemanticException.labelNotFound(typeLabel);
                        if (type.isRelationType()) throw GraqlSemanticException.kCoreOnRelationType(typeLabel);
                        return type.subs();
                    })
                    .map(SchemaConcept::label)
                    .collect(toSet());
        }

        Set<Label> scopeTypeLabels = Sets.union(scopeTypeLabels(query), targetTypeLabels);

        if (!scopeContainsInstance(query)) {
            return Stream.empty();
        }

        ComputerResult result;
        Set<LabelId> scopeTypeLabelIDs = convertLabelsToIds(scopeTypeLabels);
        Set<LabelId> targetTypeLabelIDs = convertLabelsToIds(targetTypeLabels);

        try {
            result = compute(new CorenessVertexProgram(k),
                    new DegreeDistributionMapReduce(targetTypeLabelIDs, CorenessVertexProgram.CORENESS),
                    scopeTypeLabelIDs);
        } catch (NoResultException e) {
            return Stream.empty();
        }

        Map<Long, Set<ConceptId>> centralityMap = result.memory().get(DegreeDistributionMapReduce.class.getName());

        return centralityMap.entrySet().stream()
                .map(centrality -> new ConceptSetMeasure(centrality.getValue(), centrality.getKey()));
    }

    private Stream<ConceptSet> runComputeCluster(GraqlCompute.Cluster query) {
        if (query.using().equals(K_CORE)) return runComputeKCore(query);
        if (query.using().equals(CONNECTED_COMPONENT)) return runComputeConnectedComponent(query);

        throw new IllegalArgumentException("Unrecognised Graql Compute Cluster algorithm: " + query.method());
    }


    private Stream<ConceptSet> runComputeConnectedComponent(GraqlCompute.Cluster query) {
        boolean restrictSize = query.where().size().isPresent();

        if (!scopeContainsInstance(query)) {
            LOG.info("Selected types don't have instances");
            return Stream.empty();
        }

        Set<LabelId> scopeTypeLabelIDs = convertLabelsToIds(scopeTypeLabels(query));

        GraknVertexProgram<?> vertexProgram;
        if (query.where().contains().isPresent()) {
            ConceptId conceptId = ConceptId.of(query.where().contains().get());
            if (!scopeContainsInstances(query, conceptId)) {
                throw GraqlSemanticException.instanceDoesNotExist();
            }
            vertexProgram = new ConnectedComponentVertexProgram(conceptId);
        } else {
            vertexProgram = new ConnectedComponentsVertexProgram();
        }

        GraknMapReduce<?> mapReduce;
        if (restrictSize) {
            mapReduce = new ClusterMemberMapReduce(ConnectedComponentsVertexProgram.CLUSTER_LABEL, query.where().size().get());
        }
        else mapReduce = new ClusterMemberMapReduce(ConnectedComponentsVertexProgram.CLUSTER_LABEL);

        Memory memory = compute(vertexProgram, mapReduce, scopeTypeLabelIDs).memory();
        Map<String, Set<ConceptId>> result = memory.get(mapReduce.getClass().getName());
        return result.values().stream().map(ConceptSet::new);

//        TODO: Enable the following compute cluster-size through a separate compute method
//        if (!query.where().members().get()) {
//            GraknMapReduce<?> mapReduce;
//            if (restrictSize) {
//                mapreduce = new ClusterSizeMapReduce(ConnectedComponentsVertexProgram.CLUSTER_LABEL, query.where().size().get());
//            } else {
//                mapReduce = new ClusterSizeMapReduce(ConnectedComponentsVertexProgram.CLUSTER_LABEL);
//            }
//            Map<String, Long> result = memory.get(mapReduce.getClass().getName());
//            return result.values().stream().map(Value::new);
//        }
    }

    private Stream<ConceptSet> runComputeKCore(GraqlCompute.Cluster query) {
        long k = query.where().k().get();

        if (k < 2L) throw GraqlSemanticException.kValueSmallerThanTwo();

        if (!scopeContainsInstance(query)) {
            return Stream.empty();
        }

        ComputerResult computerResult;
        Set<LabelId> subLabelIds = convertLabelsToIds(scopeTypeLabels(query));
        try {
            computerResult = compute(
                    new KCoreVertexProgram(k),
                    new ClusterMemberMapReduce(KCoreVertexProgram.K_CORE_LABEL),
                    subLabelIds);
        } catch (NoResultException e) {
            return Stream.empty();
        }

        Map<String, Set<ConceptId>> result = computerResult.memory().get(ClusterMemberMapReduce.class.getName());
        return result.values().stream().map(ConceptSet::new);
    }

    /**
     * Helper method to get list of all shortest paths
     *
     * @param resultGraph edge map
     * @param fromID      starting vertex
     * @return
     */
    private List<List<ConceptId>> getComputePathResultList(Multimap<ConceptId, ConceptId> resultGraph, ConceptId fromID) {
        List<List<ConceptId>> allPaths = new ArrayList<>();
        List<ConceptId> firstPath = new ArrayList<>();
        firstPath.add(fromID);

        Deque<List<ConceptId>> queue = new ArrayDeque<>();
        queue.addLast(firstPath);
        while (!queue.isEmpty()) {
            List<ConceptId> currentPath = queue.pollFirst();
            if (resultGraph.containsKey(currentPath.get(currentPath.size() - 1))) {
                Collection<ConceptId> successors = resultGraph.get(currentPath.get(currentPath.size() - 1));
                Iterator<ConceptId> iterator = successors.iterator();
                for (int i = 0; i < successors.size() - 1; i++) {
                    List<ConceptId> extendedPath = new ArrayList<>(currentPath);
                    extendedPath.add(iterator.next());
                    queue.addLast(extendedPath);
                }
                currentPath.add(iterator.next());
                queue.addLast(currentPath);
            } else {
                allPaths.add(currentPath);
            }
        }
        return allPaths;
    }

    /**
     * Helper method t get the list of all shortest path, but also including the implicit relations that connect
     * entities and attributes
     *
     * @param allPaths
     * @return
     */
    private List<List<ConceptId>> getComputePathResultListIncludingImplicitRelations(List<List<ConceptId>> allPaths) {
        List<List<ConceptId>> extendedPaths = new ArrayList<>();
        for (List<ConceptId> currentPath : allPaths) {
            boolean hasAttribute = currentPath.stream().anyMatch(conceptID -> conceptManager.getConcept(conceptID).isAttribute());
            if (!hasAttribute) {
                extendedPaths.add(currentPath);
            }
        }

        // If there exist a path without attributes, we don't need to expand any path
        // as paths contain attributes would be longer after implicit relations are added
        int numExtensionAllowed = extendedPaths.isEmpty() ? Integer.MAX_VALUE : 0;
        for (List<ConceptId> currentPath : allPaths) {
            List<ConceptId> extendedPath = new ArrayList<>();
            int numExtension = 0; // record the number of extensions needed for the current path
            for (int j = 0; j < currentPath.size() - 1; j++) {
                extendedPath.add(currentPath.get(j));
                ConceptId resourceRelationId = getResourceEdgeId(currentPath.get(j), currentPath.get(j + 1));
                if (resourceRelationId != null) {
                    numExtension++;
                    if (numExtension > numExtensionAllowed) break;
                    extendedPath.add(resourceRelationId);
                }
            }
            if (numExtension == numExtensionAllowed) {
                extendedPath.add(currentPath.get(currentPath.size() - 1));
                extendedPaths.add(extendedPath);
            } else if (numExtension < numExtensionAllowed) {
                extendedPath.add(currentPath.get(currentPath.size() - 1));
                extendedPaths.clear(); // longer paths are discarded
                extendedPaths.add(extendedPath);
                // update the minimum number of extensions needed so all the paths have the same length
                numExtensionAllowed = numExtension;
            }
        }
        return extendedPaths;
    }

    /**
     * Helper method to get the label IDs of role players in a relation
     *
     * @return a set of type label IDs
     */
    private Set<Label> scopeTypeLabelsImplicitPlayers(GraqlCompute query) {
        return scopeTypes(query)
                .filter(Concept::isRelationType)
                .map(Concept::asRelationType)
                .filter(RelationType::isImplicit)
                .flatMap(RelationType::roles)
                .flatMap(Role::players)
                .map(SchemaConcept::label)
                .collect(toSet());
    }

    /**
     * Get the resource edge id if there is one. Return null if not.
     */
    private ConceptId getResourceEdgeId(ConceptId conceptId1, ConceptId conceptId2) {
        if (Utility.mayHaveResourceEdge(conceptManager, conceptId1, conceptId2)) {
            Optional<Concept> firstConcept = executorFactory.transactional(true).match(
                    Graql.match(
                            var("x").id(conceptId1.getValue()),
                            var("y").id(conceptId2.getValue()),
                            var("z").rel(var("x")).rel(var("y"))))
                    .map(answer -> answer.get("z"))
                    .findFirst();
            if (firstConcept.isPresent()) {
                return firstConcept.get().id();
            }
        }
        return null;
    }

    /**
     * Helper method to get implicit relation types of attributes
     *
     * @param types
     * @return a set of type Labels
     */
    private static Set<Label> getAttributeImplicitRelationTypeLabes(Set<Type> types) {
        // If the sub graph contains attributes, we may need to add implicit relations to the path
        return types.stream()
                .filter(Concept::isAttributeType)
                .map(attributeType -> Schema.ImplicitType.HAS.getLabel(attributeType.label()))
                .collect(toSet());
    }

    /**
     * Helper method to get the types to be included in the query target
     *
     * @return a set of Types
     */
    private ImmutableSet<Type> targetTypes(Computable.Targetable<?> query) {
        if (query.of().isEmpty()) {
            throw GraqlSemanticException.statisticsAttributeTypesNotSpecified();
        }

        return query.of().stream()
                .map(t -> {
                    Label label = Label.of(t);
                    Type type = conceptManager.getSchemaConcept(label);
                    if (type == null) throw GraqlSemanticException.labelNotFound(label);
                    if (!type.isAttributeType()) throw GraqlSemanticException.mustBeAttributeType(type.label());
                    return type;
                })
                .flatMap(Type::subs)
                .collect(ImmutableSet.toImmutableSet());
    }

    /**
     * Helper method to get the labels of the type in the query target
     *
     * @return a set of type Labels
     */
    private Set<Label> targetTypeLabels(Computable.Targetable<?> query) {
        return targetTypes(query).stream()
                .map(SchemaConcept::label)
                .collect(ImmutableSet.toImmutableSet());
    }

    /**
     * Helper method to check whether the concept types in the target have any instances
     *
     * @return true if they exist, false if they don't
     */
    private boolean targetContainsInstance(GraqlCompute.Statistics.Value query) {
        Set<Label> targetLabels = targetTypeLabels(query);
        ImmutableSet<Label> scopeLabels = scopeTypeLabels(query);
        BiFunction<Label, Label, Pattern> patternFunction = (attributeType, type) -> Graql.and(
                Graql.var("x").has(attributeType.getValue(), Graql.var()),
                Graql.var("x").isa(Graql.type(type.getValue()))
        );
        return targetLabels.stream()
                .flatMap(attributeType ->
                        scopeLabels.stream()
                                .map(type -> patternFunction.apply(attributeType, type))
                                .map(pattern -> Graql.and(Collections.singleton(pattern)))
                                .flatMap(pattern -> traversalExecutor.traverse(pattern))
                ).findFirst().isPresent();
    }

    /**
     * Helper method to get all the concept types that should scope of compute query, which includes the implicit types
     * between attributes and entities, if target types were provided. This is used for compute statistics queries.
     *
     * @return a set of type labels
     */
    private Set<Label> extendedScopeTypeLabels(GraqlCompute.Statistics.Value query) {
        Set<Label> extendedTypeLabels = getAttributeImplicitRelationTypeLabes(targetTypes(query));
        extendedTypeLabels.addAll(scopeTypeLabels(query));
        extendedTypeLabels.addAll(query.of().stream().map(Label::of).collect(toSet()));
        return extendedTypeLabels;
    }

    /**
     * Helper method to get the types to be included in the query scope
     *
     * @return stream of Concept Types
     */
    private Stream<Type> scopeTypes(GraqlCompute query) {
        // Get all types if query.inTypes() is empty, else get all scoped types of each meta type.
        // Only include attributes and implicit "has-xxx" relations when user specifically asked for them.
        if (query.in().isEmpty()) {
            ImmutableSet.Builder<Type> typeBuilder = ImmutableSet.builder();

            if (scopeIncludesAttributes(query)) {
                // this implies that Attributes and Implicit relations are included
                // always set with compute count and statistics
                conceptManager.getMetaConcept().subs().forEach(typeBuilder::add);
            } else {
                conceptManager.getMetaEntityType().subs().forEach(typeBuilder::add);
                conceptManager.getMetaRelationType().subs()
                        .filter(relationType -> !relationType.isImplicit()).forEach(typeBuilder::add);
            }

            return typeBuilder.build().stream();
        } else {
            Stream<Type> subTypes = query.in().stream().map(t -> {
                Label label = Label.of(t);
                Type type = conceptManager.getType(label);
                if (type == null) throw GraqlSemanticException.labelNotFound(label);
                return type;
            }).flatMap(Type::subs);

            if (!scopeIncludesAttributes(query)) {
                subTypes = subTypes.filter(relationType -> !relationType.isImplicit());
            }

            return subTypes;
        }
    }
    /**
     * Helper method to get the labels of the type in the query scope
     *
     * @return a set of Concept Type Labels
     */
    private ImmutableSet<Label> scopeTypeLabels(GraqlCompute query) {
        return scopeTypes(query).map(SchemaConcept::label).collect(ImmutableSet.toImmutableSet());
    }


    /**
     * Helper method to check whether the concept types in the scope have any instances
     *
     * @return
     */
    private boolean scopeContainsInstance(GraqlCompute query) {
        Set<Label> labels = scopeTypeLabels(query);
        if (labels.isEmpty()) return false;

        return labels.stream()
                .map(type -> Graql.var("x").isa(Graql.type(type.getValue())))
                .map(Pattern.class::cast)
                .map(pattern -> Graql.and(Collections.singleton(pattern)))
                .flatMap(pattern -> traversalExecutor.traverse(pattern))
                .findFirst().isPresent();
    }

    /**
     * Helper method to check if concept instances exist in the query scope
     *
     * @param ids
     * @return true if they exist, false if they don't
     */
    private boolean scopeContainsInstances(GraqlCompute query, ConceptId... ids) {
        for (ConceptId id : ids) {
            Thing thing = conceptManager.getConcept(id);
            if (thing == null || !scopeTypeLabels(query).contains(thing.type().label())) return false;
        }
        return true;
    }

    /**
     * Helper method to check whether attribute types should be included in the query scope
     *
     * @return true if they exist, false if they don't
     */
    private boolean scopeIncludesAttributes(GraqlCompute query) {
        return query.includesAttributes() || scopeIncludesImplicitOrAttributeTypes(query);
    }

    /**
     * Helper method to check whether implicit or attribute types are included in the query scope
     *
     * @return true if they exist, false if they don't
     */
    private boolean scopeIncludesImplicitOrAttributeTypes(GraqlCompute query) {
        if (query.in().isEmpty()) return false;
        return query.in().stream().anyMatch(t -> {
            Label label = Label.of(t);
            SchemaConcept type = conceptManager.getSchemaConcept(label);
            return (type != null && (type.isAttributeType() || type.isImplicit()));
        });
    }

    /**
     * Helper method to convert type labels to IDs
     *
     * @param labelSet
     * @return a set of LabelIds
     */
    private Set<LabelId> convertLabelsToIds(Set<Label> labelSet) {
        return labelSet.stream()
                .map(conceptManager::convertToId)
                .filter(LabelId::isValid)
                .collect(toSet());
    }

}
