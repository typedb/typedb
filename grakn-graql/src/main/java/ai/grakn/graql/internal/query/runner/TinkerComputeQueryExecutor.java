/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.graql.internal.query.runner;

import ai.grakn.ComputeJob;
import ai.grakn.GraknComputer;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Label;
import ai.grakn.concept.LabelId;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.concept.Type;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.analytics.ComputeQuery;
import ai.grakn.graql.analytics.StatisticsQuery;
import ai.grakn.graql.analytics.ConnectedComponentQuery;
import ai.grakn.graql.analytics.CorenessQuery;
import ai.grakn.graql.analytics.CountQuery;
import ai.grakn.graql.analytics.DegreeQuery;
import ai.grakn.graql.analytics.KCoreQuery;
import ai.grakn.graql.analytics.MaxQuery;
import ai.grakn.graql.analytics.MeanQuery;
import ai.grakn.graql.analytics.MedianQuery;
import ai.grakn.graql.analytics.MinQuery;
import ai.grakn.graql.analytics.PathQuery;
import ai.grakn.graql.analytics.StdQuery;
import ai.grakn.graql.analytics.SumQuery;
import ai.grakn.graql.internal.analytics.ClusterMemberMapReduce;
import ai.grakn.graql.internal.analytics.ClusterSizeMapReduce;
import ai.grakn.graql.internal.analytics.ConnectedComponentVertexProgram;
import ai.grakn.graql.internal.analytics.ConnectedComponentsVertexProgram;
import ai.grakn.graql.internal.analytics.CorenessVertexProgram;
import ai.grakn.graql.internal.analytics.CountMapReduceWithAttribute;
import ai.grakn.graql.internal.analytics.CountVertexProgram;
import ai.grakn.graql.internal.analytics.DegreeDistributionMapReduce;
import ai.grakn.graql.internal.analytics.DegreeStatisticsVertexProgram;
import ai.grakn.graql.internal.analytics.DegreeVertexProgram;
import ai.grakn.graql.internal.analytics.GraknMapReduce;
import ai.grakn.graql.internal.analytics.GraknVertexProgram;
import ai.grakn.graql.internal.analytics.KCoreVertexProgram;
import ai.grakn.graql.internal.analytics.MaxMapReduce;
import ai.grakn.graql.internal.analytics.MeanMapReduce;
import ai.grakn.graql.internal.analytics.MedianVertexProgram;
import ai.grakn.graql.internal.analytics.MinMapReduce;
import ai.grakn.graql.internal.analytics.NoResultException;
import ai.grakn.graql.internal.analytics.ShortestPathVertexProgram;
import ai.grakn.graql.internal.analytics.StdMapReduce;
import ai.grakn.graql.internal.analytics.SumMapReduce;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author Felix Chapman
 */
public class TinkerComputeQueryExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(TinkerComputeQueryExecutor.class);
    // TODO: rename this too
    private final EmbeddedGraknTx<?> tx;

    private TinkerComputeQueryExecutor(EmbeddedGraknTx<?> tx) {
        this.tx = tx;
    }

    static TinkerComputeQueryExecutor create(EmbeddedGraknTx<?> tx) {
        return new TinkerComputeQueryExecutor(tx);
    }

    public <T> ComputeJob<T> run(ConnectedComponentQuery<T> query) {
        return runCompute(query, tinkerComputeQuery -> {
            if (!tinkerComputeQuery.selectedTypesHaveInstance()) {
                LOG.info("Selected types don't have instances");
                return (T) Collections.emptyMap();
            }

            Set<LabelId> subLabelIds = convertLabelsToIds(tinkerComputeQuery.subLabels());

            GraknVertexProgram<?> vertexProgram;
            if (query.start().isPresent()) {
                ConceptId conceptId = query.start().get();
                if (!tinkerComputeQuery.verticesExistInSubgraph(conceptId)) {
                    throw GraqlQueryException.instanceDoesNotExist();
                }
                vertexProgram = new ConnectedComponentVertexProgram(conceptId);
            } else {
                vertexProgram = new ConnectedComponentsVertexProgram();
            }

            GraknMapReduce<?> mapReduce;

            if(query.size().isPresent()){
                if (query.isMembersSet()) {
                    mapReduce = new ClusterMemberMapReduce(ConnectedComponentsVertexProgram.CLUSTER_LABEL, query.size().get());
                } else {
                    mapReduce = new ClusterSizeMapReduce(ConnectedComponentsVertexProgram.CLUSTER_LABEL, query.size().get());
                }
            } else {
                if (query.isMembersSet()) {
                    mapReduce = new ClusterMemberMapReduce(ConnectedComponentsVertexProgram.CLUSTER_LABEL);
                } else {
                    mapReduce = new ClusterSizeMapReduce(ConnectedComponentsVertexProgram.CLUSTER_LABEL);
                }
            }

            Memory memory = tinkerComputeQuery.compute(vertexProgram, mapReduce, subLabelIds).memory();
            return memory.get(mapReduce.getClass().getName());
        });
    }

    public ComputeJob<Map<Long, Set<String>>> run(CorenessQuery query) {
        return runCompute(query, tinkerComputeQuery -> {
            long k = query.minK();

            if (k < 2L) throw GraqlQueryException.kValueSmallerThanTwo();

            Set<Label> ofLabels;

            // Check if ofType is valid before returning emptyMap
            if (query.ofTypes().isEmpty()) {
                ofLabels = tinkerComputeQuery.subLabels();
            } else {
                ofLabels = query.ofTypes().stream()
                        .flatMap(typeLabel -> {
                            Type type = tx.getSchemaConcept(typeLabel);
                            if (type == null) throw GraqlQueryException.labelNotFound(typeLabel);
                            if (type.isRelationshipType()) throw GraqlQueryException.kCoreOnRelationshipType(typeLabel);
                            return type.subs();
                        })
                        .map(SchemaConcept::getLabel)
                        .collect(Collectors.toSet());
            }

            Set<Label> subLabels = Sets.union(tinkerComputeQuery.subLabels(), ofLabels);

            if (!tinkerComputeQuery.selectedTypesHaveInstance()) {
                return Collections.emptyMap();
            }

            ComputerResult result;
            Set<LabelId> subLabelIds = convertLabelsToIds(subLabels);
            Set<LabelId> ofLabelIds = convertLabelsToIds(ofLabels);

            try {
                result = tinkerComputeQuery.compute(
                        new CorenessVertexProgram(k),
                        new DegreeDistributionMapReduce(ofLabelIds, CorenessVertexProgram.CORENESS),
                        subLabelIds);
            } catch (NoResultException e) {
                return Collections.emptyMap();
            }

            return result.memory().get(DegreeDistributionMapReduce.class.getName());
        });
    }

    public ComputeJob<Long> run(CountQuery query) {
        return runCompute(query, tinkerComputeQuery -> {

            if (!tinkerComputeQuery.selectedTypesHaveInstance()) {
                LOG.debug("Count = 0");
                return 0L;
            }

            Set<LabelId> typeLabelIds = convertLabelsToIds(tinkerComputeQuery.subLabels());
            Map<Integer, Long> count;

            Set<LabelId> rolePlayerLabelIds = tinkerComputeQuery.getRolePlayerLabelIds();
            rolePlayerLabelIds.addAll(typeLabelIds);

            ComputerResult result = tinkerComputeQuery.compute(
                    new CountVertexProgram(),
                    new CountMapReduceWithAttribute(),
                    rolePlayerLabelIds, false);
            count = result.memory().get(CountMapReduceWithAttribute.class.getName());

            long finalCount = count.keySet().stream()
                    .filter(id -> typeLabelIds.contains(LabelId.of(id)))
                    .mapToLong(count::get).sum();
            if (count.containsKey(GraknMapReduce.RESERVED_TYPE_LABEL_KEY)) {
                finalCount += count.get(GraknMapReduce.RESERVED_TYPE_LABEL_KEY);
            }

            LOG.debug("Count = " + finalCount);
            return finalCount;
        });
    }

    public ComputeJob<Map<Long, Set<String>>> run(DegreeQuery query) {
        return runCompute(query, tinkerComputeQuery -> {
            Set<Label> ofLabels;

            // Check if ofType is valid before returning emptyMap
            if (query.ofTypes().isEmpty()) {
                ofLabels = tinkerComputeQuery.subLabels();
            } else {
                ofLabels = query.ofTypes().stream()
                        .flatMap(typeLabel -> {
                            Type type = tx.getSchemaConcept(typeLabel);
                            if (type == null) throw GraqlQueryException.labelNotFound(typeLabel);
                            return type.subs();
                        })
                        .map(SchemaConcept::getLabel)
                        .collect(Collectors.toSet());
            }

            Set<Label> subLabels = Sets.union(tinkerComputeQuery.subLabels(), ofLabels);

            if (!tinkerComputeQuery.selectedTypesHaveInstance()) {
                return Collections.emptyMap();
            }

            Set<LabelId> subLabelIds = convertLabelsToIds(subLabels);
            Set<LabelId> ofLabelIds = convertLabelsToIds(ofLabels);

            ComputerResult result = tinkerComputeQuery.compute(
                    new DegreeVertexProgram(ofLabelIds),
                    new DegreeDistributionMapReduce(ofLabelIds, DegreeVertexProgram.DEGREE),
                    subLabelIds);

            return result.memory().get(DegreeDistributionMapReduce.class.getName());
        });
    }

    public ComputeJob<Map<String, Set<String>>> run(KCoreQuery query) {
        return runCompute(query, tinkerComputeQuery -> {
            long k = query.k();

            if (k < 2L) throw GraqlQueryException.kValueSmallerThanTwo();

            if (!tinkerComputeQuery.selectedTypesHaveInstance()) {
                return Collections.emptyMap();
            }

            ComputerResult result;
            Set<LabelId> subLabelIds = convertLabelsToIds(tinkerComputeQuery.subLabels());
            try {
                result = tinkerComputeQuery.compute(
                        new KCoreVertexProgram(k),
                        new ClusterMemberMapReduce(KCoreVertexProgram.K_CORE_LABEL),
                        subLabelIds);
            } catch (NoResultException e) {
                return Collections.emptyMap();
            }

            return result.memory().get(ClusterMemberMapReduce.class.getName());
        });
    }

    public ComputeJob<Optional<Number>> run(MaxQuery query) {

        return new TinkerComputeJob<Optional<Number>>(tx,
                computer -> {
                    TinkerStatisticsQuery tinkerComputeQuery = TinkerStatisticsQuery.create(tx, query, computer);
                    AttributeType.DataType<?> dataType = tinkerComputeQuery.getDataTypeOfSelectedResourceTypes();
                    if (!tinkerComputeQuery.selectedResourceTypesHaveInstance()) {
                        return Optional.empty();
                    }
                    Set<LabelId> allSubLabelIds = convertLabelsToIds(tinkerComputeQuery.getCombinedSubTypes());
                    Set<LabelId> statisticsResourceLabelIds = convertLabelsToIds(tinkerComputeQuery.statisticsResourceLabels());

                    MaxMapReduce mapReduce = new MaxMapReduce(statisticsResourceLabelIds, dataType, DegreeVertexProgram.DEGREE);

                    ComputerResult result = tinkerComputeQuery.compute(
                            new DegreeStatisticsVertexProgram(statisticsResourceLabelIds),
                            mapReduce,
                            allSubLabelIds);
                    Map<Serializable, Number> map = result.memory().get(mapReduce.getClass().getName());

                    LOG.debug("Result = " + map.get(MapReduce.NullObject.instance()));

                    Number max = map.get(MapReduce.NullObject.instance());
                    return Optional.of(max);
                });
    }

    public ComputeJob<Optional<Double>> run(MeanQuery query) {

        return new TinkerComputeJob<Optional<Double>>(tx,
                computer -> {
                    TinkerStatisticsQuery tinkerComputeQuery = TinkerStatisticsQuery.create(tx, query, computer);
                    AttributeType.DataType<?> dataType = tinkerComputeQuery.getDataTypeOfSelectedResourceTypes();
                    if (!tinkerComputeQuery.selectedResourceTypesHaveInstance()) {
                        return Optional.empty();
                    }
                    Set<LabelId> allSubLabelIds = convertLabelsToIds(tinkerComputeQuery.getCombinedSubTypes());
                    Set<LabelId> statisticsResourceLabelIds = convertLabelsToIds(tinkerComputeQuery.statisticsResourceLabels());

                    MeanMapReduce mapReduce = new MeanMapReduce(statisticsResourceLabelIds, dataType, DegreeVertexProgram.DEGREE);

                    ComputerResult result = tinkerComputeQuery.compute(
                            new DegreeStatisticsVertexProgram(statisticsResourceLabelIds),
                            mapReduce,
                            allSubLabelIds);
                    Map<Serializable, Map<String, Double>> map = result.memory().get(mapReduce.getClass().getName());

                    LOG.debug("Result = " + map.get(MapReduce.NullObject.instance()));

                    Map<String, Double> meanPair = map.get(MapReduce.NullObject.instance());
                    Double mean = meanPair.get(MeanMapReduce.SUM) / meanPair.get(MeanMapReduce.COUNT);
                    return Optional.of(mean);
                });
    }

    public ComputeJob<Optional<Number>> run(MedianQuery query) {

        return new TinkerComputeJob<Optional<Number>>(tx, query);
    }

    public ComputeJob<Optional<Number>> run(MinQuery query) {
        return new TinkerComputeJob<Optional<Number>>(tx,
                computer -> {
                    TinkerStatisticsQuery tinkerComputeQuery = TinkerStatisticsQuery.create(tx, query, computer);
                    AttributeType.DataType<?> dataType = tinkerComputeQuery.getDataTypeOfSelectedResourceTypes();
                    if (!tinkerComputeQuery.selectedResourceTypesHaveInstance()) {
                        return Optional.empty();
                    }
                    Set<LabelId> allSubLabelIds = convertLabelsToIds(tinkerComputeQuery.getCombinedSubTypes());
                    Set<LabelId> statisticsResourceLabelIds = convertLabelsToIds(tinkerComputeQuery.statisticsResourceLabels());

                    MinMapReduce mapReduce = new MinMapReduce(statisticsResourceLabelIds, dataType, DegreeVertexProgram.DEGREE);

                    ComputerResult result = tinkerComputeQuery.compute(
                            new DegreeStatisticsVertexProgram(statisticsResourceLabelIds),
                            mapReduce,
                            allSubLabelIds);
                    Map<Serializable, Number> map = result.memory().get(mapReduce.getClass().getName());

                    LOG.debug("Result = " + map.get(MapReduce.NullObject.instance()));

                    Number min = map.get(MapReduce.NullObject.instance());
                    return Optional.of(min);
                });
    }

    public TinkerComputeJob<List<List<Concept>>> run(PathQuery query) {
        return runCompute(query, tinkerComputeQuery -> {

            ConceptId sourceId = query.from().get();
            ConceptId destinationId = query.to().get();

            if (!tinkerComputeQuery.verticesExistInSubgraph(sourceId, destinationId)) {
                throw GraqlQueryException.instanceDoesNotExist();
            }
            if (sourceId.equals(destinationId)) {
                return Collections.singletonList(Collections.singletonList(tx.getConcept(sourceId)));
            }

            ComputerResult result;
            Set<LabelId> subLabelIds = convertLabelsToIds(tinkerComputeQuery.subLabels());
            try {
                result = tinkerComputeQuery.compute(
                        new ShortestPathVertexProgram(sourceId, destinationId), null, subLabelIds);
            } catch (NoResultException e) {
                return Collections.emptyList();
            }

            Multimap<Concept, Concept> predecessorMapFromSource = tinkerComputeQuery.getPredecessorMap(result);
            List<List<Concept>> allPaths = tinkerComputeQuery.getAllPaths(predecessorMapFromSource, sourceId);
            if (tinkerComputeQuery.isAttributeIncluded()) { // this can be slow
                return tinkerComputeQuery.getExtendedPaths(allPaths);
            }

            LOG.info("Number of path: " + allPaths.size());
            return allPaths;
        });
    }

    public ComputeJob<Optional<Double>> run(StdQuery query) {

        return new TinkerComputeJob<Optional<Double>>(tx,
                computer -> {
                    TinkerStatisticsQuery tinkerComputeQuery = TinkerStatisticsQuery.create(tx, query, computer);
                    AttributeType.DataType<?> dataType = tinkerComputeQuery.getDataTypeOfSelectedResourceTypes();
                    if (!tinkerComputeQuery.selectedResourceTypesHaveInstance()) {
                        return Optional.empty();
                    }
                    Set<LabelId> allSubLabelIds = convertLabelsToIds(tinkerComputeQuery.getCombinedSubTypes());
                    Set<LabelId> statisticsResourceLabelIds = convertLabelsToIds(tinkerComputeQuery.statisticsResourceLabels());

                    StdMapReduce mapReduce = new StdMapReduce(statisticsResourceLabelIds, dataType, DegreeVertexProgram.DEGREE);

                    ComputerResult result = tinkerComputeQuery.compute(
                            new DegreeStatisticsVertexProgram(statisticsResourceLabelIds),
                            mapReduce,
                            allSubLabelIds);
                    Map<Serializable, Map<String, Double>> map = result.memory().get(mapReduce.getClass().getName());

                    LOG.debug("Result = " + map.get(MapReduce.NullObject.instance()));

                    Map<String, Double> stdTuple = map.get(MapReduce.NullObject.instance());
                    double squareSum = stdTuple.get(StdMapReduce.SQUARE_SUM);
                    double sum = stdTuple.get(StdMapReduce.SUM);
                    double count = stdTuple.get(StdMapReduce.COUNT);
                    Double std = Math.sqrt(squareSum / count - (sum / count) * (sum / count));
                    return Optional.of(std);
                });
    }

    public ComputeJob<Optional<Number>> run(SumQuery query) {

        return new TinkerComputeJob<Optional<Number>>(tx,
                computer -> {
                    TinkerStatisticsQuery tinkerComputeQuery = TinkerStatisticsQuery.create(tx, query, computer);
                    AttributeType.DataType<?> dataType = tinkerComputeQuery.getDataTypeOfSelectedResourceTypes();
                    if (!tinkerComputeQuery.selectedResourceTypesHaveInstance()) {
                        return Optional.empty();
                    }
                    Set<LabelId> allSubLabelIds = convertLabelsToIds(tinkerComputeQuery.getCombinedSubTypes());
                    Set<LabelId> statisticsResourceLabelIds = convertLabelsToIds(tinkerComputeQuery.statisticsResourceLabels());

                    SumMapReduce mapReduce = new SumMapReduce(statisticsResourceLabelIds, dataType, DegreeVertexProgram.DEGREE);

                    ComputerResult result = tinkerComputeQuery.compute(
                            new DegreeStatisticsVertexProgram(statisticsResourceLabelIds),
                            mapReduce,
                            allSubLabelIds);
                    Map<Serializable, Number> map = result.memory().get(mapReduce.getClass().getName());

                    LOG.debug("Result = " + map.get(MapReduce.NullObject.instance()));

                    Number sum = map.get(MapReduce.NullObject.instance());
                    return Optional.of(sum);
                });
    }

    private <T, Q extends ComputeQuery<?>> TinkerComputeJob<T> runCompute(
            Q query, ComputeRunner<T, TinkerComputeQuery<Q>> runner) {

        return new TinkerComputeJob<T>(tx,
                computer -> {
                    TinkerComputeQuery<Q> tinkerComputeQuery = TinkerComputeQuery.create(tx, query, computer);
                    return runner.apply(tinkerComputeQuery);
                });
    }

    private Set<LabelId> convertLabelsToIds(Set<Label> labelSet) {
        return labelSet.stream()
                .map(tx::convertToId)
                .filter(LabelId::isValid)
                .collect(Collectors.toSet());
    }

    private interface ComputeRunner<T, Q extends TinkerComputeQuery<?>> {
        T apply(Q tinkerComputeQuery);
    }
}