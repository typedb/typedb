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
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.graql.internal.query.runner;

/*-
 * #%L
 * grakn-graql
 * %%
 * Copyright (C) 2016 - 2018 Grakn Labs Ltd
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

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
import ai.grakn.graql.ComputeQuery;
import ai.grakn.graql.StatisticsQuery;
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
import ai.grakn.graql.analytics.PathsQuery;
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
public class TinkerComputeQueryRunner {
    private static final Logger LOG = LoggerFactory.getLogger(TinkerComputeQueryRunner.class);
    // TODO: rename this too
    private final EmbeddedGraknTx<?> tx;

    private TinkerComputeQueryRunner(EmbeddedGraknTx<?> tx) {
        this.tx = tx;
    }

    static TinkerComputeQueryRunner create(EmbeddedGraknTx<?> tx) {
        return new TinkerComputeQueryRunner(tx);
    }

    public <T> ComputeJob<T> run(ConnectedComponentQuery<T> query) {
        return runCompute(query, tinkerComputeQuery -> {
            if (!tinkerComputeQuery.selectedTypesHaveInstance()) {
                LOG.info("Selected types don't have instances");
                return (T) Collections.emptyMap();
            }

            Set<LabelId> subLabelIds = convertLabelsToIds(tinkerComputeQuery.subLabels());

            GraknVertexProgram<?> vertexProgram;
            if (query.sourceId().isPresent()) {
                ConceptId conceptId = query.sourceId().get();
                if (!tinkerComputeQuery.verticesExistInSubgraph(conceptId)) {
                    throw GraqlQueryException.instanceDoesNotExist();
                }
                vertexProgram = new ConnectedComponentVertexProgram(conceptId);
            } else {
                vertexProgram = new ConnectedComponentsVertexProgram();
            }

            Long clusterSize = query.clusterSize();

            GraknMapReduce<?> mapReduce;
            if (query.isMembersSet()) {
                mapReduce = new ClusterMemberMapReduce(ConnectedComponentsVertexProgram.CLUSTER_LABEL, clusterSize);
            } else {
                mapReduce = new ClusterSizeMapReduce(ConnectedComponentsVertexProgram.CLUSTER_LABEL, clusterSize);
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
            if (query.targetLabels().isEmpty()) {
                ofLabels = tinkerComputeQuery.subLabels();
            } else {
                ofLabels = query.targetLabels().stream()
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
            if (query.targetLabels().isEmpty()) {
                ofLabels = tinkerComputeQuery.subLabels();
            } else {
                ofLabels = query.targetLabels().stream()
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
            long k = query.kValue();

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
        return execWithMapReduce(query, MaxMapReduce::new);
    }

    public ComputeJob<Optional<Double>> run(MeanQuery query) {
        return execWithMapReduce(query, MeanMapReduce::new, meanPair ->
                meanPair.get(MeanMapReduce.SUM) / meanPair.get(MeanMapReduce.COUNT)
        );
    }

    public ComputeJob<Optional<Number>> run(MedianQuery query) {
        return runStatistics(query, tinkerComputeQuery -> {
            AttributeType.DataType<?> dataType = tinkerComputeQuery.getDataTypeOfSelectedResourceTypes();
            if (!tinkerComputeQuery.selectedResourceTypesHaveInstance()) {
                return Optional.empty();
            }
            Set<LabelId> allSubLabelIds = convertLabelsToIds(tinkerComputeQuery.getCombinedSubTypes());
            Set<LabelId> statisticsResourceLabelIds = convertLabelsToIds(tinkerComputeQuery.statisticsResourceLabels());

            ComputerResult result = tinkerComputeQuery.compute(
                    new MedianVertexProgram(statisticsResourceLabelIds, dataType),
                    null, allSubLabelIds);

            Number finalResult = result.memory().get(MedianVertexProgram.MEDIAN);
            LOG.debug("Median = " + finalResult);

            return Optional.of(finalResult);
        });
    }

    public ComputeJob<Optional<Number>> run(MinQuery query) {
        return execWithMapReduce(query, MinMapReduce::new);
    }

    public ComputeJob<Optional<List<Concept>>> run(PathQuery query) {
        PathsQuery pathsQuery = tx.graql().compute().paths();
        if (query.isAttributeIncluded()) pathsQuery = pathsQuery.includeAttribute();
        pathsQuery = pathsQuery.from(query.from()).to(query.to()).in(query.subLabels());

        return run(pathsQuery).map(result -> result.stream().findAny());
    }

    public TinkerComputeJob<List<List<Concept>>> run(PathsQuery query) {
        return runCompute(query, tinkerComputeQuery -> {

            ConceptId sourceId = query.from();
            ConceptId destinationId = query.to();

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

            LOG.info("Number of paths: " + allPaths.size());
            return allPaths;
        });
    }

    public ComputeJob<Optional<Double>> run(StdQuery query) {
        return execWithMapReduce(query, StdMapReduce::new, stdTuple -> {
            double squareSum = stdTuple.get(StdMapReduce.SQUARE_SUM);
            double sum = stdTuple.get(StdMapReduce.SUM);
            double count = stdTuple.get(StdMapReduce.COUNT);
            return Math.sqrt(squareSum / count - (sum / count) * (sum / count));
        });
    }

    public ComputeJob<Optional<Number>> run(SumQuery query) {
        return execWithMapReduce(query, SumMapReduce::new);
    }

    private <T, Q extends ComputeQuery<?>> TinkerComputeJob<T> runCompute(
            Q query, ComputeRunner<T, TinkerComputeQuery<Q>> runner) {
        return runComputeGeneric(computer -> TinkerComputeQuery.create(tx, query, computer), runner);
    }

    private <T, Q extends StatisticsQuery<?>> TinkerComputeJob<T> runStatistics(
            Q query, ComputeRunner<T, TinkerStatisticsQuery> runner) {
        return runComputeGeneric(computer -> TinkerStatisticsQuery.create(tx, query, computer), runner);
    }

    private <T, Q extends ComputeQuery<?>, TQ extends TinkerComputeQuery<Q>> TinkerComputeJob<T> runComputeGeneric(
            Function<GraknComputer, TQ> tinkerComputeQueryFactory, ComputeRunner<T, TQ> runner) {
        return TinkerComputeJob.create(tx.session(), computer -> {
            TQ tinkerComputeQuery = tinkerComputeQueryFactory.apply(computer);
            return runner.apply(tinkerComputeQuery);
        });
    }

    private Set<LabelId> convertLabelsToIds(Set<Label> labelSet) {
        return labelSet.stream()
                .map(tx::convertToId)
                .filter(LabelId::isValid)
                .collect(Collectors.toSet());
    }

    private <T, Q extends StatisticsQuery<?>> TinkerComputeJob<Optional<T>> execWithMapReduce(
            Q query, MapReduceFactory<T> mapReduceFactory) {
        return execWithMapReduce(query, mapReduceFactory, Function.identity());
    }

    private <T, S, Q extends StatisticsQuery<?>> TinkerComputeJob<Optional<S>> execWithMapReduce(
            Q query, MapReduceFactory<T> mapReduceFactory, Function<T, S> operator) {

        return runStatistics(query, tinkerComputeQuery -> {
            AttributeType.DataType<?> dataType = tinkerComputeQuery.getDataTypeOfSelectedResourceTypes();
            if (!tinkerComputeQuery.selectedResourceTypesHaveInstance()) {
                return Optional.empty();
            }
            Set<LabelId> allSubLabelIds = convertLabelsToIds(tinkerComputeQuery.getCombinedSubTypes());
            Set<LabelId> statisticsResourceLabelIds = convertLabelsToIds(tinkerComputeQuery.statisticsResourceLabels());

            GraknMapReduce<T> mapReduce =
                    mapReduceFactory.get(statisticsResourceLabelIds, dataType, DegreeVertexProgram.DEGREE);

            ComputerResult result = tinkerComputeQuery.compute(
                    new DegreeStatisticsVertexProgram(statisticsResourceLabelIds),
                    mapReduce,
                    allSubLabelIds);
            Map<Serializable, T> map = result.memory().get(mapReduce.getClass().getName());

            LOG.debug("Result = " + map.get(MapReduce.NullObject.instance()));
            return Optional.of(operator.apply(map.get(MapReduce.NullObject.instance())));
        });
    }

    private interface ComputeRunner<T, Q extends TinkerComputeQuery<?>> {
        T apply(Q tinkerComputeQuery);
    }

    private interface MapReduceFactory<S> {
        GraknMapReduce<S> get(
                Set<LabelId> statisticsResourceLabelIds, AttributeType.DataType<?> dataType, String degreePropertyKey
        );
    }
}
