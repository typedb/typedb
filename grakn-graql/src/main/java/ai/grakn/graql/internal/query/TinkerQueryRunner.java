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

package ai.grakn.graql.internal.query;

import ai.grakn.GraknComputer;
import ai.grakn.GraknTx;
import ai.grakn.QueryRunner;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Label;
import ai.grakn.concept.LabelId;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.concept.Thing;
import ai.grakn.concept.Type;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.AggregateQuery;
import ai.grakn.graql.ComputeQuery;
import ai.grakn.graql.ComputeQueryOf;
import ai.grakn.graql.DefineQuery;
import ai.grakn.graql.DeleteQuery;
import ai.grakn.graql.GetQuery;
import ai.grakn.graql.Graql;
import ai.grakn.graql.InsertQuery;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.UndefineQuery;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.analytics.ClusterQuery;
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
import ai.grakn.graql.internal.analytics.Utility;
import ai.grakn.graql.internal.util.AdminConverter;
import ai.grakn.util.Schema;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ai.grakn.graql.Graql.or;
import static ai.grakn.graql.Graql.var;
import static ai.grakn.graql.internal.analytics.GraknMapReduce.RESERVED_TYPE_LABEL_KEY;
import static ai.grakn.util.CommonUtil.toImmutableList;
import static ai.grakn.util.CommonUtil.toImmutableSet;
import static java.util.stream.Collectors.toList;

/**
 * @author Felix Chapman
 */
public class TinkerQueryRunner implements QueryRunner {

    private static final Logger LOG = LoggerFactory.getLogger(TinkerQueryRunner.class);

    @Override
    public Stream<Answer> run(GetQuery query) {
        return query.match().stream().map(result -> result.project(query.vars())).distinct();
    }

    @Override
    public Stream<Answer> run(InsertQuery query) {
        Collection<VarPatternAdmin> varPatterns = query.admin().varPatterns().stream()
                .flatMap(v -> v.innerVarPatterns().stream())
                .collect(toImmutableList());

        GraknTx tx = query.tx().orElseThrow(GraqlQueryException::noTx);

        return query.admin().match().map(
                match -> match.stream().map(answer -> QueryOperationExecutor.insertAll(varPatterns, tx, answer))
        ).orElseGet(
                () -> Stream.of(QueryOperationExecutor.insertAll(varPatterns, tx))
        );
    }

    @Override
    public void run(DeleteQuery query) {
        List<Answer> results = query.admin().match().stream().collect(toList());
        results.forEach(result -> deleteResult(result, query.admin().vars()));
    }

    @Override
    public Answer run(DefineQuery query) {
        GraknTx tx = query.tx().orElseThrow(GraqlQueryException::noTx);

        ImmutableList<VarPatternAdmin> allPatterns = AdminConverter.getVarAdmins(query.varPatterns()).stream()
                .flatMap(v -> v.innerVarPatterns().stream())
                .collect(toImmutableList());

        return QueryOperationExecutor.defineAll(allPatterns, tx);
    }

    @Override
    public void run(UndefineQuery query) {
        ImmutableList<VarPatternAdmin> allPatterns = AdminConverter.getVarAdmins(query.varPatterns()).stream()
                .flatMap(v -> v.innerVarPatterns().stream())
                .collect(toImmutableList());

        QueryOperationExecutor.undefineAll(allPatterns, query.tx().orElseThrow(GraqlQueryException::noTx));
    }

    @Override
    public <T> T run(AggregateQuery<T> query) {
        return query.aggregate().apply(query.match().stream());
    }

    @Override
    public <T> T run(ClusterQuery<T> query) {
        GraknTx tx = query.tx().orElseThrow(GraqlQueryException::noTx);
        GraknComputer computer = tx.session().getGraphComputer();

        if (!selectedTypesHaveInstance(query, tx)) {
            LOG.info("Selected types don't have instances");
            return (T) Collections.emptyMap();
        }

        Set<LabelId> subLabelIds = convertLabelsToIds(tx, subLabels(query, tx));

        GraknVertexProgram<?> vertexProgram;
        if (query.sourceId().isPresent()) {
            ConceptId conceptId = query.sourceId().get();
            if (!verticesExistInSubgraph(query, tx, conceptId)) {
                throw GraqlQueryException.instanceDoesNotExist();
            }
            vertexProgram = new ConnectedComponentVertexProgram(conceptId);
        } else {
            vertexProgram = new ConnectedComponentsVertexProgram();
        }

        Long clusterSize = query.clusterSize();
        boolean members = query.membersSet();

        GraknMapReduce<?> mapReduce;
        if (members) {
            if (clusterSize == null) {
                mapReduce = new ClusterMemberMapReduce(ConnectedComponentsVertexProgram.CLUSTER_LABEL);
            } else {
                mapReduce = new ClusterMemberMapReduce(ConnectedComponentsVertexProgram.CLUSTER_LABEL, clusterSize);
            }
        } else {
            if (clusterSize == null) {
                mapReduce = new ClusterSizeMapReduce(ConnectedComponentsVertexProgram.CLUSTER_LABEL);
            } else {
                mapReduce = new ClusterSizeMapReduce(ConnectedComponentsVertexProgram.CLUSTER_LABEL, clusterSize);
            }
        }

        Memory memory = computer.compute(vertexProgram, mapReduce, subLabelIds).memory();
        return memory.get(members ? ClusterMemberMapReduce.class.getName() : ClusterSizeMapReduce.class.getName());
    }

    @Override
    public Map<Long, Set<String>> run(CorenessQuery query) {
        return runCompute(query, (tx, computer) -> {
            long k = query.minK();

            if (k < 2L) throw GraqlQueryException.kValueSmallerThanTwo();

            Set<Label> ofLabels;

            // Check if ofType is valid before returning emptyMap
            if (query.ofLabels().isEmpty()) {
                ofLabels = subLabels(query, tx);
            } else {
                ofLabels = query.ofLabels().stream()
                        .flatMap(typeLabel -> {
                            Type type = tx.getSchemaConcept(typeLabel);
                            if (type == null) throw GraqlQueryException.labelNotFound(typeLabel);
                            if (type.isRelationshipType()) throw GraqlQueryException.kCoreOnRelationshipType(typeLabel);
                            return type.subs();
                        })
                        .map(SchemaConcept::getLabel)
                        .collect(Collectors.toSet());
            }

            Set<Label> subLabels = Sets.union(subLabels(query, tx), ofLabels);

            if (!selectedTypesHaveInstance(query, tx)) {
                return Collections.emptyMap();
            }

            ComputerResult result;
            Set<LabelId> subLabelIds = convertLabelsToIds(tx, subLabels);
            Set<LabelId> ofLabelIds = convertLabelsToIds(tx, ofLabels);

            try {
                result = computer.compute(
                        new CorenessVertexProgram(k),
                        new DegreeDistributionMapReduce(ofLabelIds, CorenessVertexProgram.CORENESS),
                        subLabelIds);
            } catch (NoResultException e) {
                return Collections.emptyMap();
            }

            return result.memory().get(DegreeDistributionMapReduce.class.getName());
        });
    }

    @Override
    public long run(CountQuery query) {
        return runCompute(query, (tx, computer) -> {

            if (!selectedTypesHaveInstance(query, tx)) {
                LOG.debug("Count = 0");
                return 0L;
            }

            Set<LabelId> typeLabelIds = convertLabelsToIds(tx, subLabels(query, tx));
            Map<Integer, Long> count;

            Set<LabelId> rolePlayerLabelIds = getRolePlayerLabelIds(query, tx);
            rolePlayerLabelIds.addAll(typeLabelIds);

            ComputerResult result = computer.compute(
                    new CountVertexProgram(),
                    new CountMapReduceWithAttribute(),
                    rolePlayerLabelIds, false);
            count = result.memory().get(CountMapReduceWithAttribute.class.getName());

            long finalCount = count.keySet().stream()
                    .filter(id -> typeLabelIds.contains(LabelId.of(id)))
                    .mapToLong(count::get).sum();
            if (count.containsKey(RESERVED_TYPE_LABEL_KEY)) {
                finalCount += count.get(RESERVED_TYPE_LABEL_KEY);
            }

            LOG.debug("Count = " + finalCount);
            return finalCount;
        });
    }

    @Override
    public Map<Long, Set<String>> run(DegreeQuery query) {
        return runCompute(query, (tx, computer) -> {
            Set<Label> ofLabels;

            // Check if ofType is valid before returning emptyMap
            if (query.ofLabels().isEmpty()) {
                ofLabels = subLabels(query, tx);
            } else {
                ofLabels = query.ofLabels().stream()
                        .flatMap(typeLabel -> {
                            Type type = tx.getSchemaConcept(typeLabel);
                            if (type == null) throw GraqlQueryException.labelNotFound(typeLabel);
                            return type.subs();
                        })
                        .map(SchemaConcept::getLabel)
                        .collect(Collectors.toSet());
            }

            Set<Label> subLabels = Sets.union(subLabels(query, tx), ofLabels);

            if (!selectedTypesHaveInstance(query, tx)) {
                return Collections.emptyMap();
            }

            Set<LabelId> subLabelIds = convertLabelsToIds(tx, subLabels);
            Set<LabelId> ofLabelIds = convertLabelsToIds(tx, ofLabels);

            ComputerResult result = computer.compute(
                    new DegreeVertexProgram(ofLabelIds),
                    new DegreeDistributionMapReduce(ofLabelIds, DegreeVertexProgram.DEGREE),
                    subLabelIds);

            return result.memory().get(DegreeDistributionMapReduce.class.getName());
        });
    }

    @Override
    public Map<String, Set<String>> run(KCoreQuery query) {
        return runCompute(query, (tx, computer) -> {
            long k = query.kValue();

            if (k < 2L) throw GraqlQueryException.kValueSmallerThanTwo();

            if (!selectedTypesHaveInstance(query, tx)) {
                return Collections.emptyMap();
            }

            ComputerResult result;
            Set<LabelId> subLabelIds = convertLabelsToIds(tx, subLabels(query, tx));
            try {
                result = computer.compute(
                        new KCoreVertexProgram(k),
                        new ClusterMemberMapReduce(KCoreVertexProgram.K_CORE_LABEL),
                        subLabelIds);
            } catch (NoResultException e) {
                return Collections.emptyMap();
            }

            return result.memory().get(ClusterMemberMapReduce.class.getName());
        });
    }

    @Override
    public Optional<Number> run(MaxQuery query) {
        return execWithMapReduce(query, MaxMapReduce::new);
    }

    @Override
    public Optional<Double> run(MeanQuery query) {
        Optional<Map<String, Double>> result = execWithMapReduce(query, MeanMapReduce::new);

        return result.map(meanPair ->
                meanPair.get(MeanMapReduce.SUM) / meanPair.get(MeanMapReduce.COUNT)
        );
    }

    @Override
    public Optional<Number> run(MedianQuery query) {
        return runCompute(query, (tx, computer) -> {
            AttributeType.DataType<?> dataType = getDataTypeOfSelectedResourceTypes(query, tx);
            if (!selectedResourceTypesHaveInstance(query, tx, statisticsResourceLabels(query, tx))) {
                return Optional.empty();
            }
            Set<LabelId> allSubLabelIds = convertLabelsToIds(tx, getCombinedSubTypes(query, tx));
            Set<LabelId> statisticsResourceLabelIds = convertLabelsToIds(tx, statisticsResourceLabels(query, tx));

            ComputerResult result = computer.compute(
                    new MedianVertexProgram(statisticsResourceLabelIds, dataType),
                    null, allSubLabelIds);

            Number finalResult = result.memory().get(MedianVertexProgram.MEDIAN);
            LOG.debug("Median = " + finalResult);

            return Optional.of(finalResult);
        });
    }

    @Override
    public Optional<Number> run(MinQuery query) {
        return execWithMapReduce(query, MinMapReduce::new);
    }

    @Override
    public Optional<List<Concept>> run(PathQuery query) {
        GraknTx tx = query.tx().orElseThrow(GraqlQueryException::noTx);

        PathsQuery pathsQuery = tx.graql().compute().paths();
        if (query.getIncludeAttribute()) pathsQuery = pathsQuery.includeAttribute();
        return pathsQuery.from(query.from()).to(query.to()).in(query.subLabels()).execute().stream().findAny();
    }

    @Override
    public List<List<Concept>> run(PathsQuery query) {
        return runCompute(query, (tx, computer) -> {

            ConceptId sourceId = query.from();
            ConceptId destinationId = query.to();

            if (!verticesExistInSubgraph(query, tx, sourceId, destinationId)) {
                throw GraqlQueryException.instanceDoesNotExist();
            }
            if (sourceId.equals(destinationId)) {
                return Collections.singletonList(Collections.singletonList(tx.getConcept(sourceId)));
            }

            ComputerResult result;
            Set<LabelId> subLabelIds = convertLabelsToIds(tx, subLabels(query, tx));
            try {
                result = computer.compute(
                        new ShortestPathVertexProgram(sourceId, destinationId), null, subLabelIds);
            } catch (NoResultException e) {
                return Collections.emptyList();
            }

            Multimap<Concept, Concept> predecessorMapFromSource = getPredecessorMap(tx, result);
            List<List<Concept>> allPaths = getAllPaths(tx, predecessorMapFromSource, sourceId);
            if (query.getIncludeAttribute()) { // this can be slow
                return getExtendedPaths(tx, allPaths);
            }

            LOG.info("Number of paths: " + allPaths.size());
            return allPaths;
        });
    }

    @Override
    public Optional<Double> run(StdQuery query) {
        Optional<Map<String, Double>> result = execWithMapReduce(query, StdMapReduce::new);

        return result.map(stdTuple -> {
            double squareSum = stdTuple.get(StdMapReduce.SQUARE_SUM);
            double sum = stdTuple.get(StdMapReduce.SUM);
            double count = stdTuple.get(StdMapReduce.COUNT);
            return Math.sqrt(squareSum / count - (sum / count) * (sum / count));
        });
    }

    @Override
    public Optional<Number> run(SumQuery query) {
        return execWithMapReduce(query, SumMapReduce::new);
    }

    private static <T, Q extends ComputeQuery<?>> T runCompute(Q query, ComputeRunner<T> runner) {
        GraknTx tx = query.tx().orElseThrow(GraqlQueryException::noTx);

        LOG.info(query + " started");
        long startTime = System.currentTimeMillis();

        GraknComputer computer = tx.session().getGraphComputer();

        T result = runner.apply(tx, computer);

        LOG.info(query + " finished in " + (System.currentTimeMillis() - startTime) + " ms");

        return result;
    }

    private interface ComputeRunner<T> {
        T apply(GraknTx tx, GraknComputer computer);
    }

    private void deleteResult(Answer result, Collection<? extends Var> vars) {
        Collection<? extends Var> toDelete = vars.isEmpty() ? result.vars() : vars;

        for (Var var : toDelete) {
            Concept concept = result.get(var);

            if (concept.isSchemaConcept()) {
                throw GraqlQueryException.deleteSchemaConcept(concept.asSchemaConcept());
            }

            concept.delete();
        }
    }

    private static Set<LabelId> convertLabelsToIds(GraknTx tx, Set<Label> labelSet) {
        return labelSet.stream()
                .map(tx.admin()::convertToId)
                .filter(LabelId::isValid)
                .collect(Collectors.toSet());
    }

    private boolean selectedTypesHaveInstance(ComputeQuery<?> query, GraknTx tx) {
        if (subLabels(query, tx).isEmpty()) {
            LOG.info("No types found while looking for instances");
            return false;
        }

        List<Pattern> checkSubtypes = subLabels(query, tx).stream()
                .map(type -> var("x").isa(Graql.label(type))).collect(toList());
        return tx.graql().infer(false).match(or(checkSubtypes)).iterator().hasNext();
    }

    private static ImmutableSet<Label> subLabels(ComputeQuery<?> query, GraknTx tx) {
        return subTypes(query, tx).map(SchemaConcept::getLabel).collect(toImmutableSet());
    }

    private static Stream<Type> subTypes(ComputeQuery<?> query, GraknTx tx) {
        // get all types if subGraph is empty, else get all subTypes of each type in subGraph
        // only include attributes and implicit "has-xxx" relationships when user specifically asked for them
        if (query.subLabels().isEmpty()) {
            ImmutableSet.Builder<Type> subTypesBuilder = ImmutableSet.builder();

            if (query.getIncludeAttribute()) {
                tx.admin().getMetaConcept().subs().forEach(subTypesBuilder::add);
            } else {
                tx.admin().getMetaEntityType().subs().forEach(subTypesBuilder::add);
                tx.admin().getMetaRelationType().subs()
                        .filter(relationshipType -> !relationshipType.isImplicit()).forEach(subTypesBuilder::add);
            }

            return subTypesBuilder.build().stream();
        } else {
            Stream<Type> subTypes = query.subLabels().stream().map(label -> {
                Type type = tx.getType(label);
                if (type == null) throw GraqlQueryException.labelNotFound(label);
                return type;
            }).flatMap(Type::subs);

            if (!query.getIncludeAttribute()) {
                subTypes = subTypes.filter(relationshipType -> !relationshipType.isImplicit());
            }

            return subTypes;
        }
    }

    private static boolean verticesExistInSubgraph(ComputeQuery<?> query, GraknTx tx, ConceptId... ids) {
        for (ConceptId id : ids) {
            Thing thing = tx.getConcept(id);
            if (thing == null || !subLabels(query, tx).contains(thing.type().getLabel())) return false;
        }
        return true;
    }

    private static Set<LabelId> getRolePlayerLabelIds(ComputeQuery<?> query, GraknTx tx) {
        return subTypes(query, tx)
                .filter(Concept::isRelationshipType)
                .map(Concept::asRelationshipType)
                .filter(RelationshipType::isImplicit)
                .flatMap(RelationshipType::relates)
                .flatMap(Role::playedByTypes)
                .map(type -> tx.admin().convertToId(type.getLabel()))
                .filter(LabelId::isValid)
                .collect(Collectors.toSet());
    }

    @Nullable
    private static AttributeType.DataType<?> getDataTypeOfSelectedResourceTypes(ComputeQueryOf<?> query, GraknTx tx) {
        AttributeType.DataType<?> dataType = null;
        for (Type type : calcStatisticsResourceTypes(query, tx)) {
            // check if the selected type is a resource-type
            if (!type.isAttributeType()) throw GraqlQueryException.mustBeAttributeType(type.getLabel());
            AttributeType<?> resourceType = type.asAttributeType();
            if (dataType == null) {
                // check if the resource-type has data-type LONG or DOUBLE
                dataType = resourceType.getDataType();
                if (!dataType.equals(AttributeType.DataType.LONG) &&
                        !dataType.equals(AttributeType.DataType.DOUBLE)) {
                    throw GraqlQueryException.resourceMustBeANumber(dataType, resourceType.getLabel());
                }

            } else {
                // check if all the resource-types have the same data-type
                if (!dataType.equals(resourceType.getDataType())) {
                    throw GraqlQueryException.resourcesWithDifferentDataTypes(query.ofLabels());
                }
            }
        }
        return dataType;
    }

    private static Set<Label> statisticsResourceLabels(ComputeQueryOf<?> query, GraknTx tx) {
        return calcStatisticsResourceTypes(query, tx).stream()
                .map(SchemaConcept::getLabel)
                .collect(toImmutableSet());
    }

    private static ImmutableSet<Type> calcStatisticsResourceTypes(ComputeQueryOf<?> query, GraknTx tx) {
        if (query.ofLabels().isEmpty()) {
            throw GraqlQueryException.statisticsAttributeTypesNotSpecified();
        }

        return query.ofLabels().stream()
                .map((label) -> {
                    Type type = tx.getSchemaConcept(label);
                    if (type == null) throw GraqlQueryException.labelNotFound(label);
                    if (!type.isAttributeType()) throw GraqlQueryException.mustBeAttributeType(type.getLabel());
                    return type;
                })
                .flatMap(Type::subs)
                .collect(toImmutableSet());
    }

    private static boolean selectedResourceTypesHaveInstance(ComputeQuery<?> query, GraknTx tx, Set<Label> statisticsResourceTypes) {
        for (Label resourceType : statisticsResourceTypes) {
            for (Label type : subLabels(query, tx)) {
                Boolean patternExist = tx.graql().infer(false).match(
                        var("x").has(resourceType, var()),
                        var("x").isa(Graql.label(type))
                ).iterator().hasNext();
                if (patternExist) return true;
            }
        }
        return false;
        //TODO: should use the following ask query when ask query is even lazier
//        List<Pattern> checkResourceTypes = statisticsResourceTypes.stream()
//                .map(type -> var("x").has(type, var())).collect(Collectors.toList());
//        List<Pattern> checkSubtypes = subLabels.stream()
//                .map(type -> var("x").isa(Graql.label(type))).collect(Collectors.toList());
//
//        return tx.get().graql().infer(false)
//                .match(or(checkResourceTypes), or(checkSubtypes)).aggregate(ask()).execute();
    }

    private static Set<Label> getCombinedSubTypes(ComputeQueryOf<?> query, GraknTx tx) {
        Set<Label> allSubTypes = getHasResourceRelationLabels(calcStatisticsResourceTypes(query, tx));
        allSubTypes.addAll(subLabels(query, tx));
        allSubTypes.addAll(query.ofLabels());
        return allSubTypes;
    }

    private static Set<Label> getHasResourceRelationLabels(Set<Type> subTypes) {
        return subTypes.stream()
                .filter(Concept::isAttributeType)
                .map(resourceType -> Schema.ImplicitType.HAS.getLabel(resourceType.getLabel()))
                .collect(Collectors.toSet());
    }

    // If the sub graph contains attributes, we may need to add implicit relations to the paths
    private List<List<Concept>> getExtendedPaths(GraknTx tx, List<List<Concept>> allPaths) {
        List<List<Concept>> extendedPaths = new ArrayList<>();
        for (List<Concept> currentPath : allPaths) {
            boolean hasAttribute = currentPath.stream().anyMatch(Concept::isAttribute);
            if (!hasAttribute) {
                extendedPaths.add(currentPath);
            }
        }

        // If there exist a path without attributes, we don't need to expand any path
        // as paths contain attributes would be longer after implicit relations are added
        int numExtensionAllowed = extendedPaths.isEmpty() ? Integer.MAX_VALUE : 0;
        for (List<Concept> currentPath : allPaths) {
            List<Concept> extendedPath = new ArrayList<>();
            int numExtension = 0; // record the number of extensions needed for the current path
            for (int j = 0; j < currentPath.size() - 1; j++) {
                extendedPath.add(currentPath.get(j));
                ConceptId resourceRelationId = Utility.getResourceEdgeId(tx,
                        currentPath.get(j).getId(), currentPath.get(j + 1).getId());
                if (resourceRelationId != null) {
                    numExtension++;
                    if (numExtension > numExtensionAllowed) break;
                    extendedPath.add(tx.getConcept(resourceRelationId));
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

    private static Multimap<Concept, Concept> getPredecessorMap(GraknTx tx, ComputerResult result) {
        Map<String, Set<String>> predecessorMapFromSource =
                result.memory().get(ShortestPathVertexProgram.PREDECESSORS_FROM_SOURCE);
        Map<String, Set<String>> predecessorMapFromDestination =
                result.memory().get(ShortestPathVertexProgram.PREDECESSORS_FROM_DESTINATION);

        Multimap<Concept, Concept> predecessors = HashMultimap.create();
        predecessorMapFromSource.forEach((id, idSet) -> idSet.forEach(id2 -> {
            predecessors.put(getConcept(tx, id), getConcept(tx, id2));
        }));
        predecessorMapFromDestination.forEach((id, idSet) -> idSet.forEach(id2 -> {
            predecessors.put(getConcept(tx, id2), getConcept(tx, id));
        }));
        return predecessors;
    }

    private List<List<Concept>> getAllPaths(
            GraknTx tx, Multimap<Concept, Concept> predecessorMapFromSource, ConceptId sourceId) {
        List<List<Concept>> allPaths = new ArrayList<>();
        List<Concept> firstPath = new ArrayList<>();
        firstPath.add(getConcept(tx, sourceId.getValue()));

        Deque<List<Concept>> queue = new ArrayDeque<>();
        queue.addLast(firstPath);
        while (!queue.isEmpty()) {
            List<Concept> currentPath = queue.pollFirst();
            if (predecessorMapFromSource.containsKey(currentPath.get(currentPath.size() - 1))) {
                Collection<Concept> successors = predecessorMapFromSource.get(currentPath.get(currentPath.size() - 1));
                Iterator<Concept> iterator = successors.iterator();
                for (int i = 0; i < successors.size() - 1; i++) {
                    List<Concept> extendedPath = new ArrayList<>(currentPath);
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

    @Nullable
    private static Thing getConcept(GraknTx tx, String conceptId) {
        return tx.getConcept(ConceptId.of(conceptId));
    }

    private static <T, Q extends ComputeQueryOf<?>> Optional<T> execWithMapReduce(
            Q query, MapReduceFactory<T> mapReduceFactory) {

        return runCompute(query, (tx, computer) -> {
            AttributeType.DataType<?> dataType = getDataTypeOfSelectedResourceTypes(query, tx);
            if (!selectedResourceTypesHaveInstance(query, tx, statisticsResourceLabels(query, tx))) {
                return Optional.empty();
            }
            Set<LabelId> allSubLabelIds = convertLabelsToIds(tx, getCombinedSubTypes(query, tx));
            Set<LabelId> statisticsResourceLabelIds = convertLabelsToIds(tx, statisticsResourceLabels(query, tx));

            GraknMapReduce<T> mapReduce =
                    mapReduceFactory.get(statisticsResourceLabelIds, dataType, DegreeVertexProgram.DEGREE);

            ComputerResult result = computer.compute(
                    new DegreeStatisticsVertexProgram(statisticsResourceLabelIds),
                    mapReduce,
                    allSubLabelIds);
            Map<Serializable, T> map = result.memory().get(mapReduce.getClass().getName());

            LOG.debug("Result = " + map.get(MapReduce.NullObject.instance()));
            return Optional.of(map.get(MapReduce.NullObject.instance()));
        });
    }

    interface MapReduceFactory<S> {
        GraknMapReduce<S> get(
                Set<LabelId> statisticsResourceLabelIds, AttributeType.DataType<?> dataType, String degreePropertyKey);
    }
}
