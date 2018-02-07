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
import ai.grakn.graql.ComputeQuery;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.StatisticsQuery;
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
import ai.grakn.util.CommonUtil;
import ai.grakn.util.Schema;
import com.google.common.collect.HashMultimap;
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

/**
 *
 *
 * @author Felix Chapman
 */
public class TinkerComputeQueryRunner {
    private static final Logger LOG = LoggerFactory.getLogger(TinkerComputeQueryRunner.class);
    private final GraknTx tx;

    private TinkerComputeQueryRunner(GraknTx tx) {
        this.tx = tx;
    }

    static TinkerComputeQueryRunner create(GraknTx tx) {
        return new TinkerComputeQueryRunner(tx);
    }

    public <T> T run(ClusterQuery<T> query) {
        GraknComputer computer = tx.session().getGraphComputer();

        if (!selectedTypesHaveInstance(query)) {
            LOG.info("Selected types don't have instances");
            return (T) Collections.emptyMap();
        }

        Set<LabelId> subLabelIds = convertLabelsToIds(subLabels(query));

        GraknVertexProgram<?> vertexProgram;
        if (query.sourceId().isPresent()) {
            ConceptId conceptId = query.sourceId().get();
            if (!verticesExistInSubgraph(query, conceptId)) {
                throw GraqlQueryException.instanceDoesNotExist();
            }
            vertexProgram = new ConnectedComponentVertexProgram(conceptId);
        } else {
            vertexProgram = new ConnectedComponentsVertexProgram();
        }

        Long clusterSize = query.clusterSize();
        boolean members = query.isMembersSet();

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

    public Map<Long, Set<String>> run(CorenessQuery query) {
        return runCompute(query, computer -> {
            long k = query.minK();

            if (k < 2L) throw GraqlQueryException.kValueSmallerThanTwo();

            Set<Label> ofLabels;

            // Check if ofType is valid before returning emptyMap
            if (query.targetLabels().isEmpty()) {
                ofLabels = subLabels(query);
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

            Set<Label> subLabels = Sets.union(subLabels(query), ofLabels);

            if (!selectedTypesHaveInstance(query)) {
                return Collections.emptyMap();
            }

            ComputerResult result;
            Set<LabelId> subLabelIds = convertLabelsToIds(subLabels);
            Set<LabelId> ofLabelIds = convertLabelsToIds(ofLabels);

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

    public long run(CountQuery query) {
        return runCompute(query, computer -> {

            if (!selectedTypesHaveInstance(query)) {
                LOG.debug("Count = 0");
                return 0L;
            }

            Set<LabelId> typeLabelIds = convertLabelsToIds(subLabels(query));
            Map<Integer, Long> count;

            Set<LabelId> rolePlayerLabelIds = getRolePlayerLabelIds(query);
            rolePlayerLabelIds.addAll(typeLabelIds);

            ComputerResult result = computer.compute(
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

    public Map<Long, Set<String>> run(DegreeQuery query) {
        return runCompute(query, computer -> {
            Set<Label> ofLabels;

            // Check if ofType is valid before returning emptyMap
            if (query.targetLabels().isEmpty()) {
                ofLabels = subLabels(query);
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

            Set<Label> subLabels = Sets.union(subLabels(query), ofLabels);

            if (!selectedTypesHaveInstance(query)) {
                return Collections.emptyMap();
            }

            Set<LabelId> subLabelIds = convertLabelsToIds(subLabels);
            Set<LabelId> ofLabelIds = convertLabelsToIds(ofLabels);

            ComputerResult result = computer.compute(
                    new DegreeVertexProgram(ofLabelIds),
                    new DegreeDistributionMapReduce(ofLabelIds, DegreeVertexProgram.DEGREE),
                    subLabelIds);

            return result.memory().get(DegreeDistributionMapReduce.class.getName());
        });
    }

    public Map<String, Set<String>> run(KCoreQuery query) {
        return runCompute(query, computer -> {
            long k = query.kValue();

            if (k < 2L) throw GraqlQueryException.kValueSmallerThanTwo();

            if (!selectedTypesHaveInstance(query)) {
                return Collections.emptyMap();
            }

            ComputerResult result;
            Set<LabelId> subLabelIds = convertLabelsToIds(subLabels(query));
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

    public Optional<Number> run(MaxQuery query) {
        return execWithMapReduce(query, MaxMapReduce::new);
    }

    public Optional<Double> run(MeanQuery query) {
        Optional<Map<String, Double>> result = execWithMapReduce(query, MeanMapReduce::new);

        return result.map(meanPair ->
                meanPair.get(MeanMapReduce.SUM) / meanPair.get(MeanMapReduce.COUNT)
        );
    }

    public Optional<Number> run(MedianQuery query) {
        return runCompute(query, computer -> {
            AttributeType.DataType<?> dataType = getDataTypeOfSelectedResourceTypes(query);
            if (!selectedResourceTypesHaveInstance(query, statisticsResourceLabels(query))) {
                return Optional.empty();
            }
            Set<LabelId> allSubLabelIds = convertLabelsToIds(getCombinedSubTypes(query));
            Set<LabelId> statisticsResourceLabelIds = convertLabelsToIds(statisticsResourceLabels(query));

            ComputerResult result = computer.compute(
                    new MedianVertexProgram(statisticsResourceLabelIds, dataType),
                    null, allSubLabelIds);

            Number finalResult = result.memory().get(MedianVertexProgram.MEDIAN);
            LOG.debug("Median = " + finalResult);

            return Optional.of(finalResult);
        });
    }

    public Optional<Number> run(MinQuery query) {
        return execWithMapReduce(query, MinMapReduce::new);
    }

    public Optional<List<Concept>> run(PathQuery query) {
        PathsQuery pathsQuery = tx.graql().compute().paths();
        if (isAttributeIncluded(query)) pathsQuery = pathsQuery.includeAttribute();
        return pathsQuery.from(query.from()).to(query.to()).in(query.subLabels()).execute().stream().findAny();
    }

    public List<List<Concept>> run(PathsQuery query) {
        return runCompute(query, computer -> {

            ConceptId sourceId = query.from();
            ConceptId destinationId = query.to();

            if (!verticesExistInSubgraph(query, sourceId, destinationId)) {
                throw GraqlQueryException.instanceDoesNotExist();
            }
            if (sourceId.equals(destinationId)) {
                return Collections.singletonList(Collections.singletonList(tx.getConcept(sourceId)));
            }

            ComputerResult result;
            Set<LabelId> subLabelIds = convertLabelsToIds(subLabels(query));
            try {
                result = computer.compute(
                        new ShortestPathVertexProgram(sourceId, destinationId), null, subLabelIds);
            } catch (NoResultException e) {
                return Collections.emptyList();
            }

            Multimap<Concept, Concept> predecessorMapFromSource = getPredecessorMap(result);
            List<List<Concept>> allPaths = getAllPaths(predecessorMapFromSource, sourceId);
            if (isAttributeIncluded(query)) { // this can be slow
                return getExtendedPaths(allPaths);
            }

            LOG.info("Number of paths: " + allPaths.size());
            return allPaths;
        });
    }

    public Optional<Double> run(StdQuery query) {
        Optional<Map<String, Double>> result = execWithMapReduce(query, StdMapReduce::new);

        return result.map(stdTuple -> {
            double squareSum = stdTuple.get(StdMapReduce.SQUARE_SUM);
            double sum = stdTuple.get(StdMapReduce.SUM);
            double count = stdTuple.get(StdMapReduce.COUNT);
            return Math.sqrt(squareSum / count - (sum / count) * (sum / count));
        });
    }

    public Optional<Number> run(SumQuery query) {
        return execWithMapReduce(query, SumMapReduce::new);
    }

    private <T, Q extends ComputeQuery<?>> T runCompute(Q query, ComputeRunner<T> runner) {
        LOG.info(query + " started");
        long startTime = System.currentTimeMillis();

        GraknComputer computer = tx.session().getGraphComputer();

        T result = runner.apply(computer);

        LOG.info(query + " finished in " + (System.currentTimeMillis() - startTime) + " ms");

        return result;
    }

    private Set<LabelId> convertLabelsToIds(Set<Label> labelSet) {
        return labelSet.stream()
                .map(tx.admin()::convertToId)
                .filter(LabelId::isValid)
                .collect(Collectors.toSet());
    }

    private boolean selectedTypesHaveInstance(ComputeQuery<?> query) {
        if (subLabels(query).isEmpty()) {
            LOG.info("No types found while looking for instances");
            return false;
        }

        List<Pattern> checkSubtypes = subLabels(query).stream()
                .map(type -> Graql.var("x").isa(Graql.label(type))).collect(Collectors.toList());
        return tx.graql().infer(false).match(Graql.or(checkSubtypes)).iterator().hasNext();
    }

    private ImmutableSet<Label> subLabels(ComputeQuery<?> query) {
        return subTypes(query).map(SchemaConcept::getLabel).collect(CommonUtil.toImmutableSet());
    }

    private Stream<Type> subTypes(ComputeQuery<?> query) {
        // get all types if subGraph is empty, else get all subTypes of each type in subGraph
        // only include attributes and implicit "has-xxx" relationships when user specifically asked for them
        if (query.subLabels().isEmpty()) {
            ImmutableSet.Builder<Type> subTypesBuilder = ImmutableSet.builder();

            if (isAttributeIncluded(query)) {
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

            if (!isAttributeIncluded(query)) {
                subTypes = subTypes.filter(relationshipType -> !relationshipType.isImplicit());
            }

            return subTypes;
        }
    }

    private boolean verticesExistInSubgraph(ComputeQuery<?> query, ConceptId... ids) {
        for (ConceptId id : ids) {
            Thing thing = tx.getConcept(id);
            if (thing == null || !subLabels(query).contains(thing.type().getLabel())) return false;
        }
        return true;
    }

    private Set<LabelId> getRolePlayerLabelIds(ComputeQuery<?> query) {
        return subTypes(query)
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
    private AttributeType.DataType<?> getDataTypeOfSelectedResourceTypes(StatisticsQuery<?> query) {
        AttributeType.DataType<?> dataType = null;
        for (Type type : calcStatisticsResourceTypes(query)) {
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
                    throw GraqlQueryException.resourcesWithDifferentDataTypes(query.attributeLabels());
                }
            }
        }
        return dataType;
    }

    private Set<Label> statisticsResourceLabels(StatisticsQuery<?> query) {
        return calcStatisticsResourceTypes(query).stream()
                .map(SchemaConcept::getLabel)
                .collect(CommonUtil.toImmutableSet());
    }

    private ImmutableSet<Type> calcStatisticsResourceTypes(StatisticsQuery<?> query) {
        if (query.attributeLabels().isEmpty()) {
            throw GraqlQueryException.statisticsAttributeTypesNotSpecified();
        }

        return query.attributeLabels().stream()
                .map((label) -> {
                    Type type = this.tx.getSchemaConcept(label);
                    if (type == null) throw GraqlQueryException.labelNotFound(label);
                    if (!type.isAttributeType()) throw GraqlQueryException.mustBeAttributeType(type.getLabel());
                    return type;
                })
                .flatMap(Type::subs)
                .collect(CommonUtil.toImmutableSet());
    }

    private boolean selectedResourceTypesHaveInstance(ComputeQuery<?> query, Set<Label> statisticsResourceTypes) {
        for (Label resourceType : statisticsResourceTypes) {
            for (Label type : subLabels(query)) {
                Boolean patternExist = tx.graql().infer(false).match(
                        Graql.var("x").has(resourceType, Graql.var()),
                        Graql.var("x").isa(Graql.label(type))
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

    private Set<Label> getCombinedSubTypes(StatisticsQuery<?> query) {
        Set<Label> allSubTypes = getHasResourceRelationLabels(calcStatisticsResourceTypes(query));
        allSubTypes.addAll(subLabels(query));
        allSubTypes.addAll(query.attributeLabels());
        return allSubTypes;
    }

    private Set<Label> getHasResourceRelationLabels(Set<Type> subTypes) {
        return subTypes.stream()
                .filter(Concept::isAttributeType)
                .map(resourceType -> Schema.ImplicitType.HAS.getLabel(resourceType.getLabel()))
                .collect(Collectors.toSet());
    }// If the sub graph contains attributes, we may need to add implicit relations to the paths

    private List<List<Concept>> getExtendedPaths(List<List<Concept>> allPaths) {
        List<List<Concept>> extendedPaths = new ArrayList<List<Concept>>();
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
            List<Concept> extendedPath = new ArrayList<Concept>();
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

    private Multimap<Concept, Concept> getPredecessorMap(ComputerResult result) {
        Map<String, Set<String>> predecessorMapFromSource =
                result.memory().get(ShortestPathVertexProgram.PREDECESSORS_FROM_SOURCE);
        Map<String, Set<String>> predecessorMapFromDestination =
                result.memory().get(ShortestPathVertexProgram.PREDECESSORS_FROM_DESTINATION);

        Multimap<Concept, Concept> predecessors = HashMultimap.create();
        predecessorMapFromSource.forEach((id, idSet) -> idSet.forEach(id2 -> {
            predecessors.put(getConcept(id), getConcept(id2));
        }));
        predecessorMapFromDestination.forEach((id, idSet) -> idSet.forEach(id2 -> {
            predecessors.put(getConcept(id2), getConcept(id));
        }));
        return predecessors;
    }

    private List<List<Concept>> getAllPaths(Multimap<Concept, Concept> predecessorMapFromSource, ConceptId sourceId) {
        List<List<Concept>> allPaths = new ArrayList<List<Concept>>();
        List<Concept> firstPath = new ArrayList<Concept>();
        firstPath.add(getConcept(sourceId.getValue()));

        Deque<List<Concept>> queue = new ArrayDeque<List<Concept>>();
        queue.addLast(firstPath);
        while (!queue.isEmpty()) {
            List<Concept> currentPath = queue.pollFirst();
            if (predecessorMapFromSource.containsKey(currentPath.get(currentPath.size() - 1))) {
                Collection<Concept> successors = predecessorMapFromSource.get(currentPath.get(currentPath.size() - 1));
                Iterator<Concept> iterator = successors.iterator();
                for (int i = 0; i < successors.size() - 1; i++) {
                    List<Concept> extendedPath = new ArrayList<Concept>(currentPath);
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
    private Thing getConcept(String conceptId) {
        return tx.getConcept(ConceptId.of(conceptId));
    }

    private <T, Q extends StatisticsQuery<?>> Optional<T> execWithMapReduce(
            Q query, MapReduceFactory<T> mapReduceFactory) {

        return runCompute(query, computer -> {
            AttributeType.DataType<?> dataType = getDataTypeOfSelectedResourceTypes(query);
            if (!selectedResourceTypesHaveInstance(query, statisticsResourceLabels(query))) {
                return Optional.empty();
            }
            Set<LabelId> allSubLabelIds = convertLabelsToIds(getCombinedSubTypes(query));
            Set<LabelId> statisticsResourceLabelIds = convertLabelsToIds(statisticsResourceLabels(query));

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

    private boolean isAttributeIncluded(ComputeQuery<?> query) {
        return query.isAttributeIncluded() || subTypesContainsImplicitOrAttributeTypes(query);
    }

    private boolean subTypesContainsImplicitOrAttributeTypes(ComputeQuery<?> query) {
        return query.subLabels().stream().anyMatch(label -> {
            SchemaConcept type = tx.getSchemaConcept(label);
            return (type != null && (type.isAttributeType() || type.isImplicit()));
        });
    }

    private interface ComputeRunner<T> {
        T apply(GraknComputer computer);
    }

    private interface MapReduceFactory<S> {
        GraknMapReduce<S> get(
                Set<LabelId> statisticsResourceLabelIds, AttributeType.DataType<?> dataType, String degreePropertyKey);
    }
}