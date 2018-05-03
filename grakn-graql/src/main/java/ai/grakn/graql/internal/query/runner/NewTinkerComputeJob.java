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
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.concept.Thing;
import ai.grakn.concept.Type;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.ComputeAnswer;
import ai.grakn.graql.Graql;
import ai.grakn.graql.NewComputeQuery;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.internal.analytics.CountMapReduceWithAttribute;
import ai.grakn.graql.internal.analytics.CountVertexProgram;
import ai.grakn.graql.internal.analytics.DegreeStatisticsVertexProgram;
import ai.grakn.graql.internal.analytics.DegreeVertexProgram;
import ai.grakn.graql.internal.analytics.GraknMapReduce;
import ai.grakn.graql.internal.analytics.MaxMapReduce;
import ai.grakn.graql.internal.analytics.MeanMapReduce;
import ai.grakn.graql.internal.analytics.MedianVertexProgram;
import ai.grakn.graql.internal.analytics.MinMapReduce;
import ai.grakn.graql.internal.analytics.NoResultException;
import ai.grakn.graql.internal.analytics.ShortestPathVertexProgram;
import ai.grakn.graql.internal.analytics.StatisticsMapReduce;
import ai.grakn.graql.internal.analytics.StdMapReduce;
import ai.grakn.graql.internal.analytics.SumMapReduce;
import ai.grakn.graql.internal.analytics.Utility;
import ai.grakn.graql.internal.query.ComputeAnswerImpl;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import ai.grakn.util.CommonUtil;
import ai.grakn.util.Schema;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
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
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ai.grakn.util.GraqlSyntax.Compute.COUNT;
import static ai.grakn.util.GraqlSyntax.Compute.MAX;
import static ai.grakn.util.GraqlSyntax.Compute.MEAN;
import static ai.grakn.util.GraqlSyntax.Compute.MEDIAN;
import static ai.grakn.util.GraqlSyntax.Compute.MIN;
import static ai.grakn.util.GraqlSyntax.Compute.PATH;
import static ai.grakn.util.GraqlSyntax.Compute.STD;
import static ai.grakn.util.GraqlSyntax.Compute.SUM;

/**
 * A Graql Compute query job executed against a {@link GraknComputer}.
 *
 * @author Haikal Pribadi
 */
class NewTinkerComputeJob implements ComputeJob<ComputeAnswer> {

    private final NewComputeQuery query;

    private static final Logger LOG = LoggerFactory.getLogger(NewTinkerComputeJob.class);
    private final EmbeddedGraknTx<?> tx;

    public NewTinkerComputeJob(EmbeddedGraknTx<?> tx, NewComputeQuery query) {
        this.tx = tx;
        this.query = query;
    }

    @Override
    public void kill() { //todo: to be removed;
        tx.session().getGraphComputer().killJobs();
    }

    @Override
    public ComputeAnswer get() {
        switch (query.method()) {
            case MIN: case MAX: case MEDIAN: case SUM: return runComputeMinMaxMedianOrSum();
            case MEAN: return runComputeMean();
            case STD: return runComputeStd();
            case COUNT: return runComputeCount();
            case PATH: return runComputePath();
        }

        throw GraqlQueryException.invalidComputeMethod();
    }

    public final ComputerResult compute(@Nullable VertexProgram<?> program,
                                        @Nullable MapReduce<?, ?, ?, ?, ?> mapReduce,
                                        @Nullable Set<LabelId> scope,
                                        Boolean includesRolePlayerEdges) {

        return tx.session().getGraphComputer().compute(program, mapReduce, scope, includesRolePlayerEdges);
    }

    public final ComputerResult compute(@Nullable VertexProgram<?> program,
                                        @Nullable MapReduce<?, ?, ?, ?, ?> mapReduce,
                                        @Nullable Set<LabelId> scope) {

        return tx.session().getGraphComputer().compute(program, mapReduce, scope);
    }

    /**
     * The Graql compute min, max, median, or sum query run method
     *
     * @return a ComputeAnswer object containing a Number that represents the answer
     */
    private ComputeAnswer runComputeMinMaxMedianOrSum() {
        return new ComputeAnswerImpl().setNumber(runComputeStatistics());
    }

    /**
     * The Graql compute mean query run method
     * @return a ComputeAnswer object containing a Number that represents the answer
     */
    private ComputeAnswer runComputeMean() {
        ComputeAnswer answer = new ComputeAnswerImpl();

        Map<String, Double> meanPair = runComputeStatistics();
        if (meanPair == null) return answer;

        Double mean = meanPair.get(MeanMapReduce.SUM) / meanPair.get(MeanMapReduce.COUNT);

        return answer.setNumber(mean);
    }

    /**
     * The Graql compute std query run method
     *
     * @return a ComputeAnswer object containing a Number that represents the answer
     */
    private ComputeAnswer runComputeStd() {
        ComputeAnswer answer = new ComputeAnswerImpl();

        Map<String, Double> stdTuple = runComputeStatistics();
        if (stdTuple == null) return answer;

        double squareSum = stdTuple.get(StdMapReduce.SQUARE_SUM);
        double sum = stdTuple.get(StdMapReduce.SUM);
        double count = stdTuple.get(StdMapReduce.COUNT);
        Double std = Math.sqrt(squareSum / count - (sum / count) * (sum / count));

        return answer.setNumber(std);
    }

    /**
     * The compute statistics base algorithm that is called in every compute statistics query
     *
     * @param <T> The return type of {@link StatisticsMapReduce}
     * @return result of compute statistics algorithm, which will be of type T
     */
    @Nullable
    private <T> T runComputeStatistics() {
        AttributeType.DataType<?> targetDataType = validateAndGetTargetDataType();
        if (!targetContainsInstance()) return null;

        Set<LabelId> extendedScopeTypes = convertLabelsToIds(extendedScopeTypeLabels());
        Set<LabelId> targetTypes = convertLabelsToIds(targetTypeLabels());

        VertexProgram program = initStatisticsVertexProgram(query, targetTypes, targetDataType);
        StatisticsMapReduce<?> mapReduce = initStatisticsMapReduce(targetTypes, targetDataType);
        ComputerResult computerResult = compute(program, mapReduce, extendedScopeTypes);

        if (query.method().equals(MEDIAN)) {
            Number result = computerResult.memory().get(MedianVertexProgram.MEDIAN);
            LOG.debug("Median = " + result);
            return (T) result;
        }

        Map<Serializable, T> resultMap = computerResult.memory().get(mapReduce.getClass().getName());
        LOG.debug("Result = " + resultMap.get(MapReduce.NullObject.instance()));
        return resultMap.get(MapReduce.NullObject.instance());
    }

    /**
     * Helper method to validate that the target types are of one data type, and get that data type
     *
     * @return the DataType of the target types
     */
    @Nullable
    private AttributeType.DataType<?> validateAndGetTargetDataType() {
        AttributeType.DataType<?> dataType = null;
        for (Type type : targetTypes()) {
            // check if the selected type is a attribute type
            if (!type.isAttributeType()) throw GraqlQueryException.mustBeAttributeType(type.getLabel());
            AttributeType<?> attributeType = type.asAttributeType();
            if (dataType == null) {
                // check if the attribute type has data-type LONG or DOUBLE
                dataType = attributeType.getDataType();
                if (!dataType.equals(AttributeType.DataType.LONG) && !dataType.equals(AttributeType.DataType.DOUBLE)) {
                    throw GraqlQueryException.attributeMustBeANumber(dataType, attributeType.getLabel());
                }
            } else {
                // check if all the attribute types have the same data-type
                if (!dataType.equals(attributeType.getDataType())) {
                    throw GraqlQueryException.attributesWithDifferentDataTypes(query.of().get());
                }
            }
        }
        return dataType;
    }

    /**
     * Helper method to intialise the vertex program for compute statistics queries
     *
     * @param query representing the compute query
     * @param targetTypes representing the attribute types in which the statistics computation is targeted for
     * @param targetDataType representing the data type of the target attribute types
     * @return an object which is a subclass of VertexProgram
     */
    private VertexProgram initStatisticsVertexProgram(NewComputeQuery query, Set<LabelId> targetTypes, AttributeType.DataType<?> targetDataType) {
        if (query.method().equals(MEDIAN)) return new MedianVertexProgram(targetTypes, targetDataType);
        else return new DegreeStatisticsVertexProgram(targetTypes);
    }

    /**
     * Helper method to initialise the MapReduce algorithm for compute statistics queries
     *
     * @param targetTypes representing the attribute types in which the statistics computation is targeted for
     * @param targetDataType representing the data type of the target attribute types
     * @return an object which is a subclass of StatisticsMapReduce
     */
    private StatisticsMapReduce<?> initStatisticsMapReduce(Set<LabelId> targetTypes, AttributeType.DataType<?> targetDataType) {
        switch (query.method()) {
            case MIN:
                return new MinMapReduce(targetTypes, targetDataType, DegreeVertexProgram.DEGREE);
            case MAX:
                return new MaxMapReduce(targetTypes, targetDataType, DegreeVertexProgram.DEGREE);
            case MEAN:
                return new MeanMapReduce(targetTypes, targetDataType, DegreeVertexProgram.DEGREE);
            case STD:
                return new StdMapReduce(targetTypes, targetDataType, DegreeVertexProgram.DEGREE);
            case SUM:
                return new SumMapReduce(targetTypes, targetDataType, DegreeVertexProgram.DEGREE);
        }

        return null;
    }

    /**
     * Run Graql compute setNumber query
     *
     * @return a ComputeAnswer object containing the setNumber value
     */
    private ComputeAnswer runComputeCount() {
        ComputeAnswer answer = new ComputeAnswerImpl();

        if (!scopeContainsInstance()) {
            LOG.debug("Count = 0");
            return answer.setNumber(0L);
        }

        Set<LabelId> typeLabelIds = convertLabelsToIds(scopeTypeLabels());
        Map<Integer, Long> count;

        Set<LabelId> rolePlayerLabelIds = getRolePlayerLabelIds();
        rolePlayerLabelIds.addAll(typeLabelIds);

        ComputerResult result = compute(
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
        return answer.setNumber(finalCount);
    }

    /**
     * The Graql compute path query run method
     *
     * @return a ComputeAnswer containing the list of shortest paths
     */
    private ComputeAnswer runComputePath() {
        ComputeAnswer answer = new ComputeAnswerImpl();

        ConceptId fromID = query.from().get();
        ConceptId toID = query.to().get();

        if (!scopeContainsInstances(fromID, toID)) throw GraqlQueryException.instanceDoesNotExist();
        if (fromID.equals(toID)) return answer.paths(ImmutableList.of(ImmutableList.of(tx.getConcept(fromID))));

        ComputerResult result;
        Set<LabelId> scopedLabelIDs = convertLabelsToIds(scopeTypeLabels());
        try {
            result = compute(new ShortestPathVertexProgram(fromID, toID), null, scopedLabelIDs);
        } catch (NoResultException e) {
            return answer.paths(Collections.emptyList());
        }

        Multimap<Concept, Concept> resultGraph = getComputePathResultGraph(result);
        List<List<Concept>> allPaths = getComputePathResultList(resultGraph, fromID);
        if (scopeIncludesAttributes()) allPaths = getComputePathResultListIncludingImplicitRelations(allPaths); // SLOW

        LOG.info("Number of path: " + allPaths.size());

        return answer.paths(allPaths);
    }

    /**
     * Helper methed to get the graph of concepts that contains all the shortest path as a result of compute path query.
     *
     * @param result
     * @return a multimap of Concept to Concepts, representing a graph
     */
    private Multimap<Concept, Concept> getComputePathResultGraph(ComputerResult result) {
        // The result is contained in 2 halves:
        // The first half of the result are the half-way paths from the source concept
        // The second half o the result are the half-way paths from the destination concept
        Map<String, Set<String>> predecessorMapFromSource =
                result.memory().get(ShortestPathVertexProgram.PREDECESSORS_FROM_SOURCE);
        Map<String, Set<String>> predecessorMapFromDestination =
                result.memory().get(ShortestPathVertexProgram.PREDECESSORS_FROM_DESTINATION);

        // Stitching the two halves provides us the full graph that contains all the shortest path
        Multimap<Concept, Concept> resultGraph = HashMultimap.create();
        predecessorMapFromSource.forEach((id, idSet) -> idSet.forEach(id2 -> {
            resultGraph.put(getConcept(id), getConcept(id2));
        }));
        predecessorMapFromDestination.forEach((id, idSet) -> idSet.forEach(id2 -> {
            resultGraph.put(getConcept(id2), getConcept(id));
        }));
        return resultGraph;
    }

    /**
     * Helper method to get list of all shortest paths
     *
     * @param resultGraph
     * @param fromID
     * @return
     */
    private List<List<Concept>> getComputePathResultList(Multimap<Concept, Concept> resultGraph, ConceptId fromID) {
        List<List<Concept>> allPaths = new ArrayList<>();
        List<Concept> firstPath = new ArrayList<>();
        firstPath.add(getConcept(fromID.getValue()));

        Deque<List<Concept>> queue = new ArrayDeque<>();
        queue.addLast(firstPath);
        while (!queue.isEmpty()) {
            List<Concept> currentPath = queue.pollFirst();
            if (resultGraph.containsKey(currentPath.get(currentPath.size() - 1))) {
                Collection<Concept> successors = resultGraph.get(currentPath.get(currentPath.size() - 1));
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

    /**
     * Helper method t get the list of all shortest path, but also including the implicit relations that connect
     * entities and attributes
     *
     * @param allPaths
     * @return
     */
    private List<List<Concept>> getComputePathResultListIncludingImplicitRelations(List<List<Concept>> allPaths) {
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

    /**
     * Helper method to get Concept from the database
     *
     * @param conceptId
     * @return the concept instance (of class Thing)
     */
    @Nullable
    private Thing getConcept(String conceptId) {
        return tx.getConcept(ConceptId.of(conceptId));
    }

    /**
     * Helper method to get the label IDs of role players in a relationship
     *
     * @return a set of type label IDs
     */
    private Set<LabelId> getRolePlayerLabelIds() {
        return scopeTypes()
                .filter(Concept::isRelationshipType)
                .map(Concept::asRelationshipType)
                .filter(RelationshipType::isImplicit)
                .flatMap(RelationshipType::relates)
                .flatMap(Role::playedByTypes)
                .map(type -> tx.convertToId(type.getLabel()))
                .filter(LabelId::isValid)
                .collect(Collectors.toSet());
    }

    /**
     * Helper method to get implicit relationship types of attributes
     *
     * @param types
     * @return a set of type Labels
     */
    private static Set<Label> getAttributeImplicitRelationTypeLabes(Set<Type> types) {
        // If the sub graph contains attributes, we may need to add implicit relations to the path
        return types.stream()
                .filter(Concept::isAttributeType)
                .map(attributeType -> Schema.ImplicitType.HAS.getLabel(attributeType.getLabel()))
                .collect(Collectors.toSet());
    }

    /**
     * Helper method to get the types to be included in the query target
     *
     * @return a set of Types
     */
    private ImmutableSet<Type> targetTypes() {
        if (!query.of().isPresent() || query.of().get().isEmpty()) {
            throw GraqlQueryException.statisticsAttributeTypesNotSpecified();
        }

        return query.of().get().stream()
                .map((label) -> {
                    Type type = tx.getSchemaConcept(label);
                    if (type == null) throw GraqlQueryException.labelNotFound(label);
                    if (!type.isAttributeType()) throw GraqlQueryException.mustBeAttributeType(type.getLabel());
                    return type;
                })
                .flatMap(Type::subs)
                .collect(CommonUtil.toImmutableSet());
    }

    /**
     * Helper method to get the labels of the type in the query target
     *
     * @return a set of type Labels
     */
    private Set<Label> targetTypeLabels() {
        return targetTypes().stream()
                .map(SchemaConcept::getLabel)
                .collect(CommonUtil.toImmutableSet());
    }

    /**
     * Helper method to check whether the concept types in the target have any instances
     *
     * @return true if they exist, false if they don't
     */
    private boolean targetContainsInstance() {
        for (Label attributeType : targetTypeLabels()) {
            for (Label type : scopeTypeLabels()) {
                Boolean patternExist = tx.graql().infer(false).match(
                        Graql.var("x").has(attributeType, Graql.var()),
                        Graql.var("x").isa(Graql.label(type))
                ).iterator().hasNext();
                if (patternExist) return true;
            }
        }
        return false;
        //TODO: should use the following ask query when ask query is even lazier
//        List<Pattern> checkResourceTypes = statisticsResourceTypes.stream()
//                .map(type -> var("x").has(type, var())).collect(Collectors.toList());
//        List<Pattern> checkSubtypes = inTypes.stream()
//                .map(type -> var("x").isa(Graql.label(type))).collect(Collectors.toList());
//
//        return tx.get().graql().infer(false)
//                .match(or(checkResourceTypes), or(checkSubtypes)).aggregate(ask()).execute();
    }

    /**
     * Helper method to get all the concept types that should scope of compute query, which includes the implicit types
     * between attributes and entities, if target types were provided. This is used for compute statistics queries.
     *
     * @return a set of type labels
     */
    private Set<Label> extendedScopeTypeLabels() {
        Set<Label> extendedTypeLabels = getAttributeImplicitRelationTypeLabes(targetTypes());
        extendedTypeLabels.addAll(scopeTypeLabels());
        extendedTypeLabels.addAll(query.of().get());
        return extendedTypeLabels;
    }

    /**
     * Helper method to get the types to be included in the query scope
     *
     * @return stream of Concept Types
     */
    private Stream<Type> scopeTypes() {
        // Get all types if query.inTypes() is empty, else get all scoped types of each meta type.
        // Only include attributes and implicit "has-xxx" relationships when user specifically asked for them.
        if (!query.in().isPresent() || query.in().get().isEmpty()) {
            ImmutableSet.Builder<Type> typeBuilder = ImmutableSet.builder();

            if (scopeIncludesAttributes()) {
                tx.admin().getMetaConcept().subs().forEach(typeBuilder::add);
            } else {
                tx.admin().getMetaEntityType().subs().forEach(typeBuilder::add);
                tx.admin().getMetaRelationType().subs()
                        .filter(relationshipType -> !relationshipType.isImplicit()).forEach(typeBuilder::add);
            }

            return typeBuilder.build().stream();
        } else {
            Stream<Type> subTypes = query.in().get().stream().map(label -> {
                Type type = tx.getType(label);
                if (type == null) throw GraqlQueryException.labelNotFound(label);
                return type;
            }).flatMap(Type::subs);

            if (!scopeIncludesAttributes()) {
                subTypes = subTypes.filter(relationshipType -> !relationshipType.isImplicit());
            }

            return subTypes;
        }
    }

    /**
     * Helper method to get the labels of the type in the query scope
     *
     * @return a set of Concept Type Labels
     */
    private ImmutableSet<Label> scopeTypeLabels() {
        return scopeTypes().map(SchemaConcept::getLabel).collect(CommonUtil.toImmutableSet());
    }


    /**
     * Helper method to check whether the concept types in the scope have any instances
     *
     * @return
     */
    private boolean scopeContainsInstance() {
        if (scopeTypeLabels().isEmpty()) return false;
        List<Pattern> checkSubtypes = scopeTypeLabels().stream()
                .map(type -> Graql.var("x").isa(Graql.label(type))).collect(Collectors.toList());

        return tx.graql().infer(false).match(Graql.or(checkSubtypes)).iterator().hasNext();
    }

    /**
     * Helper method to check if concept instances exist in the query scope
     *
     * @param ids
     * @return true if they exist, false if they don't
     */
    private boolean scopeContainsInstances(ConceptId... ids) {
        for (ConceptId id : ids) {
            Thing thing = tx.getConcept(id);
            if (thing == null || !scopeTypeLabels().contains(thing.type().getLabel())) return false;
        }
        return true;
    }

    /**
     * Helper method to check whether attribute types should be included in the query scope
     *
     * @return true if they exist, false if they don't
     */
    private final boolean scopeIncludesAttributes() {
        return query.includesAttributes() || scopeIncludesImplicitOrAttributeTypes();
    }

    /**
     * Helper method to check whether implicit or attribute types are included in the query scope
     *
     * @return true if they exist, false if they don't
     */
    private boolean scopeIncludesImplicitOrAttributeTypes() {
        if (!query.in().isPresent()) return false;
        return query.in().get().stream().anyMatch(label -> {
            SchemaConcept type = tx.getSchemaConcept(label);
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
                .map(tx::convertToId)
                .filter(LabelId::isValid)
                .collect(Collectors.toSet());
    }

}
