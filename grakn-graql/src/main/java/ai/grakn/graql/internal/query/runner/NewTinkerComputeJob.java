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
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Label;
import ai.grakn.concept.LabelId;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.concept.Thing;
import ai.grakn.concept.Type;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.ComputeAnswer;
import ai.grakn.graql.NewComputeQuery;
import ai.grakn.graql.internal.analytics.NoResultException;
import ai.grakn.graql.internal.analytics.ShortestPathVertexProgram;
import ai.grakn.graql.internal.analytics.Utility;
import ai.grakn.graql.internal.query.ComputeAnswerImpl;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import ai.grakn.util.CommonUtil;
import ai.grakn.util.GraqlSyntax;
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
        if (query.method().equals(GraqlSyntax.Compute.PATH)) return runComputePath();

        throw GraqlQueryException.invalidComputeMethod();
    }

    public final ComputerResult compute(@Nullable VertexProgram<?> program,
                                        @Nullable MapReduce<?, ?, ?, ?, ?> mapReduce,
                                        @Nullable Set<LabelId> types,
                                        Boolean includesRolePlayerEdges
    ) {
        return tx.session().getGraphComputer().compute(program, mapReduce, types, includesRolePlayerEdges);
    }

    public final ComputerResult compute(@Nullable VertexProgram<?> program,
                                        @Nullable MapReduce<?, ?, ?, ?, ?> mapReduce,
                                        @Nullable Set<LabelId> types) {

        return tx.session().getGraphComputer().compute(program, mapReduce, types);
    }

    /**
     * Run graql compute path query
     * @return a ComputeAnswer object
     */
    private ComputeAnswer runComputePath() {
        ComputeAnswer answer = new ComputeAnswerImpl();

        ConceptId fromID = query.from().get();
        ConceptId toID = query.to().get();

        if (!conceptsExistAndWithinScope(fromID, toID)) throw GraqlQueryException.instanceDoesNotExist();
        if (fromID.equals(toID)) return answer.paths(ImmutableList.of(ImmutableList.of(tx.getConcept(fromID))));

        ComputerResult result;
        Set<LabelId> scopedLabelIDs = convertLabelsToIds(scopedTypeLabels());
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
     * Helper methed to get predecessor map from the result of shortest path computation
     *
     * @param result
     * @return a multmap of Concept to Concepts
     */
    private Multimap<Concept, Concept> getComputePathResultGraph(ComputerResult result) {
        Map<String, Set<String>> predecessorMapFromSource =
                result.memory().get(ShortestPathVertexProgram.PREDECESSORS_FROM_SOURCE);
        Map<String, Set<String>> predecessorMapFromDestination =
                result.memory().get(ShortestPathVertexProgram.PREDECESSORS_FROM_DESTINATION);

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
    final List<List<Concept>> getComputePathResultList(Multimap<Concept, Concept> resultGraph, ConceptId fromID) {
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
     * @param allPaths
     * @return
     */
    final List<List<Concept>> getComputePathResultListIncludingImplicitRelations(List<List<Concept>> allPaths) {
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
     * Helper method to get the types to be included in the scope of computation
     *
     * @return stream of Concept Types
     */
    private Stream<Type> scopedTypes() {
        // get all types if query.inTypes() is empty, else get all scoped types of each meta type in subGraph
        // only include attributes and implicit "has-xxx" relationships when user specifically asked for them
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
     * Helper method to get the inType labels
     *
     * @return a set of Concept Type Labels
     */
    private ImmutableSet<Label> scopedTypeLabels() {
        return scopedTypes().map(SchemaConcept::getLabel).collect(CommonUtil.toImmutableSet());
    }

    /**
     * Helper method to check whether attribute types should be included in the scope of computation
     *
     * @return true if they exist, false if they don't
     */
    private final boolean scopeIncludesAttributes() {
        return query.includesAttributes() || scopeIncludesImplicitOrAttributeTypes();
    }

    /**
     * Helper method to check whether implicit or attribute types are included in the scope of computation
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
     * Helper method to check if Concept instances exist in the scope of computation
     *
     * @param ids
     * @return true if they exist, false if they don't
     */
    private boolean conceptsExistAndWithinScope(ConceptId... ids) {
        for (ConceptId id : ids) {
            Thing thing = tx.getConcept(id);
            if (thing == null || !scopedTypeLabels().contains(thing.type().getLabel())) return false;
        }
        return true;
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
