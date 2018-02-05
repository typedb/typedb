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

package ai.grakn.graql.internal.query.analytics;

import ai.grakn.GraknComputer;
import ai.grakn.GraknTx;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.LabelId;
import ai.grakn.concept.Thing;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.analytics.PathsQuery;
import ai.grakn.graql.internal.analytics.NoResultException;
import ai.grakn.graql.internal.analytics.ShortestPathVertexProgram;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;

import javax.annotation.Nullable;
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

import static ai.grakn.graql.internal.analytics.Utility.getResourceEdgeId;
import static ai.grakn.graql.internal.util.StringConverter.nullableIdToString;

class PathsQueryImpl extends AbstractComputeQuery<List<List<Concept>>, PathsQuery> implements PathsQuery {

    private ConceptId sourceId = null;
    private ConceptId destinationId = null;

    PathsQueryImpl(Optional<GraknTx> tx) {
        super(tx);
    }

    @Override
    protected final List<List<Concept>> innerExecute(GraknTx tx, GraknComputer computer) {
        if (sourceId == null) throw GraqlQueryException.noPathSource();
        if (destinationId == null) throw GraqlQueryException.noPathDestination();

        if (!verticesExistInSubgraph(tx, sourceId, destinationId)) {
            throw GraqlQueryException.instanceDoesNotExist();
        }
        if (sourceId.equals(destinationId)) {
            return Collections.singletonList(Collections.singletonList(tx.<Thing>getConcept(sourceId)));
        }

        ComputerResult result;
        Set<LabelId> subLabelIds = convertLabelsToIds(tx, subLabels(tx));
        try {
            result = computer.compute(
                    new ShortestPathVertexProgram(sourceId, destinationId), null, subLabelIds);
        } catch (NoResultException e) {
            return Collections.emptyList();
        }

        Multimap<Concept, Concept> predecessorMapFromSource = getPredecessorMap(tx, result);
        List<List<Concept>> allPaths = getAllPaths(tx, predecessorMapFromSource);
        if (getIncludeAttribute()) { // this can be slow
            return getExtendedPaths(tx, allPaths);
        }

        LOGGER.info("Number of paths: " + allPaths.size());
        return allPaths;
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
                ConceptId resourceRelationId = getResourceEdgeId(tx,
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

    private Multimap<Concept, Concept> getPredecessorMap(GraknTx tx, ComputerResult result) {
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

    private List<List<Concept>> getAllPaths(GraknTx tx, Multimap<Concept, Concept> predecessorMapFromSource) {
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
    private Thing getConcept(GraknTx tx, String conceptId) {
        return tx.getConcept(ConceptId.of(conceptId));
    }

    @Override
    public PathsQuery from(ConceptId sourceId) {
        this.sourceId = sourceId;
        return this;
    }

    @Override
    public PathsQuery to(ConceptId destinationId) {
        this.destinationId = destinationId;
        return this;
    }

    @Override
    String graqlString() {
        return "paths from " + nullableIdToString(sourceId) + " to " + nullableIdToString(destinationId)
                + subtypeString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        PathsQueryImpl pathQuery = (PathsQueryImpl) o;

        return sourceId.equals(pathQuery.sourceId) && destinationId.equals(pathQuery.destinationId);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + sourceId.hashCode();
        result = 31 * result + destinationId.hashCode();
        return result;
    }
}
