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

package ai.grakn.graql.internal.query.analytics;

import ai.grakn.GraknTx;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Label;
import ai.grakn.concept.LabelId;
import ai.grakn.concept.Thing;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.analytics.ClusterQuery;
import ai.grakn.graql.analytics.PathQuery;
import ai.grakn.graql.internal.analytics.ClusterMemberMapReduce;
import ai.grakn.graql.internal.analytics.NoResultException;
import ai.grakn.graql.internal.analytics.ShortestPathVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static ai.grakn.graql.internal.analytics.Utility.getResourceEdgeId;
import static ai.grakn.graql.internal.util.StringConverter.idToString;

class PathQueryImpl extends AbstractComputeQuery<Optional<List<Concept>>> implements PathQuery {

    private ConceptId sourceId = null;
    private ConceptId destinationId = null;

    PathQueryImpl(Optional<GraknTx> graph) {
        this.tx = graph;
    }

    @Override
    public Optional<List<Concept>> execute() {
        LOGGER.info("ShortestPathVertexProgram is called");
        long startTime = System.currentTimeMillis();

        if (sourceId == null) throw GraqlQueryException.noPathSource();
        if (destinationId == null) throw GraqlQueryException.noPathDestination();
        initSubGraph();
        getAllSubTypes();

        if (!verticesExistInSubgraph(sourceId, destinationId)) {
            throw GraqlQueryException.instanceDoesNotExist();
        }
        if (sourceId.equals(destinationId)) {
            return Optional.of(Collections.singletonList(tx.get().getConcept(sourceId)));
        }
        ComputerResult result;

        Set<LabelId> subLabelIds = convertLabelsToIds(subLabels);

        try {
            result = getGraphComputer().compute(
                    new ShortestPathVertexProgram(sourceId, destinationId),
                    new ClusterMemberMapReduce(ShortestPathVertexProgram.FOUND_IN_ITERATION),
                    subLabelIds);
        } catch (NoResultException e) {
            LOGGER.info("ShortestPathVertexProgram is done in " + (System.currentTimeMillis() - startTime) + " ms");
            return Optional.empty();
        }
        Map<Integer, Set<String>> map = result.memory().get(ClusterMemberMapReduce.class.getName());
        String middlePoint = result.memory().get(ShortestPathVertexProgram.MIDDLE);
        if (!middlePoint.equals("")) map.put(0, Collections.singleton(middlePoint));

        List<ConceptId> path = new ArrayList<>();
        path.add(sourceId);
        path.addAll(map.entrySet().stream()
                .sorted(Comparator.comparingInt(Map.Entry::getKey))
                .map(pair -> ConceptId.of(pair.getValue().iterator().next()))
                .collect(Collectors.toList()));
        path.add(destinationId);

        List<ConceptId> fullPath = new ArrayList<>();
        for (int index = 0; index < path.size() - 1; index++) {
            fullPath.add(path.get(index));
            if (includeAttribute) {
                ConceptId resourceRelationId = getResourceEdgeId(tx.get(), path.get(index), path.get(index + 1));
                if (resourceRelationId != null) {
                    fullPath.add(resourceRelationId);
                }
            }
        }
        fullPath.add(destinationId);

        LOGGER.debug("The path found is: " + fullPath);
        LOGGER.info("ShortestPathVertexProgram is done in " + (System.currentTimeMillis() - startTime) + " ms");
        return Optional.of(fullPath.stream().map(tx.get()::<Thing>getConcept).collect(Collectors.toList()));
    }

    @Override
    public PathQuery from(ConceptId sourceId) {
        this.sourceId = sourceId;
        return this;
    }

    @Override
    public PathQuery to(ConceptId destinationId) {
        this.destinationId = destinationId;
        return this;
    }

    @Override
    public PathQuery includeAttribute() {
        return (PathQuery) super.includeAttribute();
    }

    @Override
    public boolean isReadOnly() {
        return true;
    }

    @Override
    public PathQuery in(String... subTypeLabels) {
        return (PathQuery) super.in(subTypeLabels);
    }

    @Override
    public PathQuery in(Collection<Label> subLabels) {
        return (PathQuery) super.in(subLabels);
    }

    @Override
    String graqlString() {
        return "path from " + idToString(sourceId) + " to " + idToString(destinationId) + subtypeString();
    }

    @Override
    public PathQuery withTx(GraknTx tx) {
        return (PathQuery) super.withTx(tx);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        PathQueryImpl pathQuery = (PathQueryImpl) o;

        if (!sourceId.equals(pathQuery.sourceId)) return false;
        return destinationId.equals(pathQuery.destinationId);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + sourceId.hashCode();
        result = 31 * result + destinationId.hashCode();
        return result;
    }
}
