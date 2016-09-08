/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 *
 */

package io.mindmaps.graql.internal.query.match;

import io.mindmaps.MindmapsGraph;
import io.mindmaps.concept.Concept;
import io.mindmaps.concept.Type;
import io.mindmaps.util.ErrorMessage;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Modifier that specifies the graph to execute the match query with.
 */
class MatchQueryGraph extends MatchQueryModifier {

    private final MindmapsGraph graph;

    MatchQueryGraph(MindmapsGraph graph, MatchQueryInternal inner) {
        super(inner);
        this.graph = graph;
    }

    @Override
    public Stream<Map<String, Concept>> stream(
            Optional<MindmapsGraph> graph, Optional<MatchOrder> order
    ) {
        if (graph.isPresent()) {
            throw new IllegalStateException(ErrorMessage.MULTIPLE_GRAPH.getMessage());
        }

        return inner.stream(Optional.of(this.graph), order);
    }

    @Override
    public Optional<MindmapsGraph> getGraph() {
        return Optional.of(graph);
    }

    @Override
    public Set<Type> getTypes() {
        return inner.getTypes(graph);
    }

    @Override
    public String toString() {
        return inner.toString();
    }
}
