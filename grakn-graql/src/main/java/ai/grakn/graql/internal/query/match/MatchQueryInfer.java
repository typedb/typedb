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

package ai.grakn.graql.internal.query.match;

import ai.grakn.GraknGraph;
import ai.grakn.concept.Concept;
import ai.grakn.graql.Reasoner;
import ai.grakn.util.ErrorMessage;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static ai.grakn.graql.internal.util.CommonUtil.optionalOr;

/**
 * Modifier that specifies the graph to execute the match query with.
 */
class MatchQueryInfer extends MatchQueryModifier {

    MatchQueryInfer(MatchQueryInternal inner) {
        super(inner);
    }

    @Override
    public Stream<Map<String, Concept>> stream(
            Optional<GraknGraph> optionalGraph, Optional<MatchOrder> order
    ) {
        GraknGraph graph = optionalOr(optionalGraph, inner.getGraph()).orElseThrow(
                () -> new IllegalStateException(ErrorMessage.NO_GRAPH.getMessage())
        );

        // TODO: Handle ordering
        return new Reasoner(graph).resolveToQuery(inner).stream();
    }

    @Override
    protected String modifierString() {
        throw new RuntimeException("modifierString should never be called");
    }

    @Override
    public String toString() {
        return inner.toString();
    }
}
