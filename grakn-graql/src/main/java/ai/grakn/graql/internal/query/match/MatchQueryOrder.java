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
import ai.grakn.util.ErrorMessage;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * "Order" modify that orders the underlying match query
 */
class MatchQueryOrder extends MatchQueryModifier {

    private final MatchOrderImpl order;

    MatchQueryOrder(MatchQueryInternal inner, MatchOrderImpl order) {
        super(inner);
        this.order = order;
    }

    @Override
    public Stream<Map<String, Concept>> stream(
            Optional<GraknGraph> graph, Optional<MatchOrder> order
    ) {
        if (order.isPresent()) {
            throw new IllegalStateException(ErrorMessage.MULTIPLE_ORDER.getMessage());
        }

        return inner.stream(graph, Optional.of(this.order));
    }

    @Override
    protected String modifierString() {
        return order.toString();
    }
}
