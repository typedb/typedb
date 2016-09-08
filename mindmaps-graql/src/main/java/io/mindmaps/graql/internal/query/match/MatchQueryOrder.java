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
import io.mindmaps.util.ErrorMessage;

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
            Optional<MindmapsGraph> graph, Optional<MatchOrder> order
    ) {
        if (order.isPresent()) {
            throw new IllegalStateException(ErrorMessage.MULTIPLE_ORDER.getMessage());
        }

        return inner.stream(graph, Optional.of(this.order));
    }

    @Override
    public String toString() {
        return inner.toString() + " " + order.toString();
    }
}
