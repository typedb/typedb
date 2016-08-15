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

import io.mindmaps.constants.ErrorMessage;
import io.mindmaps.core.MindmapsTransaction;
import io.mindmaps.core.model.Concept;
import io.mindmaps.graql.MatchQueryMap;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * "Order" modify that orders the underlying match query
 */
public class MatchQueryOrder extends MatchQueryMapDefault {

    private final MatchOrder order;

    public MatchQueryOrder(MatchQueryMap.Admin inner, MatchOrder order) {
        super(inner);
        this.order = order;
    }

    @Override
    public Stream<Map<String, Concept>> stream(
            Optional<MindmapsTransaction> transaction, Optional<MatchOrder> order
    ) {
        if (order.isPresent()) {
            throw new IllegalStateException(ErrorMessage.MULTIPLE_ORDER.getMessage());
        }

        return inner.stream(transaction, Optional.of(this.order));
    }

    @Override
    protected Stream<Map<String, Concept>> transformStream(Stream<Map<String, Concept>> stream) {
        return stream;
    }

    @Override
    public String toString() {
        return inner.toString() + " " + order.toString();
    }
}
