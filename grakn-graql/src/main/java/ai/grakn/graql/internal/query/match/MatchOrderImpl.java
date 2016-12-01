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

import ai.grakn.concept.Concept;
import ai.grakn.graql.Order;

import java.util.Comparator;
import java.util.Map;
import java.util.stream.Stream;

class MatchOrderImpl implements MatchOrder {

    private final String var;

    private final Comparator<Map<String, Concept>> comparator;

    MatchOrderImpl(String var, Order order) {
        this.var = var;

        Comparator<Map<String, Concept>> comparator = Comparator.comparing(this::getOrderValue);

        this.comparator = (order == Order.desc) ? comparator.reversed() : comparator;
    }

    @Override
    public String getVar() {
        return var;
    }

    @Override
    public Stream<Map<String, Concept>> orderStream(Stream<Map<String, Concept>> stream) {
        return stream.sorted(comparator);
    }

    private Comparable<? super Comparable> getOrderValue(Map<String, Concept> result) {
        //noinspection unchecked
        return (Comparable<? super Comparable>) result.get(var).asResource().getValue();
    }

    @Override
    public String toString() {
        return "order by $" + var + " ";
    }
}
