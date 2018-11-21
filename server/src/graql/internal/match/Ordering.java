/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package grakn.core.graql.internal.match;

import grakn.core.graql.Match;
import grakn.core.graql.Order;
import grakn.core.graql.Var;
import grakn.core.graql.answer.ConceptMap;
import com.google.auto.value.AutoValue;

import java.util.Comparator;
import java.util.stream.Stream;

/**
 * A class for handling ordering {@link Match}es.
 *
 */
@AutoValue
abstract class Ordering {

    abstract Var var();
    abstract Order order();

    static Ordering of(Var var, Order order) {
        return new AutoValue_Ordering(var, order);
    }

    /**
     * Order the stream
     * @param stream the stream to order
     */
    Stream<ConceptMap> orderStream(Stream<ConceptMap> stream) {
        return stream.sorted(comparator());
    }

    private Comparator<ConceptMap> comparator() {
        Comparator<ConceptMap> comparator = Comparator.comparing(this::getOrderValue);
        return (order() == Order.desc) ? comparator.reversed() : comparator;
    }

    // All data types are comparable, so this is safe
    @SuppressWarnings("unchecked")
    private Comparable<? super Comparable> getOrderValue(ConceptMap result) {
        return (Comparable<? super Comparable>) result.get(var()).asAttribute().value();
    }

    @Override
    public String toString() {
        return "order by " + var() + " ";
    }
}
