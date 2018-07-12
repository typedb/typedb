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

package ai.grakn.graql.internal.query.match;

import ai.grakn.graql.Match;
import ai.grakn.graql.Order;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Answer;
import com.google.auto.value.AutoValue;

import java.util.Comparator;
import java.util.stream.Stream;

/**
 * A class for handling ordering {@link Match}es.
 *
 * @author Felix Chapman
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
    Stream<Answer> orderStream(Stream<Answer> stream) {
        return stream.sorted(comparator());
    }

    private Comparator<Answer> comparator() {
        Comparator<Answer> comparator = Comparator.comparing(this::getOrderValue);
        return (order() == Order.desc) ? comparator.reversed() : comparator;
    }

    // All data types are comparable, so this is safe
    @SuppressWarnings("unchecked")
    private Comparable<? super Comparable> getOrderValue(Answer result) {
        return (Comparable<? super Comparable>) result.get(var()).asAttribute().value();
    }

    @Override
    public String toString() {
        return "order by " + var() + " ";
    }
}
