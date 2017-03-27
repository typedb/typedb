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

import ai.grakn.graql.Order;
import ai.grakn.graql.VarName;

import ai.grakn.graql.admin.Answer;
import java.util.Comparator;
import java.util.stream.Stream;

class MatchOrderImpl implements MatchOrder {

    private final VarName var;

    private final Comparator<Answer> comparator;

    MatchOrderImpl(VarName var, Order order) {
        this.var = var;

        Comparator<Answer> comparator = Comparator.comparing(this::getOrderValue);

        this.comparator = (order == Order.desc) ? comparator.reversed() : comparator;
    }

    @Override
    public VarName getVar() {
        return var;
    }

    @Override
    public Stream<Answer> orderStream(Stream<Answer> stream) {
        return stream.sorted(comparator);
    }

    // All data types are comparable, so this is safe
    @SuppressWarnings("unchecked")
    private Comparable<? super Comparable> getOrderValue(Answer result) {
        return (Comparable<? super Comparable>) result.get(var).asResource().getValue();
    }

    @Override
    public String toString() {
        return "order by " + var + " ";
    }
}
