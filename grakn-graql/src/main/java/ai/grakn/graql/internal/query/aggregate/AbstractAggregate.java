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

package ai.grakn.graql.internal.query.aggregate;

import ai.grakn.graql.Aggregate;
import ai.grakn.graql.NamedAggregate;

import java.util.Comparator;

/**
 * Abstract implementation of an {@link Aggregate}, providing an implementation of the {@link Aggregate#as(String)}}
 * method.
 *
 * @param <S> The result type of the aggregate.
 *
 * @author Felix Chapman
 */
public abstract class AbstractAggregate<S> implements Aggregate<S> {

    @Override
    public final NamedAggregate<S> as(String name) {
        return new NamedAggregateImpl<>(this, name);
    }

    /**
     * A Comparator class to compare the 2 numbers only if they have the same primitive type.
     */
    public static class NumberPrimitiveTypeComparator implements Comparator<Number> {

        @Override
        public int compare(Number a, Number b) {
            if (((Object) a).getClass().equals(((Object) b).getClass()) && a instanceof Comparable) {
                return ((Comparable) a).compareTo(b);
            }

            throw new RuntimeException("Invalid attempt to compare non-comparable primitive type of Numbers in Aggregate function");
        }
    }
}
