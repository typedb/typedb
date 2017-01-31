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

package ai.grakn.graql.internal.query.aggregate;

import ai.grakn.graql.Aggregate;
import ai.grakn.graql.NamedAggregate;

/**
 * Abstract implementation of an {@link Aggregate}, providing an implementation of the {@link Aggregate#as(String)}}
 * method.
 *
 * @param <T> The input type to the aggregate.
 * @param <S> The result type of the aggregate.
 *
 * @author Felix Chapman
 */
public abstract class AbstractAggregate<T, S> implements Aggregate<T, S> {

    @Override
    public final NamedAggregate<T, S> as(String name) {
        return new NamedAggregateImpl<>(this, name);
    }
}
