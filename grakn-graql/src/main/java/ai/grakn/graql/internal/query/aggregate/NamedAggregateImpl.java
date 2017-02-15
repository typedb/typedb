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

class NamedAggregateImpl<T, S> implements NamedAggregate<T, S> {
    private final Aggregate<T, S> aggregate;
    private final String name;

    NamedAggregateImpl(Aggregate<T, S> aggregate, String name) {
        this.aggregate = aggregate;
        this.name = name;
    }

    @Override
    public Aggregate<T, S> getAggregate() {
        return aggregate;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return getAggregate() + " as " + getName();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        NamedAggregateImpl<?, ?> that = (NamedAggregateImpl<?, ?>) o;

        if (!aggregate.equals(that.aggregate)) return false;
        return name.equals(that.name);
    }

    @Override
    public int hashCode() {
        int result = aggregate.hashCode();
        result = 31 * result + name.hashCode();
        return result;
    }
}
