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

package ai.grakn.graql.internal.query.predicate;

import ai.grakn.graql.internal.util.StringConverter;
import org.apache.tinkerpop.gremlin.process.traversal.P;

import java.util.Optional;

class EqPredicate extends ComparatorPredicate {

    private final Object value;

    /**
     * @param value the value that this predicate is testing against
     */
    EqPredicate(Object value) {
        super(value);
        this.value = value;
    }

    @Override
    protected String getSymbol() {
        return "=";
    }

    @Override
    <V> P<V> gremlinPredicate(V value) {
        return P.eq(value);
    }

    @Override
    public boolean isSpecific() {
        return true;
    }

    @Override
    public Optional<Object> equalsValue() {
        return Optional.of(value);
    }

    @Override
    public String toString() {
        return StringConverter.valueToString(value);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        return super.equals(o);
    }

    @Override
    public int hashCode() {
        return super.hashCode() + 37;
    }
}
