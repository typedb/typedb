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

package grakn.core.graql.internal.query.predicate;

import grakn.core.util.StringUtil;
import org.apache.tinkerpop.gremlin.process.traversal.P;

import java.util.Optional;

class EqPredicate extends ComparatorPredicate {

    /**
     * @param value the value that this predicate is testing against
     */
    EqPredicate(Object value) {
        super(value);
    }

    @Override
    protected String getSymbol() {
        return "==";
    }

    @Override
    <V> P<V> gremlinPredicate(V value) {
        return P.eq(value);
    }

    @Override
    public boolean isSpecific() {
        return !getInnerVar().isPresent();
    }

    @Override
    public boolean containsEquality(){ return true;}

    @Override
    public int signum() { return 0; }

    @Override
    public String toString() {
        Optional<Object> value = value();
        if (value.isPresent()) {
            // Omit the `=` if we're using a literal value, not a var
            return StringUtil.valueToString(value.get());
        } else {
            return super.toString();
        }
    }

    @Override
    public Optional<Object> equalsValue() {
        return value();
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
