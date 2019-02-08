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

package grakn.core.graql.query.predicate;

import org.apache.tinkerpop.gremlin.process.traversal.P;

public class NeqPredicate extends ComparatorPredicate {

    /**
     * @param value the value that this predicate is testing against
     */
    NeqPredicate(Object value) { super(value);}

    @Override
    protected String getSymbol() {
        return "!==";
    }

    @Override
    <V> P<V> gremlinPredicate(V value) {
        return P.neq(value);
    }

    @Override
    public int signum() { return 0; }

    @Override
    public boolean isCompatibleWith(ValuePredicate predicate){
        ComparatorPredicate that = (ComparatorPredicate) predicate;
        Object val = this.value().orElse(null);
        Object thatVal = that.value().orElse(null);
        if (val == null || thatVal == null) return true;
        //checks for !=/= contradiction
        return !val.equals(thatVal)
                || (val.equals(thatVal) && !(that instanceof EqPredicate));
    }
}
