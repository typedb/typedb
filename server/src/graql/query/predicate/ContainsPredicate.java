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

import grakn.core.graql.ValuePredicate;
import grakn.core.graql.admin.VarPatternAdmin;
import org.apache.tinkerpop.gremlin.process.traversal.P;

class ContainsPredicate extends ComparatorPredicate {

    /**
     * @param substring the value that this predicate is testing against
     */
    ContainsPredicate(String substring) {
        super(substring);
    }

    /**
     * @param var the variable that this predicate is testing against
     */
    ContainsPredicate(VarPatternAdmin var) {
        super(var);
    }

    @Override
    protected String getSymbol() {
        return "contains";
    }

    @Override
    <V> P<V> gremlinPredicate(V value) {
        return new P<>((v, s) -> ((String) v).contains((String) s), value);
    }

    @Override
    public int signum() { return 0;}

    @Override
    public boolean isCompatibleWith(ValuePredicate predicate){
        if (predicate instanceof ContainsPredicate) return true;
        if (!(predicate instanceof EqPredicate)) return false;
        EqPredicate that = (EqPredicate) predicate;
        Object val = this.value().orElse(null);
        Object thatVal = that.value().orElse(null);
        return val == null || thatVal != null && this.gremlinPredicate(val).test(thatVal);
    }
}
