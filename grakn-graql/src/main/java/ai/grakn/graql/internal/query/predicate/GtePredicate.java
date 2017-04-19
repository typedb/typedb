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

import javax.lang.model.type.PrimitiveType;
import org.apache.tinkerpop.gremlin.process.traversal.P;

class GtePredicate extends ComparatorPredicate {

    /**
     * @param value the value that this predicate is testing against
     */
    GtePredicate(Object value) {
        super(value);
    }

    @Override
    protected String getSymbol() {
        return ">=";
    }

    @Override
    <V> P<V> gremlinPredicate(V value) {
        return P.gte(value);
    }

    @Override
    protected boolean isCompatibleWithEqPredicate(EqPredicate p) {
        Object val = p.getValue().orElse(null);
        Object thisVal = this.getValue().orElse(null);
        //if no value present then a variable hence compatible
        if (val == null || thisVal == null) return true;
        if (val.getClass() != thisVal.getClass()) return false;

        float v2 = (float) val;
        float v = (float) thisVal;
        return v >= v2;
    }
}
