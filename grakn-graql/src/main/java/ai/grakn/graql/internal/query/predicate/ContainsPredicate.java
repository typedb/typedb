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

import ai.grakn.graql.admin.VarAdmin;
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
    ContainsPredicate(VarAdmin var) {
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
}
