/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs
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

import ai.grakn.graql.admin.ValuePredicateAdmin;
import com.google.common.collect.ImmutableSet;
import ai.grakn.graql.admin.ValuePredicateAdmin;
import org.apache.tinkerpop.gremlin.process.traversal.P;

class OrPredicate extends AbstractValuePredicate {

    private final ValuePredicateAdmin predicate1;
    private final ValuePredicateAdmin predicate2;

    OrPredicate(ValuePredicateAdmin predicate1, ValuePredicateAdmin predicate2, ImmutableSet<Object> innerValues) {
        super(innerValues);
        this.predicate1 = predicate1;
        this.predicate2 = predicate2;
    }

    @Override
    public P<Object> getPredicate() {
        return predicate1.getPredicate().or(predicate2.getPredicate());
    }

    @Override
    public String toString() {
        return "(" + predicate1 + " or " + predicate2 + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        OrPredicate that = (OrPredicate) o;

        if (predicate1 != null ? !predicate1.equals(that.predicate1) : that.predicate1 != null) return false;
        return predicate2 != null ? predicate2.equals(that.predicate2) : that.predicate2 == null;

    }

    @Override
    public int hashCode() {
        int result = predicate1 != null ? predicate1.hashCode() : 0;
        result = 31 * result + (predicate2 != null ? predicate2.hashCode() : 0);
        return result;
    }
}
