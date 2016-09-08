/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.graql.internal.query.predicate;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.mindmaps.graql.ValuePredicate;
import io.mindmaps.graql.admin.ValuePredicateAdmin;

import java.util.Collections;
import java.util.Set;

/**
 * Implementation of ValuePredicate
 */
abstract class AbstractValuePredicate implements ValuePredicateAdmin {

    private final ImmutableSet<Object> innerValues;

    /**
     * @param innerValues the values that this predicate is testing against
     */
    AbstractValuePredicate(ImmutableSet<Object> innerValues) {
        this.innerValues = innerValues;
    }

    @Override
    public ValuePredicate and(ValuePredicate other) {
        ImmutableSet<Object> innerUnion = ImmutableSet.copyOf(Sets.union(innerValues, other.admin().getInnerValues()));
        return new AndPredicate(this, other.admin(), innerUnion);
    }

    @Override
    public ValuePredicate or(ValuePredicate other) {
        ImmutableSet<Object> innerUnion = ImmutableSet.copyOf(Sets.union(innerValues, other.admin().getInnerValues()));
        return new OrPredicate(this, other.admin(), innerUnion);
    }

    @Override
    public Set<Object> getInnerValues() {
        return Collections.unmodifiableSet(innerValues);
    }
}
