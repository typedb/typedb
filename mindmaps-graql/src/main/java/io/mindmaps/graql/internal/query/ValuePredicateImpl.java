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

package io.mindmaps.graql.internal.query;

import com.google.common.collect.Sets;
import io.mindmaps.graql.api.query.ValuePredicate;
import org.apache.tinkerpop.gremlin.process.traversal.P;

import java.util.*;

/**
 * Implementation of ValuePredicate
 */
public class ValuePredicateImpl implements ValuePredicate.Admin {

    private final P<Object> predicate;
    private final String repr;
    private final boolean equals;
    private final Set<Object> innerValues;

    /**
     * @param predicate the gremlin predicate to use
     * @param repr the string representation of this predicate in Graql syntax
     * @param innerValue the value that this predicate is testing against
     * @param equals true only if the predicate is an "equals" predicate
     */
    public ValuePredicateImpl(P<Object> predicate, String repr, Object innerValue, boolean equals) {
        this(predicate, repr, equals, Collections.singleton(innerValue));
    }

    private ValuePredicateImpl(P<Object> predicate, String repr, boolean equals, Collection<Object> innerValues) {
        this.predicate = predicate;
        this.repr = repr;
        this.equals = equals;
        this.innerValues = new HashSet<>(innerValues);
    }

    @Override
    public ValuePredicate and(ValuePredicate other) {
        P<Object> and = predicate.and(other.admin().getPredicate());
        Sets.SetView<Object> innerUnion = Sets.union(innerValues, other.admin().getInnerValues());
        return new ValuePredicateImpl(and, "(" + repr + " and " + other.admin().toString() + ")", false, innerUnion);
    }

    @Override
    public ValuePredicate or(ValuePredicate other) {
        P<Object> or = predicate.or(other.admin().getPredicate());
        Sets.SetView<Object> innerUnion = Sets.union(innerValues, other.admin().getInnerValues());
        return new ValuePredicateImpl(or, "(" + repr + " or " + other.admin().toString() + ")", false, innerUnion);
    }

    @Override
    public Admin admin() {
        return this;
    }

    @Override
    public boolean isSpecific() {
        return equals;
    }

    @Override
    public Optional<Object> equalsValue() {
        if (equals) {
            return Optional.of(predicate.getValue());
        } else {
            return Optional.empty();
        }
    }

    @Override
    public Set<Object> getInnerValues() {
        return Collections.unmodifiableSet(innerValues);
    }

    @Override
    public P<Object> getPredicate() {
        return predicate;
    }

    @Override
    public String toString() {
        return repr;
    }
}
