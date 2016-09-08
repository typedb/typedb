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
import io.mindmaps.graql.internal.util.StringConverter;
import org.apache.tinkerpop.gremlin.process.traversal.P;

import java.util.Optional;

class EqPredicate extends AbstractValuePredicate {

    private Object value;

    /**
     * @param value the value that this predicate is testing against
     */
    EqPredicate(Object value) {
        super(ImmutableSet.of(value));
        this.value = value;
    }

    @Override
    public P<Object> getPredicate() {
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
}
