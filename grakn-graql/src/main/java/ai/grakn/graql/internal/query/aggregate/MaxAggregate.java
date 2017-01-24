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

package ai.grakn.graql.internal.query.aggregate;

import ai.grakn.concept.Concept;
import ai.grakn.graql.VarName;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static java.util.Comparator.naturalOrder;

/**
 * Aggregate that finds maximum of a match query.
 */
class MaxAggregate<T extends Comparable<T>> extends AbstractAggregate<Map<VarName, Concept>, Optional<T>> {

    private final VarName varName;

    MaxAggregate(VarName varName) {
        this.varName = varName;
    }

    @Override
    public Optional<T> apply(Stream<? extends Map<VarName, Concept>> stream) {
        return stream.map(this::getValue).max(naturalOrder());
    }

    @Override
    public String toString() {
        return "max " + varName;
    }

    private T getValue(Map<VarName, Concept> result) {
        return result.get(varName).<T>asResource().getValue();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MaxAggregate<?> that = (MaxAggregate<?>) o;

        return varName.equals(that.varName);
    }

    @Override
    public int hashCode() {
        return varName.hashCode();
    }
}
