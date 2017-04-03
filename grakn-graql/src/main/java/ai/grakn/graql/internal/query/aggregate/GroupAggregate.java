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
import ai.grakn.graql.Aggregate;
import ai.grakn.graql.VarName;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.stream.Collector;
import java.util.stream.Stream;

import static ai.grakn.util.ErrorMessage.VARIABLE_NOT_IN_QUERY;
import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;

/**
 * Aggregate that groups results of a match query by variable name, applying an aggregate to each group.
 * @param <T> the type of each group
 */
class GroupAggregate<T> extends AbstractAggregate<Map<VarName, Concept>, Map<Concept, T>> {

    private final VarName varName;
    private final Aggregate<? super Map<VarName, Concept>, T> innerAggregate;

    GroupAggregate(VarName varName, Aggregate<? super Map<VarName, Concept>, T> innerAggregate) {
        this.varName = varName;
        this.innerAggregate = innerAggregate;
    }

    @Override
    public Map<Concept, T> apply(Stream<? extends Map<VarName, Concept>> stream) {
        Collector<Map<VarName, Concept>, ?, T> applyAggregate =
                collectingAndThen(toList(), list -> innerAggregate.apply(list.stream()));

        return stream.collect(groupingBy(this::getConcept, applyAggregate));
    }

    private @Nonnull Concept getConcept(Map<VarName, Concept> result) {
        Concept concept = result.get(varName);
        if (concept == null) {
            throw new IllegalArgumentException(VARIABLE_NOT_IN_QUERY.getMessage(varName));
        }
        return concept;
    }

    @Override
    public String toString() {
        if (innerAggregate instanceof ListAggregate) {
            return "group " + varName;
        } else {
            return "group " + varName + " " + innerAggregate.toString();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        GroupAggregate<?> that = (GroupAggregate<?>) o;

        if (!varName.equals(that.varName)) return false;
        return innerAggregate.equals(that.innerAggregate);
    }

    @Override
    public int hashCode() {
        int result = varName.hashCode();
        result = 31 * result + innerAggregate.hashCode();
        return result;
    }
}
