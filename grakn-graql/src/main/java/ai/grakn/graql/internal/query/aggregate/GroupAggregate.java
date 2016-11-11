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

import ai.grakn.graql.Aggregate;
import ai.grakn.concept.Concept;
import ai.grakn.graql.Aggregate;

import java.util.Map;
import java.util.stream.Collector;
import java.util.stream.Stream;

import static java.util.stream.Collectors.*;

/**
 * Aggregate that groups results of a match query by variable name, applying an aggregate to each group.
 * @param <T> the type of each group
 */
class GroupAggregate<T> extends AbstractAggregate<Map<String, Concept>, Map<Concept, T>> {

    private final String varName;
    private final Aggregate<? super Map<String, Concept>, T> innerAggregate;

    GroupAggregate(String varName, Aggregate<? super Map<String, Concept>, T> innerAggregate) {
        this.varName = varName;
        this.innerAggregate = innerAggregate;
    }

    @Override
    public Map<Concept, T> apply(Stream<? extends Map<String, Concept>> stream) {
        Collector<Map<String, Concept>, ?, T> applyAggregate =
                collectingAndThen(toList(), list -> innerAggregate.apply(list.stream()));

        return stream.collect(groupingBy(result -> result.get(varName), applyAggregate));
    }

    @Override
    public String toString() {
        if (innerAggregate instanceof ListAggregate) {
            return "group $" + varName;
        } else {
            return "group $" + varName + " " + innerAggregate.toString();
        }
    }
}
