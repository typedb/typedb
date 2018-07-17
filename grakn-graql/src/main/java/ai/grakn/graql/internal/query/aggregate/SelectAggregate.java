/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.graql.internal.query.aggregate;

import ai.grakn.graql.NamedAggregate;
import ai.grakn.graql.admin.Answer;
import com.google.common.collect.ImmutableSet;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

/**
 * An aggregate that combines several aggregates together into a map (where keys are the names of the aggregates)
 * @param <T> the type of the aggregate results
 */
class SelectAggregate<T> extends AbstractAggregate<Map<String, T>> {

    private final ImmutableSet<NamedAggregate<? extends T>> aggregates;

    SelectAggregate(ImmutableSet<NamedAggregate<? extends T>> aggregates) {
        this.aggregates = aggregates;
    }

    @Override
    public Map<String, T> apply(Stream<? extends Answer> stream) {
        List<? extends Answer> list = stream.collect(toList());

        Map<String, T> map = new HashMap<>();

        for (NamedAggregate<? extends T> aggregate : aggregates) {
            map.put(aggregate.getName(), aggregate.getAggregate().apply(list.stream()));
        }

        return map;
    }

    @Override
    public String toString() {
        return "(" + aggregates.stream().map(Object::toString).collect(Collectors.joining(", ")) + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SelectAggregate<?> that = (SelectAggregate<?>) o;

        return aggregates.equals(that.aggregates);
    }

    @Override
    public int hashCode() {
        return aggregates.hashCode();
    }
}
