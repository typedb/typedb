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
 *
 */

package io.mindmaps.graql;

import io.mindmaps.core.model.Concept;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collector;
import java.util.stream.Stream;

import static java.util.stream.Collectors.*;

/**
 * An aggregate operation to perform on a query.
 * @param <T> the type of the query to perform the aggregate operation on
 * @param <S> the type of the result of the aggregate operation
 */
public interface Aggregate<T, S> {

    /**
     * The function to apply to the stream of results to produce the aggregate result.
     * @param stream a stream of query results
     * @return the result of the aggregate operation
     */
    S apply(Stream<? extends T> stream);

    /**
     * Return a {@link NamedAggregate}. This is used when operating on a query with multiple aggregates.
     * @param name the name of the aggregate
     * @return a new named aggregate
     */
    default NamedAggregate<T, S> as(String name) {
        return new NamedAggregate<>(this, name);
    }

    /**
     * Create an aggregate that will count the results of a query.
     */
    static Aggregate<Object, Long> count() {
        return Stream::count;
    }

    /**
     * Create an aggregate that will group a query by a variable name.
     * @param varName the variable name to group results by
     */
    static Aggregate<Map<String, Concept>, Map<Concept, List<Map<String, Concept>>>> group(String varName) {
        return stream -> stream.collect(groupingBy(result -> result.get(varName)));
    }

    /**
     * Create an aggregate that will group a query by a variable name and apply the given aggregate to each group
     * @param varName the variable name to group results by
     * @param aggregate the aggregate to apply to each group
     * @param <T> the type the aggregate returns
     */
    static <T> Aggregate<Map<String, Concept>, Map<Concept, T>> group(String varName, Aggregate<? super Map<String, Concept>, T> aggregate) {
        return stream -> {
            Collector<Map<String, Concept>, ?, T> applyAggregate =
                    collectingAndThen(toList(), x -> aggregate.apply(x.stream()));

            return stream.collect(groupingBy(result -> result.get(varName), applyAggregate));
        };
    }

    /**
     * Create an aggregate that will collect together several named aggregates into a map.
     * @param aggregates the aggregates to join together
     * @param <S> the type that the query returns
     * @param <T> the type that each aggregate returns
     */
    static <S, T> Aggregate<S, Map<String, T>> select(NamedAggregate<? super S, ? extends T>... aggregates) {
        return stream -> {

            List<? extends S> list = stream.collect(toList());

            Map<String, T> map = new HashMap<>();

            for (NamedAggregate<? super S, ? extends T> aggregate : aggregates) {
                map.put(aggregate.getName(), aggregate.getAggregate().apply(list.stream()));
            }

            return map;
        };
    }
}
