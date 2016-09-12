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

import io.mindmaps.MindmapsGraph;
import io.mindmaps.concept.Concept;
import io.mindmaps.graql.Aggregate;
import io.mindmaps.graql.AggregateQuery;
import io.mindmaps.graql.admin.MatchQueryAdmin;

import java.util.Map;
import java.util.stream.Stream;

/**
 * Implementation of AggregateQuery
 * @param <T> the type of the aggregate result
 */
class AggregateQueryImpl<T> implements AggregateQuery<T> {

    private final MatchQueryAdmin matchQuery;
    private final Aggregate<? super Map<String, Concept>, T> aggregate;

    AggregateQueryImpl(MatchQueryAdmin matchQuery, Aggregate<? super Map<String, Concept>, T> aggregate) {
        this.matchQuery = matchQuery;
        this.aggregate = aggregate;
    }

    @Override
    public AggregateQuery<T> withGraph(MindmapsGraph graph) {
        return new AggregateQueryImpl<>(matchQuery.withGraph(graph).admin(), aggregate);
    }

    @Override
    public T execute() {
        return aggregate.apply(matchQuery.stream());
    }

    @Override
    public Stream<String> resultsString() {
        return Stream.of(execute().toString());
    }

    @Override
    public boolean isReadOnly() {
        // An aggregate query may modify the graph if using a user-defined aggregate method
        return false;
    }

    @Override
    public String toString() {
        return matchQuery.toString() + " aggregate " + aggregate.toString();
    }
}
