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

package ai.grakn.graql.internal.query;

import ai.grakn.GraknGraph;
import ai.grakn.concept.Concept;
import ai.grakn.graql.Aggregate;
import ai.grakn.graql.AggregateQuery;
import ai.grakn.graql.Printer;
import ai.grakn.graql.admin.MatchQueryAdmin;
import ai.grakn.graql.VarName;

import java.util.Map;
import java.util.stream.Stream;

/**
 * Implementation of AggregateQuery
 * @param <T> the type of the aggregate result
 */
class AggregateQueryImpl<T> implements AggregateQuery<T> {

    private final MatchQueryAdmin matchQuery;
    private final Aggregate<? super Map<VarName, Concept>, T> aggregate;

    AggregateQueryImpl(MatchQueryAdmin matchQuery, Aggregate<? super Map<VarName, Concept>, T> aggregate) {
        this.matchQuery = matchQuery;
        this.aggregate = aggregate;
    }

    @Override
    public AggregateQuery<T> withGraph(GraknGraph graph) {
        return new AggregateQueryImpl<>(matchQuery.withGraph(graph).admin(), aggregate);
    }

    @Override
    public T execute() {
        return aggregate.apply(matchQuery.streamWithVarNames());
    }

    @Override
    public Stream<String> resultsString(Printer printer) {
        return Stream.of(printer.graqlString(execute()));
    }

    @Override
    public boolean isReadOnly() {
        // An aggregate query may modify the graph if using a user-defined aggregate method
        return false;
    }

    @Override
    public String toString() {
        return matchQuery.toString() + " aggregate " + aggregate.toString() + ";";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AggregateQueryImpl<?> that = (AggregateQueryImpl<?>) o;

        if (!matchQuery.equals(that.matchQuery)) return false;
        return aggregate.equals(that.aggregate);
    }

    @Override
    public int hashCode() {
        int result = matchQuery.hashCode();
        result = 31 * result + aggregate.hashCode();
        return result;
    }
}
