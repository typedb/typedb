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
import ai.grakn.graql.AskQuery;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.Printer;
import ai.grakn.graql.admin.AskQueryAdmin;

import java.util.stream.Stream;

/**
 * An AskQuery to check if a given pattern matches anywhere in the graph
 */
class AskQueryImpl implements AskQueryAdmin {

    private final MatchQuery matchQuery;

    /**
     * @param matchQuery the match query that the ask query will search for in the graph
     */
    AskQueryImpl(MatchQuery matchQuery) {
        this.matchQuery = matchQuery;
    }

    @Override
    public Boolean execute() {
        return matchQuery.iterator().hasNext();
    }

    @Override
    public Stream<String> resultsString(Printer printer) {
        return Stream.of(printer.graqlString(execute()));
    }

    @Override
    public boolean isReadOnly() {
        return true;
    }

    @Override
    public AskQuery withGraph(GraknGraph graph) {
        return new AskQueryImpl(matchQuery.withGraph(graph));
    }

    @Override
    public AskQueryAdmin admin() {
        return this;
    }

    @Override
    public String toString() {
        return matchQuery.toString() + " ask;";
    }

    @Override
    public MatchQuery getMatchQuery() {
        return matchQuery;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AskQueryImpl askQuery = (AskQueryImpl) o;

        return matchQuery.equals(askQuery.matchQuery);
    }

    @Override
    public int hashCode() {
        return matchQuery.hashCode();
    }
}