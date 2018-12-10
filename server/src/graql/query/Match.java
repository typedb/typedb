/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package grakn.core.graql.query;

import grakn.core.graql.answer.ConceptMap;
import grakn.core.graql.query.pattern.Variable;
import grakn.core.graql.query.pattern.Statement;
import grakn.core.server.Transaction;
import grakn.core.graql.admin.MatchAdmin;
import grakn.core.graql.answer.Answer;

import javax.annotation.CheckReturnValue;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Stream;

/**
 * a part of a query used for finding data in a knowledge base that matches the given patterns.
 * <p>
 * The {@link Match} is the pattern-matching part of a query. The patterns are described in a declarative fashion,
 * then the {@link Match} will traverse the knowledge base in an efficient fashion to find any matching answers.
 * <p>
 * @see ConceptMap
 *
 */
public interface Match extends Iterable<ConceptMap> {

    /**
     * @return iterator over match results
     */
    @Override
    @CheckReturnValue
    default Iterator<ConceptMap> iterator() {
        return stream().iterator();
    }

    /**
     * @return a stream of match results
     */
    @CheckReturnValue
    Stream<ConceptMap> stream();

    /**
     * Construct a get query with all all {@link Variable}s mentioned in the query
     */
    @CheckReturnValue
    GetQuery get();

    /**
     * @param vars an array of variables to select
     * @return a {@link GetQuery} that selects the given variables
     */
    @CheckReturnValue
    GetQuery get(String var, String... vars);

    /**
     * @param vars an array of {@link Variable}s to select
     * @return a {@link GetQuery} that selects the given variables
     */
    @CheckReturnValue
    GetQuery get(Variable var, Variable... vars);

    /**
     * @param vars a set of {@link Variable}s to select
     * @return a {@link GetQuery} that selects the given variables
     */
    @CheckReturnValue
    GetQuery get(Set<Variable> vars);

    /**
     * @param vars an array of variables to insert for each result of this {@link Match}
     * @return an insert query that will insert the given variables for each result of this {@link Match}
     */
    @CheckReturnValue
    InsertQuery insert(Statement... vars);

    /**
     * @param vars a collection of variables to insert for each result of this {@link Match}
     * @return an insert query that will insert the given variables for each result of this {@link Match}
     */
    @CheckReturnValue
    InsertQuery insert(Collection<? extends Statement> vars);

    /**
     * Construct a delete query with all all {@link Variable}s mentioned in the query
     */
    @CheckReturnValue
    DeleteQuery delete();

    /**
     * @param vars an array of variables to delete for each result of this {@link Match}
     * @return a delete query that will delete the given variables for each result of this {@link Match}
     */
    @CheckReturnValue
    DeleteQuery delete(String var, String... vars);

    /**
     * @param vars an array of variables to delete for each result of this {@link Match}
     * @return a delete query that will delete the given variables for each result of this {@link Match}
     */
    @CheckReturnValue
    DeleteQuery delete(Variable var, Variable... vars);

    /**
     * @param vars a collection of variables to delete for each result of this {@link Match}
     * @return a delete query that will delete the given variables for each result of this {@link Match}
     */
    @CheckReturnValue
    DeleteQuery delete(Set<Variable> vars);

    /**
     * Aggregate results of a query.
     * @param aggregate the aggregate operation to apply
     * @param <T> the type of the aggregate result
     * @return a query that will yield the aggregate result
     */
    @CheckReturnValue
    <T extends Answer> AggregateQuery<T> aggregate(Aggregate<T> aggregate);

    /**
     * @return admin instance for inspecting and manipulating this query
     */
    @CheckReturnValue
    MatchAdmin admin();
}
