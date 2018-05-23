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

package ai.grakn.graql;

import ai.grakn.GraknTx;
import ai.grakn.concept.Concept;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.MatchAdmin;

import javax.annotation.CheckReturnValue;
import java.util.Collection;
import java.util.Set;
import java.util.stream.Stream;

/**
 * a part of a query used for finding data in a knowledge base that matches the given patterns.
 * <p>
 * The {@link Match} is the pattern-matching part of a query. The patterns are described in a declarative fashion,
 * then the {@link Match} will traverse the knowledge base in an efficient fashion to find any matching answers.
 * <p>
 * @see Answer
 *
 * @author Felix Chapman
 */
public interface Match extends Streamable<Answer> {

    /**
     * @param var a variable to get
     * @return a output of concepts
     */
    @CheckReturnValue
    Stream<Concept> get(String var);

    /**
     * @param var a variable to get
     * @return a output of concepts
     */
    @CheckReturnValue
    Stream<Concept> get(Var var);

    /**
     * Get all {@link Var}s mentioned in the query
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
     * @param vars an array of {@link Var}s to select
     * @return a {@link GetQuery} that selects the given variables
     */
    @CheckReturnValue
    GetQuery get(Var var, Var... vars);

    /**
     * @param vars a set of {@link Var}s to select
     * @return a {@link GetQuery} that selects the given variables
     */
    @CheckReturnValue
    GetQuery get(Set<Var> vars);

    /**
     * @param vars an array of variables to insert for each result of this {@link Match}
     * @return an insert query that will insert the given variables for each result of this {@link Match}
     */
    @CheckReturnValue
    InsertQuery insert(VarPattern... vars);

    /**
     * @param vars a collection of variables to insert for each result of this {@link Match}
     * @return an insert query that will insert the given variables for each result of this {@link Match}
     */
    @CheckReturnValue
    InsertQuery insert(Collection<? extends VarPattern> vars);

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
    DeleteQuery delete(Var... vars);

    /**
     * @param vars a collection of variables to delete for each result of this {@link Match}
     * @return a delete query that will delete the given variables for each result of this {@link Match}
     */
    @CheckReturnValue
    DeleteQuery delete(Collection<? extends Var> vars);

    /**
     * Order the results by degree in ascending order
     * @param varName the variable name to order the results by
     * @return a new {@link Match} with the given ordering
     */
    @CheckReturnValue
    Match orderBy(String varName);

    /**
     * Order the results by degree in ascending order
     * @param varName the variable name to order the results by
     * @return a new {@link Match} with the given ordering
     */
    @CheckReturnValue
    Match orderBy(Var varName);

    /**
     * Order the results by degree
     * @param varName the variable name to order the results by
     * @param order the ordering to use
     * @return a new {@link Match} with the given ordering
     */
    @CheckReturnValue
    Match orderBy(String varName, Order order);

    /**
     * Order the results by degree
     * @param varName the variable name to order the results by
     * @param order the ordering to use
     * @return a new {@link Match} with the given ordering
     */
    @CheckReturnValue
    Match orderBy(Var varName, Order order);

    /**
     * @param tx the graph to execute the query on
     * @return a new {@link Match} with the graph set
     */
    Match withTx(GraknTx tx);

    /**
     * @param limit the maximum number of results the query should return
     * @return a new {@link Match} with the limit set
     */
    @CheckReturnValue
    Match limit(long limit);

    /**
     * @param offset the number of results to skip
     * @return a new {@link Match} with the offset set
     */
    @CheckReturnValue
    Match offset(long offset);

    /**
     * Aggregate results of a query.
     * @param aggregate the aggregate operation to apply
     * @param <S> the type of the aggregate result
     * @return a query that will yield the aggregate result
     */
    @CheckReturnValue
    <S> AggregateQuery<S> aggregate(Aggregate<? super Answer, S> aggregate);

    /**
     * @return admin instance for inspecting and manipulating this query
     */
    @CheckReturnValue
    MatchAdmin admin();
}
