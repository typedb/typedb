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

package ai.grakn.graql;

import ai.grakn.GraknGraph;
import ai.grakn.graql.admin.MatchQueryAdmin;
import ai.grakn.concept.Concept;

import java.util.*;
import java.util.stream.Stream;

import static ai.grakn.graql.Order.asc;
import static java.util.stream.Collectors.toList;

/**
 * a query used for finding data in a graph that matches the given patterns.
 * <p>
 * The {@code MatchQuery} is a pattern-matching query. The patterns are described in a declarative fashion, forming a
 * subgraph, then the {@code MatchQuery} will traverse the graph in an efficient fashion to find any matching subgraphs.
 * <p>
 * Each matching subgraph will produce a map, where keys are variable names and values are concepts in the graph.
 */
public interface MatchQuery extends Query<List<Map<String, Concept>>>, Streamable<Map<String, Concept>> {

    @Override
    default List<Map<String, Concept>> execute() {
        return stream().collect(toList());
    }

    /**
     * @param names an array of variable names to select
     * @return a new MatchQuery that selects the given variables
     */
    default MatchQuery select(String... names) {
        return select(new HashSet<>(Arrays.asList(names)));
    }

    /**
     * @param names a set of variable names to select
     * @return a new MatchQuery that selects the given variables
     */
    MatchQuery select(Set<String> names);

    /**
     * @param name a variable name to get
     * @return a stream of concepts
     */
    Stream<Concept> get(String name);

    /**
     * @return an ask query that will return true if any matches are found
     */
    AskQuery ask();

    /**
     * @param vars an array of variables to insert for each result of this match query
     * @return an insert query that will insert the given variables for each result of this match query
     */
    default InsertQuery insert(Var... vars) {
        return insert(Arrays.asList(vars));
    }

    /**
     * @param vars a collection of variables to insert for each result of this match query
     * @return an insert query that will insert the given variables for each result of this match query
     */
    InsertQuery insert(Collection<? extends Var> vars);

    /**
     * @param names an array of variable names to delete for each result of this match query
     * @return a delete query that will delete the given variable names for each result of this match query
     */
    DeleteQuery delete(String... names);

    /**
     * @param deleters an array of variables stating what properties to delete for each result of this match query
     * @return a delete query that will delete the given properties for each result of this match query
     */
    default DeleteQuery delete(Var... deleters) {
        return delete(Arrays.asList(deleters));
    }

    /**
     * @param deleters a collection of variables stating what properties to delete for each result of this match query
     * @return a delete query that will delete the given properties for each result of this match query
     */
    DeleteQuery delete(Collection<? extends Var> deleters);

    /**
     * Order the results by degree in ascending order
     * @param varName the variable name to order the results by
     * @return a new MatchQuery with the given ordering
     */
    default MatchQuery orderBy(String varName) {
        return orderBy(varName, asc);
    }

    /**
     * Order the results by degree
     * @param varName the variable name to order the results by
     * @param order the ordering to use
     * @return a new MatchQuery with the given ordering
     */
    MatchQuery orderBy(String varName, Order order);

    /**
     * @param graph the graph to execute the query on
     * @return a new MatchQuery with the graph set
     */
    MatchQuery withGraph(GraknGraph graph);

    /**
     * @param limit the maximum number of results the query should return
     * @return a new MatchQuery with the limit set
     */
    MatchQuery limit(long limit);

    /**
     * @param offset the number of results to skip
     * @return a new MatchQuery with the offset set
     */
    MatchQuery offset(long offset);

    /**
     * remove any duplicate results from the query
     * @return a new MatchQuery without duplicate results
     */
    MatchQuery distinct();

    /**
     * Use rules in the graph in order to infer additional results
     */
    MatchQuery infer();

    /**
     * Aggregate results of a query.
     * @param aggregate the aggregate operation to apply
     * @param <S> the type of the aggregate result
     * @return a query that will yield the aggregate result
     */
    <S> AggregateQuery<S> aggregate(Aggregate<? super Map<String, Concept>, S> aggregate);

    /**
     * @return admin instance for inspecting and manipulating this query
     */
    MatchQueryAdmin admin();
}
