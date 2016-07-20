package io.mindmaps.graql.api.query;

import io.mindmaps.core.dao.MindmapsTransaction;
import io.mindmaps.core.model.Concept;
import io.mindmaps.core.model.Type;

import java.util.*;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

/**
 * a query used for finding data in a graph that matches the given patterns.
 * <p>
 * The {@code MatchQuery} is a pattern-matching query. The patterns are described in a declarative fashion, forming a
 * subgraph, then the {@code MatchQuery} will traverse the graph in an efficient fashion to find any matching subgraphs.
 * Each matching subgraph will produce a map, where keys are variable names and values are concepts in the graph.
 */
@SuppressWarnings("UnusedReturnValue")
public interface MatchQuery extends Streamable<Map<String, Concept>> {

    /**
     * @return a stream of result maps, where keys are variable names and values are concepts
     */
    Stream<Map<String, Concept>> stream();

    /**
     * @param names an array of variable names to select
     * @return this
     */
    default MatchQuery select(String... names) {
        return select(Arrays.asList(names));
    }

    /**
     * @param names a collection of variable names to select
     * @return this
     */
    MatchQuery select(Collection<String> names);

    /**
     * @param name a variable name to get
     * @return a streamable/iterable of concepts
     */
    Streamable<Concept> get(String name);

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
    default DeleteQuery delete(String... names) {
        List<Var> deleters = Arrays.asList(names).stream().map(QueryBuilder::var).collect(toList());
        return delete(deleters);
    }

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
     * @param transaction the transaction to execute the query on
     * @return this
     */
    MatchQuery withTransaction(MindmapsTransaction transaction);

    /**
     * @param limit the maximum number of results the query should return
     * @return this
     */
    MatchQuery limit(long limit);

    /**
     * @param offset the number of results to skip
     * @return this
     */
    MatchQuery offset(long offset);

    /**
     * remove any duplicate results from the query
     * @return this
     */
    MatchQuery distinct();

    /**
     * Order the results by degree in ascending order
     * @param varName the variable name to order the results by
     * @return this
     */
    default MatchQuery orderBy(String varName) {
        return orderBy(varName, true);
    }

    /**
     * Order the results by degree
     * @param varName the variable name to order the results by
     * @param asc whether to use ascending order
     * @return this
     */
    MatchQuery orderBy(String varName, boolean asc);

    /**
     * Order the results by a resource in ascending order
     * @param varName the variable name to order the results by
     * @param resourceType the resource type attached to the variable to use for ordering
     * @return this
     */
    default MatchQuery orderBy(String varName, String resourceType) {
        return orderBy(varName, resourceType, true);
    }

    /**
     * Order the results by a resource
     * @param varName the variable name to order the results by
     * @param resourceType the resource type attached to the variable to use for ordering
     * @param asc whether to use ascending order
     * @return this
     */
    MatchQuery orderBy(String varName, String resourceType, boolean asc);

    /**
     * @return admin instance for inspecting and manipulating this query
     */
    Admin admin();

    /**
     * Admin class for inspecting and manipulating a MatchQuery
     */
    interface Admin extends MatchQuery {
        /**
         * @return all concept types referred to explicitly in the query
         */
        Set<Type> getTypes();

        /**
         * @return all selected variable names in the query
         */
        Set<String> getSelectedNames();

        /**
         * @return the pattern to match in the graph
         */
        Pattern.Conjunction<Pattern.Admin> getPattern();
    }
}
