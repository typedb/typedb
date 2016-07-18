package io.mindmaps.graql.api.query;

import io.mindmaps.core.dao.MindmapsTransaction;

import java.util.Collection;

/**
 * A query for deleting concepts from a match query.
 * <p>
 * A {@code DeleteQuery} is built from a {@code MatchQuery} and will perform a delete operation for every result of
 * the @{code MatchQuery}.
 * <p>
 * The delete operation to perform is based on what {@code Var} objects are provided to it. If only variable names
 * are provided, then the delete query will delete the concept bound to each given variable name. If property flags
 * are provided, e.g. {@code var("x").has("name")} then only those properties are deleted.
 */
public interface DeleteQuery {

    /**
     * Execute the delete query
     */
    void execute();

    /**
     * @param transaction the transaction to execute the query on
     * @return this
     */
    DeleteQuery withTransaction(MindmapsTransaction transaction);

    /**
     * @return admin instance for inspecting and manipulating this query
     */
    Admin admin();

    /**
     * Admin class for inspecting and manipulating a DeleteQuery
     */
    interface Admin extends DeleteQuery {
        /**
         * @return the variables to delete
         */
        Collection<Var.Admin> getDeleters();

        /**
         * @return the match query this delete query is operating on
         */
        MatchQuery getMatchQuery();
    }
}
