package io.mindmaps.graql.api.query;

import io.mindmaps.core.dao.MindmapsTransaction;

/**
 * A query that will return whether a match query can be found in the graph.
 * <p>
 * An {@code AskQuery} is created from a {@code MatchQuery}, which describes what patterns it should find.
 */
public interface AskQuery {
    /**
     * @return whether the given patterns can be found in the graph
     */
    boolean execute();

    /**
     * @param transaction the transaction to execute the query on
     * @return this
     */
    AskQuery withTransaction(MindmapsTransaction transaction);

    /**
     * @return admin instance for inspecting and manipulating this query
     */
    Admin admin();

    /**
     * Admin class for inspecting and manipulating an AskQuery
     */
    interface Admin extends AskQuery {
        /**
         * @return the match query used to create this ask query
         */
        MatchQuery getMatchQuery();
    }
}
