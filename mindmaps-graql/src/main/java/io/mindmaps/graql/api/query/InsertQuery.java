package io.mindmaps.graql.api.query;

import io.mindmaps.core.dao.MindmapsTransaction;
import io.mindmaps.core.model.Concept;

import java.util.Collection;
import java.util.Optional;

/**
 * A query for inserting data.
 * <p>
 * A {@code InsertQuery} can be built from a {@code QueryBuilder} or a {@code MatchQuery}.
 * <p>
 * When built from a {@code QueryBuilder}, the insert query will execute once, inserting all the variables provided.
 * <p>
 * When built from a {@code MatchQuery}, the insert query will execute for each result of the {@code MatchQuery},
 * where variable names in the {@code InsertQuery} are bound to the concept in the result of the {@code MatchQuery}.
 */
public interface InsertQuery extends Streamable<Concept> {

    /**
     * @param transaction the transaction to execute the query on
     * @return this
     */
    InsertQuery withTransaction(MindmapsTransaction transaction);

    /**
     * Execute the insert query
     */
    void execute();

    /**
     * @return admin instance for inspecting and manipulating this query
     */
    Admin admin();

    /**
     * Admin class for inspecting and manipulating an InsertQuery
     */
    interface Admin extends InsertQuery {
        /**
         * @return the match query that this insert query is using, if it was provided one
         */
        Optional<MatchQuery> getMatchQuery();

        /**
         * @return the variables to insert in the insert query
         */
        Collection<Var.Admin> getVars();

        /**
         * @return a collection of Vars to insert, including any nested vars
         */
        Collection<Var.Admin> getAllVars();
    }
}
