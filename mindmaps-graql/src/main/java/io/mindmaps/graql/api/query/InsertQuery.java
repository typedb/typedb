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
