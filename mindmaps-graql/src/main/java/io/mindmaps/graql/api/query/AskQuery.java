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
