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

package io.mindmaps.graql.internal.query;

import io.mindmaps.core.MindmapsTransaction;
import io.mindmaps.graql.AskQuery;
import io.mindmaps.graql.MatchQueryMap;

/**
 * An AskQuery to check if a given pattern matches anywhere in the graph
 */
public class AskQueryImpl implements AskQuery.Admin {

    private final MatchQueryMap matchQuery;

    /**
     * @param matchQuery the match query that the ask query will search for in the graph
     */
    public AskQueryImpl(MatchQueryMap matchQuery) {
        this.matchQuery = matchQuery;
    }

    @Override
    public boolean execute() {
        return matchQuery.iterator().hasNext();
    }

    @Override
    public AskQuery withTransaction(MindmapsTransaction transaction) {
        return new AskQueryImpl(matchQuery.withTransaction(transaction));
    }

    @Override
    public Admin admin() {
        return this;
    }

    @Override
    public String toString() {
        return matchQuery.toString() + " ask";
    }

    @Override
    public MatchQueryMap getMatchQuery() {
        return matchQuery;
    }
}