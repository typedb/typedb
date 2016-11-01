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
 *
 */

package io.mindmaps.graql.admin;

import io.mindmaps.graql.DeleteQuery;
import io.mindmaps.graql.MatchQuery;

import java.util.Collection;

/**
 * Admin class for inspecting and manipulating a DeleteQuery
 */
public interface DeleteQueryAdmin extends DeleteQuery {
    /**
     * @return the variables to delete
     */
    Collection<VarAdmin> getDeleters();

    /**
     * @return the match query this delete query is operating on
     */
    MatchQuery getMatchQuery();
}
