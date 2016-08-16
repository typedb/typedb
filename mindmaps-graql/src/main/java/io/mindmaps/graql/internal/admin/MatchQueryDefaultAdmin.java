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

package io.mindmaps.graql.internal.admin;

import io.mindmaps.core.model.Concept;
import io.mindmaps.graql.MatchQueryDefault;

import java.util.Map;
import java.util.Set;

/**
 * Admin class for inspecting and manipulating a MatchQuery
 */
public interface MatchQueryDefaultAdmin extends MatchQueryDefault, MatchQueryAdmin<Map<String, Concept>> {

    @Override
    default MatchQueryDefaultAdmin admin() {
        return this;
    }

    /**
     * @return all selected variable names in the query
     */
    Set<String> getSelectedNames();
}
