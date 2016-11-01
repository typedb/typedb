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

package io.mindmaps.graql;

import io.mindmaps.MindmapsGraph;
import io.mindmaps.graql.admin.AskQueryAdmin;

/**
 * A query that will return whether a match query can be found in the graph.
 * <p>
 * An {@code AskQuery} is created from a {@code MatchQuery}, which describes what patterns it should find.
 */
public interface AskQuery extends Query<Boolean> {

    /**
     * @param graph the graph to execute the query on
     * @return a new AskQuery with the graph set
     */
    AskQuery withGraph(MindmapsGraph graph);

    /**
     * @return admin instance for inspecting and manipulating this query
     */
    AskQueryAdmin admin();
}
