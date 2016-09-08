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
import io.mindmaps.concept.Concept;
import io.mindmaps.graql.admin.InsertQueryAdmin;

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
     * @param graph the graph to execute the query on
     * @return a new InsertQuery with the graph set
     */
    InsertQuery withGraph(MindmapsGraph graph);

    /**
     * Execute the insert query
     */
    void execute();

    /**
     * @return admin instance for inspecting and manipulating this query
     */
    InsertQueryAdmin admin();

}
