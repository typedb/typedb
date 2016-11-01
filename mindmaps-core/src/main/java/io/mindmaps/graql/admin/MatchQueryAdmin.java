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

import io.mindmaps.MindmapsGraph;
import io.mindmaps.concept.Type;
import io.mindmaps.graql.MatchQuery;

import java.util.Optional;
import java.util.Set;

/**
 * Admin class for inspecting and manipulating a MatchQuery
 */
public interface MatchQueryAdmin extends MatchQuery {

    @Override
    default MatchQueryAdmin admin() {
        return this;
    }

    /**
     * @param graph the graph to use to get types from
     * @return all concept types referred to explicitly in the query
     */
    Set<Type> getTypes(MindmapsGraph graph);

    /**
     * @return all concept types referred to explicitly in the query
     */
    Set<Type> getTypes();

    /**
     * @return the pattern to match in the graph
     */
    Conjunction<PatternAdmin> getPattern();

    /**
     * @return the graph the query operates on, if one was provided
     */
    Optional<MindmapsGraph> getGraph();

    /**
     * @return all selected variable names in the query
     */
    Set<String> getSelectedNames();
}
