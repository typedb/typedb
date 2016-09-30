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

package io.mindmaps.graql.admin;

import io.mindmaps.MindmapsGraph;
import io.mindmaps.concept.Concept;

import java.util.stream.Stream;

/**
 * A property of a {@link VarAdmin}, such as "isa movie" or "has name 'Jim'"
 */
public interface VarProperty {

    /**
     * Build a Graql string representation of this property
     * @param builder a string builder to append to
     */
    void buildString(StringBuilder builder);

    /**
     * Get the Graql string representation of this property
     */
    default String graqlString() {
        StringBuilder builder = new StringBuilder();
        buildString(builder);
        return builder.toString();
    }

    /**
     * Get a stream of any inner {@link VarAdmin} within this `VarProperty`.
     */
    Stream<VarAdmin> getInnerVars();

    /**
     * Get a stream of any inner {@link VarAdmin} within this `VarProperty`, including any that may have been
     * implicitly created (such as with "has-resource").
     */
    Stream<VarAdmin> getImplicitInnerVars();

    /**
     * Delete the given property from the graph, if possible.
     * @param graph the graph to operate on
     * @param concept the concept to delete properties of
     */
    void deleteProperty(MindmapsGraph graph, Concept concept) throws IllegalStateException;

    /**
     * True if there is at most one of these properties for each {@link VarAdmin}
     */
    default boolean isUnique() {
        return false;
    }
}
