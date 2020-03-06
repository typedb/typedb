/*
 * Copyright (C) 2020 Grakn Labs
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package grakn.core.graph.core;

import java.util.List;

/**
 * List of JanusGraphVertexs.
 * <p>
 * Basic interface for a list of vertices which supports retrieving individuals vertices or iterating over all of them,
 * but does not support modification.
 * <p>
 * VertexList is returned by JanusGraphVertexQuery. Depending on how the query was executed that returned this VertexList,
 * getting vertex ids might be significantly faster than retrieving vertex objects.
 */
public interface VertexList extends Iterable<JanusGraphVertex> {

    /**
     * Returns the number of vertices in this list.
     *
     * @return Number of vertices in the list.
     */
    int size();

    /**
     * Returns the vertex at a given position in the list.
     *
     * @param pos Position for which to retrieve the vertex.
     * @return JanusGraphVertex at the given position
     */
    JanusGraphVertex get(int pos);

    /**
     * Whether this list of vertices is sorted by id in increasing order.
     */
    boolean isSorted();

    /**
     * Returns a sub list of this list of vertices from the given position with the given number of vertices.
     */
    VertexList subList(int fromPosition, int length);

    /**
     * Returns a list of ids of all vertices in this list of vertices in the same order of the original vertex list.
     * <p>
     * Uses an efficient primitive variable-sized array.
     *
     * @return A list of idAuthorities of all vertices in this list of vertices in the same order of the original vertex list.
     */
    List<Long> getIDs();

    /**
     * Returns the id of the vertex at the specified position
     *
     * @param pos The position of the vertex in the list
     * @return The id of that vertex
     */
    long getID(int pos);

}
