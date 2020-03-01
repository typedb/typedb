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

package grakn.core.graph.graphdb.query.vertex;

import grakn.core.graph.core.JanusGraphVertex;
import grakn.core.graph.core.VertexList;

/**
 * Extends on the VertexList interface by provided methods to add elements to the list
 * which is needed during query execution when the result list is created.
 *
 */
public interface VertexListInternal extends VertexList {

    /**
     * Adds the provided vertex to this list.
     */
    void add(JanusGraphVertex n);

    /**
     * Copies all vertices in the given vertex list into this list.
     */
    void addAll(VertexList vertices);

}
