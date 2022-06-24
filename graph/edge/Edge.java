/*
 * Copyright (C) 2022 Vaticle
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
 *
 */

package com.vaticle.typedb.core.graph.edge;

import com.vaticle.typedb.core.graph.common.Encoding;
import com.vaticle.typedb.core.graph.vertex.Vertex;

public interface Edge<EDGE_ENCODING extends Encoding.Edge, VERTEX extends Vertex<?, ?>> {

    /**
     * Returns the encoding of this edge.
     *
     * @return the encoding of this edge
     */
    EDGE_ENCODING encoding();

    /**
     * Returns the tail vertex of this edge.
     *
     * @return the tail vertex of this edge
     */
    VERTEX from();

    /**
     * Returns the head vertex of this edge.
     *
     * @return the head vertex of this edge
     */
    VERTEX to();

    /**
     * Deletes this edge from the graph.
     *
     * The delete operation should also remove this edge from its tail and head vertices.
     */
    void delete();

    /**
     * Commits the edge to the graph for persistent storage.
     */
    void commit();
}
