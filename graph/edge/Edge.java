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
 *
 */

package hypergraph.graph.edge;

import hypergraph.graph.util.Schema;
import hypergraph.graph.vertex.VertexImpl;

public interface Edge<EDGE_SCHEMA extends Schema.Edge, VERTEX extends VertexImpl> {

    /**
     * Returns the schema of this edge.
     *
     * @return the schema of this edge
     */
    EDGE_SCHEMA schema();

    /**
     * Returns the status of this edge.
     *
     * The status could either be either {@code buffered}, {@code committed}, or {@code persisted}.
     *
     * @return the status of this edge
     */
    Schema.Status status();

    /**
     * Returns the {@code iid} of this edge pointing outwards.
     *
     * @return the {@code iid} of this edge pointing outwards
     */
    byte[] outIID();

    /**
     * Returns the {@code iid} of this edge pointing inwards.
     *
     * @return the {@code iid} of this edge pointing inwards
     */
    byte[] inIID();

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
     *
     * After committing this edge to the graph, the status of this edges should be {@code persisted}.
     */
    void commit();
}
