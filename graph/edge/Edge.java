/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.graph.edge;

import com.vaticle.typedb.core.encoding.Encoding;
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

    boolean isDeleted();

    /**
     * Commits the edge to the graph for persistent storage.
     */
    void commit();
}
