/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.graph.vertex;

import com.vaticle.typedb.core.common.parameters.Concept.Existence;
import com.vaticle.typedb.core.encoding.Encoding;
import com.vaticle.typedb.core.encoding.iid.VertexIID;
import com.vaticle.typedb.core.graph.GraphManager;
import com.vaticle.typedb.core.graph.ThingGraph;
import com.vaticle.typedb.core.graph.adjacency.ThingAdjacency;

public interface ThingVertex extends Vertex<VertexIID.Thing, Encoding.Vertex.Thing> {

    /**
     * Returns the {@code ThingGraph} containing all {@code ThingVertex}.
     *
     * @return the {@code ThingGraph} containing all {@code ThingVertex}
     */
    ThingGraph graph();

    /**
     * Returns the {@code GraphManager} containing both {@code TypeGraph} and {@code ThingGraph}.
     *
     * @return the {@code GraphManager} containing both {@code TypeGraph} and {@code ThingGraph}
     */
    GraphManager graphs();

    /**
     * Returns the {@code TypeVertex} in which this {@code ThingVertex} is an instance of.
     *
     * @return the {@code TypeVertex} in which this {@code ThingVertex} is an instance of
     */
    TypeVertex type();

    /**
     * Returns the {@code ThingAdjacency} set of outgoing edges.
     *
     * @return the {@code ThingAdjacency} set of outgoing edges
     */
    ThingAdjacency.Out outs();

    /**
     * Returns the {@code ThingAdjacency} set of incoming edges.
     *
     * @return the {@code ThingAdjacency} set of incoming edges
     */
    ThingAdjacency.In ins();

    /**
     * Returns the mode of {@code Existence} of this {@code ThingVertex}.
     *
     * @return {@code INFERRED} if this {@code ThingVertex} is a result of inference, {@code STORED} otherwise
     */
    Existence existence();

    /**
     * Returns true if this {@code ThingVertex} is an instance of {@code AttributeVertex}.
     *
     * @return true if this {@code ThingVertex} is an instance of {@code AttributeVertex}
     */
    boolean isAttribute();

    /**
     * Casts this {@code ThingVertex} into an {@code AttributeVertex} if it is one.
     *
     * @return this object as an {@code AttributeVertex}
     */
    AttributeVertex<?> asAttribute();

    boolean isWrite();

    ThingVertex.Write asWrite();

    ThingVertex.Write toWrite();

    interface Write extends ThingVertex {

        /**
         * Returns the {@code ThingAdjacency} set of outgoing edges.
         *
         * @return the {@code ThingAdjacency} set of outgoing edges
         */
        ThingAdjacency.Write.Out outs();

        /**
         * Returns the {@code ThingAdjacency} set of incoming edges.
         *
         * @return the {@code ThingAdjacency} set of incoming edges
         */
        ThingAdjacency.Write.In ins();

        void setModified();

        void delete();

        boolean isDeleted();

        void commit();

        @Override
        AttributeVertex.Write<?> asAttribute();

    }

}
