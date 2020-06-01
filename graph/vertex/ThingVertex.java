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

package hypergraph.graph.vertex;

import hypergraph.graph.ThingGraph;
import hypergraph.graph.adjacency.ThingAdjacency;
import hypergraph.graph.edge.ThingEdge;
import hypergraph.graph.util.IID;
import hypergraph.graph.util.Schema;

public interface ThingVertex extends Vertex<
        IID.Vertex.Thing,
        Schema.Vertex.Thing, ThingVertex,
        Schema.Edge.Thing, ThingEdge> {

    /**
     * Returns the {@code Graph} containing all {@code ThingVertex}.
     *
     * @return the {@code Graph} containing all {@code ThingVertex}
     */
    @Override
    ThingGraph graph();

    /**
     * Returns the {@code ThingAdjacency} set of outgoing edges.
     *
     * @return the {@code ThingAdjacency} set of outgoing edges
     */
    @Override
    ThingAdjacency outs();

    /**
     * Returns the {@code ThingAdjacency} set of incoming edges.
     *
     * @return the {@code ThingAdjacency} set of incoming edges
     */
    @Override
    ThingAdjacency ins();

    /**
     * Returns the {@code TypeVertex} in which this {@code ThingVertex} is an instance of.
     *
     * @return the {@code TypeVertex} in which this {@code ThingVertex} is an instance of
     */
    TypeVertex type();

    /**
     * Returns true if this {@code ThingVertex} is a result of inference.
     *
     * @return true if this {@code ThingVertex} is a result of inference
     */
    boolean isInferred();

    /**
     * Sets a boolean flag to indicate whether this vertex was a result of inference
     *
     * @param isInferred indicating whether this vertex was a result of inference
     */
    void isInferred(boolean isInferred);
}
