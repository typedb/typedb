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

package grakn.core.graph.adjacency;

import grakn.core.graph.edge.Edge;
import grakn.core.graph.util.Encoding;
import grakn.core.graph.vertex.Vertex;

import java.util.Iterator;
import java.util.function.Consumer;

public interface Adjacency<
        EDGE_ENCODING extends Encoding.Edge,
        EDGE extends Edge<?, ?, VERTEX>,
        VERTEX extends Vertex<?, ?, VERTEX, EDGE_ENCODING, EDGE>> {

    /**
     * Returns an {@code IteratorBuilder} to retrieve vertices of a set of edges.
     *
     * This method allows us to traverse the graph, by going from one vertex to
     * another, that are connected by edges that match the provided {@code encoding}.
     *
     * @param encoding the {@code Encoding} to filter the type of edges
     * @return an {@code IteratorBuilder} to retrieve vertices of a set of edges.
     */
    IteratorBuilder<VERTEX> edge(EDGE_ENCODING encoding);

    /**
     * Returns an edge of type {@code encoding} that connects to an {@code adjacent}
     * vertex.
     *
     * @param encoding   type of the edge to filter by
     * @param adjacent vertex that the edge connects to
     * @return an edge of type {@code encoding} that connects to {@code adjacent}.
     */
    EDGE edge(EDGE_ENCODING encoding, VERTEX adjacent);

    /**
     * Puts an adjacent vertex over an edge with a given encoding.
     *
     * The owner of this {@code Adjacency} map will also be added as an adjacent
     * vertex to the provided vertex, through an opposite facing edge stored in
     * an {@code Adjacency} map with an opposite direction to this one. I.e.
     * This is a recursive put operation.
     *
     * @param encoding   of the edge that will connect the owner to the adjacent vertex
     * @param adjacent the adjacent vertex
     */
    EDGE put(EDGE_ENCODING encoding, VERTEX adjacent);

    /**
     * Deletes all edges with a given encoding from the {@code Adjacency} map.
     *
     * This is a recursive delete operation. Deleting the edges from this
     * {@code Adjacency} map will also delete it from the {@code Adjacency} map
     * of the previously adjacent vertex.
     *
     * @param encoding type of the edge to the adjacent vertex
     */
    void delete(EDGE_ENCODING encoding);

    void deleteAll();

    void loadToBuffer(EDGE edge);

    void removeFromBuffer(EDGE edge);

    void forEach(Consumer<EDGE> function);

    enum Direction {
        OUT(true),
        IN(false);

        private final boolean isOut;

        Direction(boolean isOut) {
            this.isOut = isOut;
        }

        public boolean isOut() {
            return isOut;
        }

        public boolean isIn() {
            return !isOut;
        }
    }

    interface IteratorBuilder<ITER_VERTEX extends Vertex> {

        Iterator<ITER_VERTEX> to();

        Iterator<ITER_VERTEX> from();
    }
}
