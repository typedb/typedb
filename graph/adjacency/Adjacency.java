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

package hypergraph.graph.adjacency;

import hypergraph.graph.edge.Edge;
import hypergraph.graph.iid.VertexIID;
import hypergraph.graph.util.Schema;
import hypergraph.graph.vertex.Vertex;

import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

import static java.util.Collections.emptyList;

public interface Adjacency<
        EDGE_SCHEMA extends Schema.Edge,
        EDGE extends Edge<EDGE_SCHEMA, ?, VERTEX>,
        VERTEX extends Vertex<?, ?, VERTEX, EDGE_SCHEMA, EDGE>> {

    IteratorBuilder<VERTEX> edge(EDGE_SCHEMA schema);

    EDGE edge(EDGE_SCHEMA schema, VERTEX adjacent);

    /**
     * Puts an adjacent vertex over an edge with a given schema.
     *
     * The owner of this {@code Adjacency} map will also be added as an adjacent
     * vertex to the provided vertex, through an opposite facing edge stored in
     * an {@code Adjacency} map with an opposite direction to this one. I.e.
     * This is a recursive put operation.
     *
     * @param schema   of the edge that will connect the owner to the adjacent vertex
     * @param adjacent the adjacent vertex
     */
    void put(EDGE_SCHEMA schema, VERTEX adjacent);

    void putNonRecursive(EDGE edge);

    void load(EDGE edge);

    void delete(EDGE_SCHEMA schema, VERTEX adjacent);

    /**
     * Deletes an edge with a given schema from the {@code Adjacency} map.
     *
     * Deleting the edge renders all adjacent vertices connected through this
     * edge to no longer be connected to the owner of this {@code Adjacency}
     * map, both through this {@code Adjacency} or the opposite one that the
     * disconnected vertices own. I.e. this is a recursive delete operation
     *
     * @param schema of the edge that will connect the owner to the adjacent vertex
     */
    void delete(EDGE_SCHEMA schema);

    void deleteNonRecursive(EDGE edge);

    void deleteAll();

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
