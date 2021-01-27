/*
 * Copyright (C) 2021 Grakn Labs
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

import grakn.core.common.iterator.ResourceIterator;
import grakn.core.graph.common.Encoding;
import grakn.core.graph.edge.TypeEdge;
import grakn.core.graph.vertex.TypeVertex;

public interface TypeAdjacency {

    /**
     * Returns an {@code IteratorBuilder} to retrieve vertices of a set of edges.
     *
     * This method allows us to traverse the graph, by going from one vertex to
     * another, that are connected by edges that match the provided {@code encoding}.
     *
     * @param encoding the {@code Encoding} to filter the type of edges
     * @return an {@code IteratorBuilder} to retrieve vertices of a set of edges.
     */
    TypeIteratorBuilder edge(Encoding.Edge.Type encoding);

    /**
     * Returns an edge of type {@code encoding} that connects to an {@code adjacent}
     * vertex.
     *
     * @param encoding type of the edge to filter by
     * @param adjacent vertex that the edge connects to
     * @return an edge of type {@code encoding} that connects to {@code adjacent}.
     */
    TypeEdge edge(Encoding.Edge.Type encoding, TypeVertex adjacent);

    TypeEdge put(Encoding.Edge.Type encoding, TypeVertex adjacent);

    /**
     * Deletes all edges with a given encoding from the {@code Adjacency} map.
     *
     * This is a recursive delete operation. Deleting the edges from this
     * {@code Adjacency} map will also delete it from the {@code Adjacency} map
     * of the previously adjacent vertex.
     *
     * @param encoding type of the edge to the adjacent vertex
     */
    void delete(Encoding.Edge.Type encoding);

    void deleteAll();

    TypeEdge cache(TypeEdge edge);

    void remove(TypeEdge edge);

    void commit();


    /**
     * When used in combination with purely retrieving type edges (by infix encoding),
     * this iterator builder performs safe vertex downcasts at both ends of the edge
     */
    class TypeIteratorBuilder {

        private final ResourceIterator<TypeEdge> edgeIterator;

        public TypeIteratorBuilder(ResourceIterator<TypeEdge> edgeIterator) {
            this.edgeIterator = edgeIterator;
        }

        public ResourceIterator<TypeVertex> from() {
            return edgeIterator.map(edge -> edge.from().asType());
        }

        public ResourceIterator<TypeVertex> to() {
            return edgeIterator.map(edge -> edge.to().asType());
        }

        public ResourceIterator<TypeVertex> overridden() {
            return edgeIterator.map(TypeEdge::overridden);
        }

        public ResourceIterator<TypeEdge> edge() {
            return edgeIterator;
        }
    }

}
