/*
 * Copyright (C) 2021 Vaticle
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

package com.vaticle.typedb.core.graph.adjacency;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.graph.common.Encoding;
import com.vaticle.typedb.core.graph.edge.ThingEdge;
import com.vaticle.typedb.core.graph.iid.EdgeIID;
import com.vaticle.typedb.core.graph.iid.IID;
import com.vaticle.typedb.core.graph.vertex.ThingVertex;

import java.util.Arrays;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_OPERATION;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;

public interface ThingAdjacency {

    /**
     * Returns an {@code IteratorBuilder} to retrieve vertices of a set of edges.
     *
     * This method allows us to traverse the graph, by going from one vertex to
     * another, that are connected by edges that match the provided {@code encoding}.
     *
     * @param encoding the {@code Encoding} to filter the type of edges
     * @return an {@code IteratorBuilder} to retrieve vertices of a set of edges.
     */
    IteratorBuilder edge(Encoding.Edge.Thing encoding);

    /**
     * Returns an {@code ThingIteratorSortedBuilder} to retrieve vertices of a set of edges.
     *
     * This method allows us to traverse the graph, by going from one vertex to
     * another, that are connected by edges that match the provided {@code encoding}
     * and {@code lookAhead}.
     *
     * @param encoding  type of the edge to filter by
     * @param lookAhead information of the adjacent edge to filter the edges with
     * @return an {@code IteratorBuilder} to retrieve vertices of a set of edges.
     */
    default SortedIteratorBuilder edge(Encoding.Edge.Thing encoding, IID... lookAhead) {
        if (encoding == Encoding.Edge.Thing.HAS) return edgeHas(lookAhead);
        else if (encoding == Encoding.Edge.Thing.PLAYING) return edgeHas(lookAhead);
        else if (encoding == Encoding.Edge.Thing.RELATING) return edgeHas(lookAhead);
        else if (encoding == Encoding.Edge.Thing.ROLEPLAYER) {
            if (lookAhead.length > 0)
                return edgeRolePlayer(lookAhead[0], Arrays.copyOfRange(lookAhead, 1, lookAhead.length));
            else throw TypeDBException.of(ILLEGAL_OPERATION);
        } else throw TypeDBException.of(ILLEGAL_STATE);
    }

    SortedIteratorBuilder edgeHas(IID... lookAhead);

    SortedIteratorBuilder edgePlaying(IID... lookAhead);

    SortedIteratorBuilder edgeRelating(IID... lookAhead);

    FunctionalIterator<EdgeDirected> edgeRolePlayer();

    FunctionalIterator.Sorted<EdgeDirected> edgeRolePlayer(IID roleType, IID... lookAhead);

    /**
     * Returns an edge of type {@code encoding} that connects to an {@code adjacent}
     * vertex.
     *
     * @param encoding type of the edge to filter by
     * @param adjacent vertex that the edge connects to
     * @return an edge of type {@code encoding} that connects to {@code adjacent}.
     */
    ThingEdge edge(Encoding.Edge.Thing encoding, ThingVertex adjacent);

    /**
     * Returns an edge of type {@code encoding} that connects to an {@code adjacent}
     * vertex, that is an optimisation edge over a given {@code optimised} vertex.
     *
     * @param encoding  type of the edge to filter by
     * @param adjacent  vertex that the edge connects to
     * @param optimised vertex that this optimised edge is compressing
     * @return an edge of type {@code encoding} that connects to {@code adjacent} through {@code optimised}.
     */
    ThingEdge edge(Encoding.Edge.Thing encoding, ThingVertex adjacent, ThingVertex optimised);

    interface IteratorBuilder {

        FunctionalIterator<ThingVertex> from();

        FunctionalIterator<ThingVertex> to();

        FunctionalIterator<ThingEdge> get();
    }

    interface SortedIteratorBuilder {

        FunctionalIterator<ThingVertex> from();

        FunctionalIterator<ThingVertex> to();

        FunctionalIterator.Sorted<EdgeDirected> get();
    }

    interface Write extends ThingAdjacency {

        /**
         * Puts an adjacent vertex over an edge with a given encoding.
         *
         * The owner of this {@code Adjacency} map will also be added as an adjacent
         * vertex to the provided vertex, through an opposite facing edge stored in
         * an {@code Adjacency} map with an opposite direction to this one. I.e.
         * This is a recursive put operation.
         *
         * @param encoding   of the edge that will connect the owner to the adjacent vertex
         * @param adjacent   the adjacent vertex
         * @param isInferred
         * @return an edge of type {@code encoding} that connects to {@code adjacent}.
         */
        ThingEdge put(Encoding.Edge.Thing encoding, ThingVertex.Write adjacent, boolean isInferred);

        /**
         * Puts an edge of type {@code encoding} from the owner to an adjacent vertex,
         * which is an optimisation edge over a given {@code optimised} vertex.
         *
         * The owner of this {@code Adjacency} map will also be added as an adjacent
         * vertex to the provided vertex, through an opposite facing edge stored in
         * an {@code Adjacency} map with an opposite direction to this one. I.e.
         * This is a recursive put operation.
         *
         * @param encoding   type of the edge
         * @param adjacent   the adjacent vertex
         * @param optimised  vertex that this optimised edge is compressing
         * @param isInferred
         * @return an edge of type {@code encoding} that connects to {@code adjacent}.
         */
        ThingEdge put(Encoding.Edge.Thing encoding, ThingVertex.Write adjacent, ThingVertex.Write optimised, boolean isInferred);

        /**
         * Deletes all edges with a given encoding from the {@code Adjacency} map.
         *
         * This is a recursive delete operation. Deleting the edges from this
         * {@code Adjacency} map will also delete it from the {@code Adjacency} map
         * of the previously adjacent vertex.
         *
         * @param encoding type of the edge to the adjacent vertex
         */
        void delete(Encoding.Edge.Thing encoding);

        /**
         * Deletes a set of edges that match the provided properties.
         *
         * @param encoding  type of the edge to filter by
         * @param lookAhead information of the adjacent edge to filter the edges with
         */
        void delete(Encoding.Edge.Thing encoding, IID... lookAhead);

        void deleteAll();

        ThingEdge cache(ThingEdge edge);

        void remove(ThingEdge edge);

        void commit();

    }

    EdgeDirected asSortable(ThingEdge edge);

    abstract class EdgeDirected implements Comparable<EdgeDirected> {

        public final ThingEdge edge;

        EdgeDirected(ThingEdge edge) {
            this.edge = edge;
        }

        public abstract EdgeIID.Thing getKey();

        public static EdgeDirected in(ThingEdge edge) {
            EdgeIID.Thing inIID = edge.inIID();
            return new EdgeDirected(edge) {
                @Override
                public EdgeIID.Thing getKey() {
                    return inIID;
                }
            };
        }

        public static EdgeDirected out(ThingEdge edge) {
            EdgeIID.Thing outIID = edge.outIID();
            return new EdgeDirected(edge) {
                @Override
                public EdgeIID.Thing getKey() {
                    return outIID;
                }
            };
        }

        public ThingEdge getEdge() {
            return edge;
        }

        @Override
        public int compareTo(EdgeDirected other) {
            return getKey().compareTo(other.getKey());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final EdgeDirected that = (EdgeDirected) o;
            return edge.equals(that.edge);
        }

        @Override
        public int hashCode() {
            return edge.hashCode();
        }
    }
}
