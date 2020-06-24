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

import hypergraph.graph.edge.ThingEdge;
import hypergraph.graph.iid.IID;
import hypergraph.graph.iid.SuffixIID;
import hypergraph.graph.iid.VertexIID;
import hypergraph.graph.util.Schema;
import hypergraph.graph.vertex.ThingVertex;

public interface ThingAdjacency extends Adjacency<Schema.Edge.Thing, ThingEdge, ThingVertex> {

    /**
     * Returns an {@code IteratorBuilder} to retrieve vertices of a set of edges.
     *
     * This method allows us to traverse the graph, by going from one vertex to
     * another, that are connected by edges that match the provided {@code schema}
     * and {@code lookahead}.
     *
     * @param schema    type of the edge to filter by
     * @param lookAhead information of the adjacent edge to filter the edges with
     * @return an {@code IteratorBuilder} to retrieve vertices of a set of edges.
     */
    IteratorBuilder<ThingVertex> edge(Schema.Edge.Thing schema, IID... lookAhead);

    /**
     * Returns an edge of type {@code schema} that connects to an {@code adjacent}
     * vertex with the provided {@code infixTail} and {@code suffixTail}.
     *
     * @param schema     type of the edge to filter by
     * @param adjacent   vertex that the edge connects to
     * @param infixTail  appending the {@code Infix} of the edge
     * @param suffixTail appending to the end of the edge
     * @return an edge of type {@code schema} that connects to {@code adjacent}.
     */
    ThingEdge edge(Schema.Edge.Thing schema, ThingVertex adjacent, VertexIID.Type infixTail, SuffixIID suffixTail);

    /**
     * Puts an edge of type {@code schema} from the owner to an adjacent vertex.
     *
     * The owner of this {@code Adjacency} map will also be added as an adjacent
     * vertex to the provided vertex, through an opposite facing edge stored in
     * an {@code Adjacency} map with an opposite direction to this one. I.e.
     * This is a recursive put operation.
     *
     * @param schema     type of the edge
     * @param adjacent   the adjacent vertex
     * @param infixTail  which will be appended to the {@code Infix} of the edge
     * @param suffixTail which will be appended to the end of the edge
     */
    void put(Schema.Edge.Thing schema, ThingVertex adjacent, VertexIID.Type infixTail, SuffixIID suffixTail);

    /**
     * Deletes a set of edges that match the provided properties.
     *
     * @param schema    type of the edge to filter by
     * @param lookAhead information of the adjacent edge to filter the edges with
     */
    void delete(Schema.Edge.Thing schema, IID[] lookAhead);
}
