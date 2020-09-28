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

import grakn.core.common.iterator.ResourceIterator;
import grakn.core.graph.edge.SchemaEdge;
import grakn.core.graph.util.Encoding;
import grakn.core.graph.vertex.RuleVertex;
import grakn.core.graph.vertex.SchemaVertex;
import grakn.core.graph.vertex.TypeVertex;

public interface SchemaAdjacency {

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

    RuleIteratorBuilder edge(Encoding.Edge.Rule encoding);

    /**
     * Returns an edge of type {@code encoding} that connects to an {@code adjacent}
     * vertex.
     *
     * @param encoding type of the edge to filter by
     * @param adjacent vertex that the edge connects to
     * @return an edge of type {@code encoding} that connects to {@code adjacent}.
     */
    SchemaEdge edge(Encoding.Edge.Type encoding, TypeVertex adjacent);

    SchemaEdge put(Encoding.Edge.Schema encoding, SchemaVertex<?, ?> adjacent);

//    TODO not implemented yet -- we may not entirely need to have separate TypeEdge and RuleEdge?
//    TypeEdge put(Encoding.Edge.Type encoding, TypeVertex adjacent);
//    // note: inherent directionality means edge encoding will need to be checked against vertex type passed
//    RuleEdge put(Encoding.Edge.Rule encoding, RuleVertex adjacent);
//    RuleEdge put(Encoding.Edge.Rule encoding, TypeVertex adjacent);

    /**
     * Deletes all edges with a given encoding from the {@code Adjacency} map.
     *
     * This is a recursive delete operation. Deleting the edges from this
     * {@code Adjacency} map will also delete it from the {@code Adjacency} map
     * of the previously adjacent vertex.
     *
     * @param encoding type of the edge to the adjacent vertex
     */
    void delete(Encoding.Edge.Schema encoding);

    void deleteAll();

    SchemaEdge cache(SchemaEdge edge);

    void remove(SchemaEdge edge);

    void commit();

    interface TypeIteratorBuilder {

        ResourceIterator<TypeVertex> from();

        ResourceIterator<TypeVertex> to();

        ResourceIterator<TypeVertex> overridden();
    }

    interface RuleIteratorBuilder {

        ResourceIterator<RuleVertex> from();

        ResourceIterator<TypeVertex> to();
    }
}
