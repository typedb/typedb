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

import hypergraph.graph.Graph;
import hypergraph.graph.adjacency.Adjacency;
import hypergraph.graph.edge.Edge;
import hypergraph.graph.util.IID;
import hypergraph.graph.util.Schema;

public interface Vertex<
        VERTEX_IID extends IID.Vertex,
        VERTEX_SCHEMA extends Schema.Vertex,
        VERTEX extends Vertex<VERTEX_IID, VERTEX_SCHEMA, VERTEX, EDGE_SCHEMA, EDGE>,
        EDGE_SCHEMA extends Schema.Edge,
        EDGE extends Edge<?, EDGE_SCHEMA, VERTEX>> {

    Graph<VERTEX_IID, VERTEX> graph();

    VERTEX_IID iid();

    void iid(VERTEX_IID iid);

    Schema.Status status();

    VERTEX_SCHEMA schema();

    Adjacency<EDGE_SCHEMA, EDGE, VERTEX> outs();

    Adjacency<EDGE_SCHEMA, EDGE, VERTEX> ins();

    void commit();

    void delete();
}
