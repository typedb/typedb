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

import hypergraph.graph.GraphManager;
import hypergraph.graph.Schema;

public abstract class Vertex {

    private final GraphManager graph;
    private final byte[] iid;

    private Schema.Status status;
    private Schema.Vertex schema;

    Vertex(GraphManager graph, Schema.Status status, Schema.Vertex schema, byte[] iid) {
        this.graph = graph;
        this.status = status;
        this.schema = schema;
        this.iid = iid;
    }

    public Schema.Status status() {
        return status;
    }

    public byte[] iid() {
        return iid;
    }

    public static TypeVertex.Buffered createBufferedTypeVertex(GraphManager graph, Schema.Vertex.Type type, byte[] iid, String label) {
        return new TypeVertex.Buffered(graph, type, iid, label);
    }
}
