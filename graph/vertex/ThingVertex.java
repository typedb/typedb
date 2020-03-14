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
import hypergraph.graph.KeyGenerator;
import hypergraph.graph.Schema;

import java.nio.ByteBuffer;

public abstract class ThingVertex extends Vertex {

    ThingVertex(GraphManager graph, Schema.Status status, Schema.Vertex.Thing schema, byte[] iid) {
        super(graph, status, schema, iid);
    }

    @Override
    public Schema.Vertex.Thing schema() {
        return (Schema.Vertex.Thing) super.schema();
    }

    public static byte[] generateIID(KeyGenerator keyGenerator, Schema.Vertex.Thing schema) {
        return null; // TODO
    }

}
