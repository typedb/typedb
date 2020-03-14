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

import java.util.Arrays;

public abstract class Vertex {

    private final GraphManager graph;
    private final Schema.Status status;
    private final Schema.Vertex schema;
    private final int hash;

    private byte[] iid;

    Vertex(GraphManager graph, Schema.Status status, Schema.Vertex schema, byte[] iid) {
        this.graph = graph;
        this.status = status;
        this.schema = schema;
        this.iid = iid;
        this.hash = Arrays.hashCode(iid);
    }

    public Schema.Status status() {
        return status;
    }

    public Schema.Vertex schema() {
        return schema;
    }

    public byte[] iid() {
        return iid;
    }

    public void iid(byte[] iid) {
        this.iid = iid;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        return Arrays.equals(iid, ((Vertex) object).iid);
    }

    @Override
    public final int hashCode() {
        return hash;
    }
}
