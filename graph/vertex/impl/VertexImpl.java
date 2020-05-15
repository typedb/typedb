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

package hypergraph.graph.vertex.impl;

import hypergraph.graph.edge.Edge;
import hypergraph.graph.util.Schema;
import hypergraph.graph.vertex.Vertex;

import java.util.Arrays;

public abstract class VertexImpl<
        VERTEX_SCHEMA extends Schema.Vertex,
        VERTEX extends Vertex<VERTEX_SCHEMA, VERTEX, EDGE_SCHEMA, EDGE>,
        EDGE_SCHEMA extends Schema.Edge,
        EDGE extends Edge<EDGE_SCHEMA, VERTEX>> implements Vertex<VERTEX_SCHEMA, VERTEX, EDGE_SCHEMA, EDGE> {

    protected final VERTEX_SCHEMA schema;

    protected byte[] iid;

    VertexImpl(byte[] iid, VERTEX_SCHEMA schema) {
        this.schema = schema;
        this.iid = iid;
    }

    @Override
    public VERTEX_SCHEMA schema() {
        return schema;
    }

    @Override
    public byte[] iid() {
        return iid;
    }

    @Override
    public void iid(byte[] iid) {
        this.iid = iid;
    }

    @Override
    public String toString() {
        return this.getClass().getCanonicalName() + ": [" + schema + "] " + Arrays.toString(iid);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        VertexImpl that = (VertexImpl) object;
        return Arrays.equals(this.iid, that.iid);
    }

    @Override
    public final int hashCode() {
        return Arrays.hashCode(iid);
    }
}
