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

package hypergraph.graph.edge;

import hypergraph.graph.Schema;
import hypergraph.graph.vertex.Vertex;

import java.util.Arrays;
import java.util.Objects;

public abstract class Edge<EDGE_SCHEMA extends Schema.Edge, VERTEX extends Vertex> {

    protected final byte[] iid;
    protected final EDGE_SCHEMA schema;
    protected VERTEX from;
    protected VERTEX to;

    public Edge(byte[] iid, EDGE_SCHEMA schema, VERTEX from, VERTEX to) {
        this.iid = iid;
        this.schema = schema;
        this.from = from;
        this.to = to;
    }

    public EDGE_SCHEMA schema() {
        return schema;
    }

    public abstract Schema.Status status();

    public abstract VERTEX from();

    public abstract VERTEX to();

    public abstract void commit();

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        Edge that = (Edge) object;
        return (Arrays.equals(this.iid, that.iid) &&
                this.schema.equals(that.schema) &&
                this.from.equals(that.from) &&
                this.to.equals(that.to));
    }

    @Override
    public final int hashCode() {
        return Objects.hash(Arrays.hashCode(iid), schema, from, to);
    }

}
