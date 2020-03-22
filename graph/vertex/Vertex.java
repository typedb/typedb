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

import hypergraph.graph.Schema;
import hypergraph.graph.edge.Edge;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public abstract class Vertex<
        VERTEX_SCHEMA extends Schema.Vertex,
        EDGE_SCHEMA extends Schema.Edge,
        EDGE extends Edge<EDGE_SCHEMA, ? extends Vertex<VERTEX_SCHEMA, EDGE_SCHEMA, EDGE>>> {

    protected final VERTEX_SCHEMA schema;

    protected final Map<EDGE_SCHEMA, Set<EDGE>> outs;
    protected final Map<EDGE_SCHEMA, Set<EDGE>> ins;

    protected byte[] iid;

    Vertex(byte[] iid, VERTEX_SCHEMA schema) {
        this.schema = schema;
        this.iid = iid;
        outs = new ConcurrentHashMap<>();
        ins = new ConcurrentHashMap<>();
    }

    public abstract Schema.Status status();

    public VERTEX_SCHEMA schema() {
        return schema;
    }

    public abstract void commit();

    public abstract Iterator<EDGE> outs(EDGE_SCHEMA schema);

    public abstract Iterator<EDGE> ins(EDGE_SCHEMA schema);

    public void out(EDGE edge) {
        outs.computeIfAbsent(edge.schema(), e -> ConcurrentHashMap.newKeySet()).add(edge);
    }

    public void in(EDGE edge) {
        ins.computeIfAbsent(edge.schema(), e -> ConcurrentHashMap.newKeySet()).add(edge);
    }

    public byte[] iid() {
        return iid;
    }

    public void iid(byte[] iid) {
        this.iid = iid;
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + ": [" + schema + "] " + Arrays.toString(iid);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        return Arrays.equals(iid, ((Vertex) object).iid);
    }

    @Override
    public final int hashCode() {
        return Arrays.hashCode(iid);
    }
}
