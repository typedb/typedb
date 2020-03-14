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
import hypergraph.graph.edge.Edge;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public abstract class Vertex {

    private final GraphManager graph;
    private final Schema.Status status;
    private final Schema.Vertex schema;
    private final int hash;

    private final Map<Schema.Edge, Set<Edge>> outs;
    private final Map<Schema.Edge, Set<Edge>> ins;

    private byte[] iid;

    Vertex(GraphManager graph, Schema.Status status, Schema.Vertex schema, byte[] iid) {
        this.graph = graph;
        this.status = status;
        this.schema = schema;
        this.iid = iid;
        hash = Arrays.hashCode(iid);
        outs = new ConcurrentHashMap<>();
        ins = new ConcurrentHashMap<>();
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

    public Set<Edge> outs(Schema.Edge schema) {
        return outs.get(schema);
    }

    public void out(Edge edge) {
        outs.putIfAbsent(edge.schema(), Collections.synchronizedSet(new HashSet<>()));
        outs.get(edge.schema()).add(edge);
    }

    public Set<Edge> ins(Schema.Edge schema) {
        return ins.get(schema);
    }

    public void in(Edge edge) {
        ins.putIfAbsent(edge.schema(), Collections.synchronizedSet(new HashSet<>()));
        ins.get(edge.schema()).add(edge);
    }

    public abstract void persist();

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
