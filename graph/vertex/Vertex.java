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
import hypergraph.graph.Storage;
import hypergraph.graph.edge.Edge;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Spliterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

public abstract class Vertex<
        VERTEX_SCHEMA extends Schema.Vertex,
        EDGE_SCHEMA extends Schema.Edge,
        EDGE extends Edge<EDGE_SCHEMA, ? extends Vertex<VERTEX_SCHEMA, EDGE_SCHEMA, EDGE>>> {

    protected final Storage storage;
    protected final VERTEX_SCHEMA schema;
    private final int hash;

    protected final Map<EDGE_SCHEMA, Set<EDGE>> outs;
    protected final Map<EDGE_SCHEMA, Set<EDGE>> ins;

    protected byte[] iid;

    Vertex(Storage storage, VERTEX_SCHEMA schema, byte[] iid) {
        this.storage = storage;
        this.schema = schema;
        this.iid = iid;
        hash = Arrays.hashCode(iid);
        outs = new ConcurrentHashMap<>();
        ins = new ConcurrentHashMap<>();
    }

    public abstract Schema.Status status();

    public VERTEX_SCHEMA schema() {
        return schema;
    }

    public abstract void commit();

    public abstract Spliterator<EDGE> outs(EDGE_SCHEMA schema);

    public abstract Spliterator<EDGE> ins(EDGE_SCHEMA schema);

    public void out(EDGE edge) {
        outs.putIfAbsent(edge.schema(), new HashSet<>());
        outs.get(edge.schema()).add(edge);
    }

    public void in(EDGE edge) {
        ins.putIfAbsent(edge.schema(), new HashSet<>());
        ins.get(edge.schema()).add(edge);
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
