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

package hypergraph.concept.type.impl;

import hypergraph.common.exception.HypergraphException;
import hypergraph.common.iterator.Iterators;
import hypergraph.concept.type.Type;
import hypergraph.graph.TypeGraph;
import hypergraph.graph.util.Schema;
import hypergraph.graph.vertex.TypeVertex;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.Objects;

public abstract class TypeImpl implements Type {

    protected final TypeVertex vertex;

    protected TypeImpl(TypeVertex vertex) {
        this.vertex = Objects.requireNonNull(vertex);
    }

    TypeImpl(TypeGraph graph, String label, Schema.Vertex.Type schema) {
        this(graph, label, schema, null);
    }

    TypeImpl(TypeGraph graph, String label, Schema.Vertex.Type schema, String scope) {
        this.vertex = graph.create(schema, label, scope);
        TypeVertex superTypeVertex = graph.get(schema.root().label(), schema.root().scope());
        vertex.outs().put(Schema.Edge.Type.SUB, superTypeVertex);
    }

    @Override
    public byte[] iid() { return vertex.iid().bytes(); }

    @Override
    public boolean isRoot() { return false; }

    @Override
    public Long count() {
        return 0L; // TODO: return total number of type instances
    }

    @Override
    public void label(String label) {
        vertex.label(label);
    }

    @Override
    public String label() {
        return vertex.label();
    }

    @Override
    public boolean isAbstract() {
        return vertex.isAbstract();
    }

    protected void superTypeVertex(TypeVertex superTypeVertex) {
        vertex.outs().delete(Schema.Edge.Type.SUB, superTypeVertex());
        vertex.outs().put(Schema.Edge.Type.SUB, superTypeVertex);
    }

    @Nullable
    protected TypeVertex superTypeVertex() {
        Iterator<TypeVertex> iterator = Iterators.filter(vertex.outs().edge(Schema.Edge.Type.SUB).to(),
                                                         v -> v.schema().equals(vertex.schema()));
        if (iterator.hasNext()) return iterator.next();
        else return null;
    }

    protected Iterator<TypeVertex> superTypeVertices() {
        return Iterators.loop(
                vertex,
                v -> v != null && v.schema().equals(this.vertex.schema()),
                v -> {
                    Iterator<TypeVertex> p = v.outs().edge(Schema.Edge.Type.SUB).to();
                    if (p.hasNext()) return p.next();
                    else return null;
                });
    }

    protected Iterator<TypeVertex> subTypeVertices() {
        return Iterators.tree(vertex, v -> v.ins().edge(Schema.Edge.Type.SUB).from());
    }

    @Override
    public void delete() {
        // TODO: Check if a type has any intances too
        if (subs().findAny().isPresent()) {
            vertex.delete();
        } else {
            throw new HypergraphException("Invalid RoleType Removal: " + label() + " has subtypes");
        }
    }

    @Override
    public String toString() {
        return this.getClass().getCanonicalName() + " {" + vertex.toString() + "}";
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        TypeImpl that = (TypeImpl) object;
        return this.vertex.equals(that.vertex);
    }

    @Override
    public final int hashCode() {
        return vertex.hashCode();
    }
}
