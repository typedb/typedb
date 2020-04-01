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

package hypergraph.concept.type;

import hypergraph.common.exception.HypergraphException;
import hypergraph.common.iterator.Iterators;
import hypergraph.graph.Graph;
import hypergraph.graph.Schema;
import hypergraph.graph.vertex.TypeVertex;

import java.util.Iterator;
import java.util.Objects;
import java.util.stream.Stream;

import static java.util.Spliterator.IMMUTABLE;
import static java.util.Spliterator.ORDERED;
import static java.util.Spliterators.spliteratorUnknownSize;
import static java.util.stream.StreamSupport.stream;

public abstract class Type<TYPE extends Type> {

    protected final TypeVertex vertex;
    protected TYPE parent;

    Type(TypeVertex vertex) {
        this.vertex = Objects.requireNonNull(vertex);
    }

    Type(Graph graph, String label, Schema.Vertex.Type schema) {
        this.vertex = graph.type().putVertex(schema, label);
        TypeVertex parentVertex = graph.type().getVertex(schema.root().label());
        vertex.outs().add(Schema.Edge.Type.SUB, parentVertex);
        parent = newInstance(parentVertex);
    }

    abstract TYPE newInstance(TypeVertex vertex);

    abstract TYPE getThis();

    public TYPE label(String label) {
        vertex.label(label);
        return getThis();
    }

    public TYPE setAbstract(boolean isAbstract) {
        vertex.setAbstract(isAbstract);
        return getThis();
    }

    public TYPE sup(TYPE parent) {
        vertex.outs().remove(Schema.Edge.Type.SUB, sup().vertex);
        vertex.outs().add(Schema.Edge.Type.SUB, parent.vertex);
        this.parent = parent;
        return getThis();
    }

    public TYPE sup() {
        if (parent != null) return parent;

        Iterator<TypeVertex> iterator = Iterators.filter(vertex.outs().get(Schema.Edge.Type.SUB),
                                                         v -> v.schema().equals(vertex.schema()));
        if (iterator.hasNext()) parent = newInstance(iterator.next());
        return parent;
    }

    public Stream<TYPE> sups() {
        Iterator<TYPE> sups = Iterators.loop(
                vertex,
                v -> v != null && v.schema().equals(this.vertex.schema()),
                v -> {
                    Iterator<TypeVertex> p = v.outs().get(Schema.Edge.Type.SUB);
                    if (p.hasNext()) return p.next();
                    else return null;
                }).apply(this::newInstance);

        return stream(spliteratorUnknownSize(sups, ORDERED | IMMUTABLE), false);
    }

    public String label() {
        return vertex.label();
    }

    public boolean isAbstract() {
        return vertex.isAbstract();
    }

    public Stream<? extends Type> subs() {
        Iterator<Type> sups = Iterators.tree(vertex, v -> v.ins().get(Schema.Edge.Type.SUB)).apply(this::newInstance);
        return stream(spliteratorUnknownSize(sups, ORDERED), false);
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + " {" + vertex.toString() + "}";
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        Type that = (Type) object;
        return this.vertex.equals(that.vertex);
    }

    @Override
    public final int hashCode() {
        return vertex.hashCode();
    }

    public static class Root extends Type {

        public Root(TypeVertex vertex) {
            super(vertex);
            assert(vertex.label().equals(Schema.Vertex.Type.Root.THING.label()));
        }

        @Override
        Type newInstance(TypeVertex vertex) {
            if (vertex.schema().equals(Schema.Vertex.Type.ATTRIBUTE_TYPE)) return AttributeType.of(vertex);
            if (vertex.schema().equals(Schema.Vertex.Type.ENTITY_TYPE)) return EntityType.of(vertex);
            if (vertex.schema().equals(Schema.Vertex.Type.RELATION_TYPE)) return RelationType.of(vertex);
            if (vertex.schema().equals(Schema.Vertex.Type.ROLE_TYPE)) return RoleType.of(vertex);
            return null;
        }

        @Override
        Type getThis() {
            return this;
        }

        @Override
        public Type label(String label) {
            throw new HypergraphException("Invalid Operation Exception: root types are immutable");
        }

        @Override
        public Type setAbstract(boolean isAbstract) {
            throw new HypergraphException("Invalid Operation Exception: root types are immutable");
        }

        @Override
        public Type sup() {
            return null;
        }

        @Override
        public Type sup(Type superType) {
            throw new HypergraphException("Invalid Operation Exception: root types are immutable");
        }
    }
}
