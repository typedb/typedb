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

import hypergraph.graph.Graph;
import hypergraph.graph.Schema;
import hypergraph.common.iterator.Iterators;
import hypergraph.graph.vertex.TypeVertex;

import java.util.Iterator;
import java.util.Objects;
import java.util.stream.Stream;

import static java.util.Spliterator.IMMUTABLE;
import static java.util.Spliterator.ORDERED;
import static java.util.Spliterators.spliteratorUnknownSize;
import static java.util.stream.StreamSupport.stream;

public abstract class Type {

    protected final TypeVertex vertex;

    public Type(TypeVertex vertex) {
        this.vertex = Objects.requireNonNull(vertex);
    }

    Type newInstance(TypeVertex vertex) {
        if (vertex.schema().equals(Schema.Vertex.Type.ATTRIBUTE_TYPE)) return new AttributeType(vertex);
        if (vertex.schema().equals(Schema.Vertex.Type.ENTITY_TYPE)) return new EntityType(vertex);
        if (vertex.schema().equals(Schema.Vertex.Type.RELATION_TYPE)) return new RelationType(vertex);
        if (vertex.schema().equals(Schema.Vertex.Type.ROLE_TYPE)) return new RoleType(vertex);
        return null;
    }

    public String label() {
        return vertex.label();
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
        }
    }

    public static abstract class Tree<TYPE extends Tree> extends Type {

        protected TYPE parent;

        Tree(TypeVertex vertex) {
            super(vertex);
        }

        Tree(Graph graph, String label, Schema.Vertex.Type schema) {
            super(graph.type().createVertex(schema, label));
            TypeVertex parentVertex = graph.type().getVertex(schema.root().label());
            vertex.outs().add(Schema.Edge.Type.SUB, parentVertex);
            parent = newInstance(parentVertex);
        }

        @Override
        abstract TYPE newInstance(TypeVertex vertex);

        abstract TYPE getThis();

        public TYPE sup(TYPE parent) {
            vertex.outs().remove(Schema.Edge.Type.SUB, sup().vertex);
            vertex.outs().add(Schema.Edge.Type.SUB, parent.vertex);
            this.parent = parent;
            return getThis();
        }

        public TYPE sup() {
            if (parent != null) return parent;

            Iterator<TypeVertex> iterator = vertex.outs().get(Schema.Edge.Type.SUB);
            if (iterator != null && iterator.hasNext()) {
                TypeVertex parentVertex = iterator.next();
                if (parentVertex.schema().equals(vertex.schema())) {
                    parent = newInstance(parentVertex);
                }
            }

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
    }
}
