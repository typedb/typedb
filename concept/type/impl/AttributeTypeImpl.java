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

import hypergraph.common.exception.Error;
import hypergraph.common.exception.HypergraphException;
import hypergraph.common.iterator.Iterators;
import hypergraph.concept.type.AttributeType;
import hypergraph.graph.util.Schema;
import hypergraph.graph.TypeGraph;
import hypergraph.graph.vertex.TypeVertexImpl;

import javax.annotation.Nullable;
import java.time.LocalDateTime;
import java.util.Iterator;
import java.util.stream.Stream;

import static hypergraph.common.exception.Error.TypeDefinition.INVALID_ROOT_TYPE_MUTATION;
import static hypergraph.common.exception.Error.ConceptRetrieval.INVALID_CONCEPT_CASTING;
import static java.util.Spliterator.IMMUTABLE;
import static java.util.Spliterator.ORDERED;
import static java.util.Spliterators.spliteratorUnknownSize;
import static java.util.stream.StreamSupport.stream;

public abstract class AttributeTypeImpl extends ThingTypeImpl implements AttributeType {

    private AttributeTypeImpl(TypeVertexImpl vertex) {
        super(vertex);
        if (vertex.schema() != Schema.Vertex.Type.ATTRIBUTE_TYPE) {
            throw new HypergraphException("Invalid Attribute Type: " + vertex.label() +
                                                  " subtypes " + vertex.schema().root().label());
        }
    }

    private AttributeTypeImpl(TypeGraph graph, java.lang.String label, Class<?> valueClass) {
        super(graph, label, Schema.Vertex.Type.ATTRIBUTE_TYPE);
        vertex.valueClass(Schema.ValueClass.of(valueClass));
    }

    public static AttributeTypeImpl of(TypeVertexImpl vertex) {
        switch (vertex.valueClass()) {
            case OBJECT:
                return new AttributeTypeImpl.Root(vertex);
            case BOOLEAN:
                return AttributeTypeImpl.Boolean.of(vertex);
            case LONG:
                return AttributeTypeImpl.Long.of(vertex);
            case DOUBLE:
                return AttributeTypeImpl.Double.of(vertex);
            case STRING:
                return AttributeTypeImpl.String.of(vertex);
            case DATETIME:
                return AttributeTypeImpl.DateTime.of(vertex);
            default:
                throw new HypergraphException("Unreachable"); // unreachable
        }
    }

    @Override
    public AttributeTypeImpl.Root asObject() {
        if (this.valueClass().equals(java.lang.Object.class)) return new AttributeTypeImpl.Root(this.vertex);
        else throw new HypergraphException(INVALID_CONCEPT_CASTING.format(AttributeType.class.getCanonicalName()));
    }

    @Override
    public AttributeTypeImpl.Boolean asBoolean() {
        throw new HypergraphException(INVALID_CONCEPT_CASTING.format(AttributeType.Boolean.class.getCanonicalName()));
    }

    @Override
    public AttributeTypeImpl.Long asLong() {
        throw new HypergraphException(INVALID_CONCEPT_CASTING.format(AttributeType.Long.class.getCanonicalName()));
    }

    @Override
    public AttributeTypeImpl.Double asDouble() {
        throw new HypergraphException(INVALID_CONCEPT_CASTING.format(AttributeType.Double.class.getCanonicalName()));
    }

    @Override
    public AttributeTypeImpl.String asString() {
        throw new HypergraphException(INVALID_CONCEPT_CASTING.format(AttributeType.Long.class.getCanonicalName()));
    }

    @Override
    public AttributeTypeImpl.DateTime asDateTime() {
        throw new HypergraphException(INVALID_CONCEPT_CASTING.format(AttributeType.DateTime.class.getCanonicalName()));
    }

    @Nullable
    @Override
    public abstract AttributeTypeImpl sup();

    @Override
    public abstract Stream<? extends AttributeTypeImpl> sups();

    @Override
    public abstract Stream<? extends AttributeTypeImpl> subs();

    protected Iterator<TypeVertexImpl> subTypeVertices(Schema.ValueClass valueClass) {
        return Iterators.tree(vertex, v -> Iterators.filter(v.ins().edge(Schema.Edge.Type.SUB).from(),
                                                            sv -> sv.valueClass().equals(valueClass)));
    }

    @Override
    public void sup(AttributeType superType) {
        if (!superType.isRoot() && !this.valueClass().equals(superType.valueClass())) {
            throw new HypergraphException(Error.TypeDefinition.INVALID_SUPERTYPE_VALUE_CLASS.format(
                    this.label(), this.valueClass().getSimpleName(),
                    superType.label(), superType.valueClass().getSimpleName())
            );
        }
        super.superTypeVertex(((AttributeTypeImpl) superType).vertex);
    }

    @Override
    public Class<?> valueClass() {
        return Object.class;
    }

    @Override
    public boolean equals(java.lang.Object object) {
        if (this == object) return true;
        if (!(object instanceof AttributeTypeImpl)) return false;
        AttributeTypeImpl that = (AttributeTypeImpl) object;
        return this.vertex.equals(that.vertex);
    }

    private static class Root extends AttributeTypeImpl {

        private Root(TypeVertexImpl vertex) {
            super(vertex);
            assert vertex.valueClass().equals(Schema.ValueClass.OBJECT);
            assert vertex.label().equals(Schema.Vertex.Type.Root.ATTRIBUTE.label());
        }

        public Class<java.lang.Object> valueClass() { return java.lang.Object.class; }

        @Override
        public AttributeTypeImpl.Root asObject() { return this; }

        @Override
        public AttributeTypeImpl.Boolean asBoolean() { return AttributeTypeImpl.Boolean.of(this.vertex); }

        @Override
        public AttributeTypeImpl.Long asLong() { return AttributeTypeImpl.Long.of(this.vertex); }

        @Override
        public AttributeTypeImpl.Double asDouble() { return AttributeTypeImpl.Double.of(this.vertex); }

        @Override
        public AttributeTypeImpl.String asString() { return AttributeTypeImpl.String.of(this.vertex); }

        @Override
        public AttributeTypeImpl.DateTime asDateTime() { return AttributeTypeImpl.DateTime.of(this.vertex); }

        @Override
        public boolean isRoot() { return true; }

        @Override
        public void label(java.lang.String label) { throw new HypergraphException(INVALID_ROOT_TYPE_MUTATION); }

        @Override
        public void isAbstract(boolean isAbstract) { throw new HypergraphException(INVALID_ROOT_TYPE_MUTATION); }

        @Override
        public void sup(AttributeType superType) { throw new HypergraphException(INVALID_ROOT_TYPE_MUTATION); }

        @Nullable
        @Override
        public AttributeTypeImpl sup() {
            return null;
        }

        @Override
        public Stream<? extends AttributeTypeImpl> sups() {
            return Stream.of(this);
        }

        @Override
        public Stream<? extends AttributeTypeImpl> subs() {
            return stream(spliteratorUnknownSize(super.subTypeVertices(), ORDERED | IMMUTABLE), false).map(v -> {
                switch (v.valueClass()) {
                    case OBJECT:
                        assert this.vertex == v;
                        return this;
                    case BOOLEAN:
                        return AttributeTypeImpl.Boolean.of(v);
                    case LONG:
                        return AttributeTypeImpl.Long.of(v);
                    case DOUBLE:
                        return AttributeTypeImpl.Double.of(v);
                    case STRING:
                        return AttributeTypeImpl.String.of(v);
                    case DATETIME:
                        return AttributeTypeImpl.DateTime.of(v);
                    default:
                        throw new HypergraphException("Unreachable");
                }
            });
        }
    }

    public static class Boolean extends AttributeTypeImpl implements AttributeType.Boolean {

        public Boolean(TypeGraph graph, java.lang.String label) {
            super(graph, label, java.lang.Boolean.class);
        }

        private Boolean(TypeVertexImpl vertex) {
            super(vertex);
            assert (vertex.label().equals(Schema.Vertex.Type.Root.ATTRIBUTE.label()) ||
                    vertex.valueClass().equals(Schema.ValueClass.BOOLEAN));
        }

        public static AttributeTypeImpl.Boolean of(TypeVertexImpl vertex) {
            return vertex.label().equals(Schema.Vertex.Type.Root.ATTRIBUTE.label()) ?
                    new Root(vertex) :
                    new AttributeTypeImpl.Boolean(vertex);
        }

        @Override
        public AttributeTypeImpl.Boolean asBoolean() { return this; }

        @Nullable
        @Override
        public AttributeTypeImpl.Boolean sup() {
            TypeVertexImpl vertex = super.superTypeVertex();
            return vertex != null ? of(vertex) : null;
        }

        @Override
        public Stream<AttributeTypeImpl.Boolean> sups() {
            Iterator<AttributeTypeImpl.Boolean> sups = Iterators.apply(super.superTypeVertices(), AttributeTypeImpl.Boolean::of);
            return stream(spliteratorUnknownSize(sups, ORDERED | IMMUTABLE), false);
        }

        @Override
        public Stream<AttributeTypeImpl.Boolean> subs() {
            Iterator<AttributeTypeImpl.Boolean> subs = Iterators.apply(super.subTypeVertices(), AttributeTypeImpl.Boolean::of);
            return stream(spliteratorUnknownSize(subs, ORDERED | IMMUTABLE), false);
        }

        @Override
        public Class<java.lang.Boolean> valueClass() { return java.lang.Boolean.class; }

        private static class Root extends AttributeTypeImpl.Boolean {

            private Root(TypeVertexImpl vertex) {
                super(vertex);
                assert vertex.label().equals(Schema.Vertex.Type.Root.ATTRIBUTE.label());
            }

            @Override
            public boolean isRoot() { return true; }

            @Override
            public void label(java.lang.String label) { throw new HypergraphException(INVALID_ROOT_TYPE_MUTATION); }

            @Override
            public void isAbstract(boolean isAbstract) { throw new HypergraphException(INVALID_ROOT_TYPE_MUTATION); }

            @Override
            public void sup(AttributeType superType) { throw new HypergraphException(INVALID_ROOT_TYPE_MUTATION); }

            @Override
            public Stream<AttributeTypeImpl.Boolean> subs() {
                Iterator<AttributeTypeImpl.Boolean> sups = Iterators.apply(
                        super.subTypeVertices(Schema.ValueClass.BOOLEAN),
                        AttributeTypeImpl.Boolean::of
                );
                return stream(spliteratorUnknownSize(sups, ORDERED | IMMUTABLE), false);
            }
        }
    }

    public static class Long extends AttributeTypeImpl implements AttributeType.Long {

        public Long(TypeGraph graph, java.lang.String label) {
            super(graph, label, java.lang.Long.class);
        }

        private Long(TypeVertexImpl vertex) {
            super(vertex);
            assert (vertex.label().equals(Schema.Vertex.Type.Root.ATTRIBUTE.label()) ||
                    vertex.valueClass().equals(Schema.ValueClass.LONG));
        }

        public static AttributeTypeImpl.Long of(TypeVertexImpl vertex) {
            return vertex.label().equals(Schema.Vertex.Type.Root.ATTRIBUTE.label()) ?
                    new Root(vertex) :
                    new AttributeTypeImpl.Long(vertex);
        }

        @Override
        public AttributeTypeImpl.Long asLong() { return this; }

        @Nullable
        @Override
        public AttributeTypeImpl.Long sup() {
            TypeVertexImpl vertex = super.superTypeVertex();
            return vertex != null ? of(vertex) : null;
        }

        @Override
        public Stream<AttributeTypeImpl.Long> sups() {
            Iterator<AttributeTypeImpl.Long> sups = Iterators.apply(super.superTypeVertices(), AttributeTypeImpl.Long::of);
            return stream(spliteratorUnknownSize(sups, ORDERED | IMMUTABLE), false);
        }

        @Override
        public Stream<AttributeTypeImpl.Long> subs() {
            Iterator<AttributeTypeImpl.Long> subs = Iterators.apply(super.subTypeVertices(), AttributeTypeImpl.Long::of);
            return stream(spliteratorUnknownSize(subs, ORDERED | IMMUTABLE), false);
        }

        @Override
        public Class<java.lang.Long> valueClass() {
            return java.lang.Long.class;
        }

        private static class Root extends AttributeTypeImpl.Long {

            private Root(TypeVertexImpl vertex) {
                super(vertex);
                assert vertex.label().equals(Schema.Vertex.Type.Root.ATTRIBUTE.label());
            }

            @Override
            public boolean isRoot() { return true; }

            @Override
            public void label(java.lang.String label) { throw new HypergraphException(INVALID_ROOT_TYPE_MUTATION); }

            @Override
            public void isAbstract(boolean isAbstract) { throw new HypergraphException(INVALID_ROOT_TYPE_MUTATION); }

            @Override
            public void sup(AttributeType superType) { throw new HypergraphException(INVALID_ROOT_TYPE_MUTATION); }

            @Override
            public Stream<AttributeTypeImpl.Long> subs() {
                Iterator<AttributeTypeImpl.Long> sups = Iterators.apply(
                        super.subTypeVertices(Schema.ValueClass.LONG),
                        AttributeTypeImpl.Long::of
                );
                return stream(spliteratorUnknownSize(sups, ORDERED | IMMUTABLE), false);
            }
        }
    }

    public static class Double extends AttributeTypeImpl implements AttributeType.Double {

        public Double(TypeGraph graph, java.lang.String label) {
            super(graph, label, java.lang.Double.class);
        }

        private Double(TypeVertexImpl vertex) {
            super(vertex);
            assert (vertex.label().equals(Schema.Vertex.Type.Root.ATTRIBUTE.label()) ||
                    vertex.valueClass().equals(Schema.ValueClass.DOUBLE));
        }

        public static AttributeTypeImpl.Double of(TypeVertexImpl vertex) {
            return vertex.label().equals(Schema.Vertex.Type.Root.ATTRIBUTE.label()) ?
                    new Root(vertex) :
                    new AttributeTypeImpl.Double(vertex);
        }

        @Override
        public AttributeTypeImpl.Double asDouble() { return this; }

        @Nullable
        @Override
        public AttributeTypeImpl.Double sup() {
            TypeVertexImpl vertex = super.superTypeVertex();
            return vertex != null ? of(vertex) : null;
        }

        @Override
        public Stream<AttributeTypeImpl.Double> sups() {
            Iterator<AttributeTypeImpl.Double> sups = Iterators.apply(super.superTypeVertices(), AttributeTypeImpl.Double::of);
            return stream(spliteratorUnknownSize(sups, ORDERED | IMMUTABLE), false);
        }

        @Override
        public Stream<AttributeTypeImpl.Double> subs() {
            Iterator<AttributeTypeImpl.Double> subs = Iterators.apply(super.subTypeVertices(), AttributeTypeImpl.Double::of);
            return stream(spliteratorUnknownSize(subs, ORDERED | IMMUTABLE), false);
        }

        @Override
        public Class<java.lang.Double> valueClass() {
            return java.lang.Double.class;
        }

        private static class Root extends AttributeTypeImpl.Double {

            private Root(TypeVertexImpl vertex) {
                super(vertex);
                assert vertex.label().equals(Schema.Vertex.Type.Root.ATTRIBUTE.label());
            }

            @Override
            public boolean isRoot() { return true; }

            @Override
            public void label(java.lang.String label) { throw new HypergraphException(INVALID_ROOT_TYPE_MUTATION); }

            @Override
            public void isAbstract(boolean isAbstract) { throw new HypergraphException(INVALID_ROOT_TYPE_MUTATION); }

            @Override
            public void sup(AttributeType superType) { throw new HypergraphException(INVALID_ROOT_TYPE_MUTATION); }

            @Override
            public Stream<AttributeTypeImpl.Double> subs() {
                Iterator<AttributeTypeImpl.Double> sups = Iterators.apply(
                        super.subTypeVertices(Schema.ValueClass.DOUBLE),
                        AttributeTypeImpl.Double::of
                );
                return stream(spliteratorUnknownSize(sups, ORDERED | IMMUTABLE), false);
            }
        }
    }

    public static class String extends AttributeTypeImpl implements AttributeType.String {

        public String(TypeGraph graph, java.lang.String label) {
            super(graph, label, java.lang.String.class);
        }

        private String(TypeVertexImpl vertex) {
            super(vertex);
            assert (vertex.label().equals(Schema.Vertex.Type.Root.ATTRIBUTE.label()) ||
                    vertex.valueClass().equals(Schema.ValueClass.STRING));
        }

        public static AttributeTypeImpl.String of(TypeVertexImpl vertex) {
            return vertex.label().equals(Schema.Vertex.Type.Root.ATTRIBUTE.label()) ?
                    new Root(vertex) :
                    new AttributeTypeImpl.String(vertex);
        }

        @Override
        public AttributeTypeImpl.String asString() { return this; }

        @Nullable
        @Override
        public AttributeTypeImpl.String sup() {
            TypeVertexImpl vertex = super.superTypeVertex();
            return vertex != null ? of(vertex) : null;
        }

        @Override
        public Stream<AttributeTypeImpl.String> sups() {
            Iterator<AttributeTypeImpl.String> sups = Iterators.apply(super.superTypeVertices(), AttributeTypeImpl.String::of);
            return stream(spliteratorUnknownSize(sups, ORDERED | IMMUTABLE), false);
        }

        @Override
        public Stream<AttributeTypeImpl.String> subs() {
            Iterator<AttributeTypeImpl.String> subs = Iterators.apply(super.subTypeVertices(), AttributeTypeImpl.String::of);
            return stream(spliteratorUnknownSize(subs, ORDERED | IMMUTABLE), false);
        }

        @Override
        public Class<java.lang.String> valueClass() {
            return java.lang.String.class;
        }

        private static class Root extends AttributeTypeImpl.String {

            private Root(TypeVertexImpl vertex) {
                super(vertex);
                assert vertex.label().equals(Schema.Vertex.Type.Root.ATTRIBUTE.label());
            }

            @Override
            public boolean isRoot() { return true; }

            @Override
            public void label(java.lang.String label) { throw new HypergraphException(INVALID_ROOT_TYPE_MUTATION); }

            @Override
            public void isAbstract(boolean isAbstract) { throw new HypergraphException(INVALID_ROOT_TYPE_MUTATION); }

            @Override
            public void sup(AttributeType superType) { throw new HypergraphException(INVALID_ROOT_TYPE_MUTATION); }

            @Override
            public Stream<AttributeTypeImpl.String> subs() {
                Iterator<AttributeTypeImpl.String> sups = Iterators.apply(
                        super.subTypeVertices(Schema.ValueClass.STRING),
                        AttributeTypeImpl.String::of
                );
                return stream(spliteratorUnknownSize(sups, ORDERED | IMMUTABLE), false);
            }
        }
    }

    public static class DateTime extends AttributeTypeImpl implements AttributeType.DateTime {

        public DateTime(TypeGraph graph, java.lang.String label) {
            super(graph, label, LocalDateTime.class);
        }

        private DateTime(TypeVertexImpl vertex) {
            super(vertex);
            assert (vertex.label().equals(Schema.Vertex.Type.Root.ATTRIBUTE.label()) ||
                    vertex.valueClass().equals(Schema.ValueClass.DATETIME));
        }

        public static AttributeTypeImpl.DateTime of(TypeVertexImpl vertex) {
            return vertex.label().equals(Schema.Vertex.Type.Root.ATTRIBUTE.label()) ?
                    new Root(vertex) :
                    new AttributeTypeImpl.DateTime(vertex);
        }

        @Override
        public AttributeTypeImpl.DateTime asDateTime() { return this; }

        @Nullable
        @Override
        public AttributeTypeImpl.DateTime sup() {
            TypeVertexImpl vertex = super.superTypeVertex();
            return vertex != null ? of(vertex) : null;
        }

        @Override
        public Stream<AttributeTypeImpl.DateTime> sups() {
            Iterator<AttributeTypeImpl.DateTime> sups = Iterators.apply(super.superTypeVertices(), AttributeTypeImpl.DateTime::of);
            return stream(spliteratorUnknownSize(sups, ORDERED | IMMUTABLE), false);
        }

        @Override
        public Stream<AttributeTypeImpl.DateTime> subs() {
            Iterator<AttributeTypeImpl.DateTime> subs = Iterators.apply(super.subTypeVertices(), AttributeTypeImpl.DateTime::of);
            return stream(spliteratorUnknownSize(subs, ORDERED | IMMUTABLE), false);
        }

        @Override
        public Class<LocalDateTime> valueClass() {
            return LocalDateTime.class;
        }

        private static class Root extends AttributeTypeImpl.DateTime {

            private Root(TypeVertexImpl vertex) {
                super(vertex);
                assert vertex.label().equals(Schema.Vertex.Type.Root.ATTRIBUTE.label());
            }

            @Override
            public boolean isRoot() { return true; }

            @Override
            public void label(java.lang.String label) { throw new HypergraphException(INVALID_ROOT_TYPE_MUTATION); }

            @Override
            public void isAbstract(boolean isAbstract) { throw new HypergraphException(INVALID_ROOT_TYPE_MUTATION); }

            @Override
            public void sup(AttributeType superType) { throw new HypergraphException(INVALID_ROOT_TYPE_MUTATION); }

            @Override
            public Stream<AttributeTypeImpl.DateTime> subs() {
                Iterator<AttributeTypeImpl.DateTime> sups = Iterators.apply(
                        super.subTypeVertices(Schema.ValueClass.DATETIME),
                        AttributeTypeImpl.DateTime::of
                );
                return stream(spliteratorUnknownSize(sups, ORDERED | IMMUTABLE), false);
            }
        }
    }
}
