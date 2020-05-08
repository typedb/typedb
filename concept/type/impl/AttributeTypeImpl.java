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
import hypergraph.graph.Graph;
import hypergraph.graph.Schema;
import hypergraph.graph.vertex.TypeVertex;

import javax.annotation.Nullable;
import java.time.LocalDateTime;
import java.util.Iterator;
import java.util.stream.Stream;

import static hypergraph.common.exception.Error.TypeDefinition.INVALID_ROOT_TYPE_MUTATION;
import static hypergraph.common.exception.Error.TypeRetrieval.INVALID_TYPE_CASTING;
import static java.util.Spliterator.IMMUTABLE;
import static java.util.Spliterator.ORDERED;
import static java.util.Spliterators.spliteratorUnknownSize;
import static java.util.stream.StreamSupport.stream;

public class AttributeTypeImpl extends ThingTypeImpl implements AttributeType {

    private AttributeTypeImpl(TypeVertex vertex) {
        super(vertex);
        if (vertex.schema() != Schema.Vertex.Type.ATTRIBUTE_TYPE) {
            throw new HypergraphException("Invalid Attribute Type: " + vertex.label() +
                                                  " subtypes " + vertex.schema().root().label());
        }
    }

    private AttributeTypeImpl(Graph.Type graph, java.lang.String label, Class<?> valueClass) {
        super(graph, label, Schema.Vertex.Type.ATTRIBUTE_TYPE);
        vertex.valueClass(Schema.ValueClass.of(valueClass));
    }

    public static AttributeTypeImpl of(TypeVertex vertex) {
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
    public void sup(AttributeType superType) {
        validateSuperTypeValueClass(superType);
        super.superTypeVertex(((AttributeTypeImpl) superType).vertex);
    }

    @Nullable
    @Override
    public AttributeTypeImpl sup() {
        TypeVertex vertex = super.superTypeVertex();
        return vertex != null ? of(vertex) : null;
    }

    @Override
    public Stream<? extends AttributeTypeImpl> sups() {
        Iterator<AttributeTypeImpl> sups = Iterators.apply(super.superTypeVertices(), AttributeTypeImpl::of);
        return stream(spliteratorUnknownSize(sups, ORDERED | IMMUTABLE), false);
    }

    @Override
    public Stream<? extends AttributeTypeImpl> subs() {
        Iterator<AttributeTypeImpl> subs = Iterators.apply(super.subTypeVertices(), AttributeTypeImpl::of);
        return stream(spliteratorUnknownSize(subs, ORDERED | IMMUTABLE), false);
    }

    @Override
    public Class<?> valueClass() {
        return Object.class;
    }

    @Override
    public AttributeTypeImpl.Root asObject() {
        if (this.valueClass().equals(java.lang.Object.class)) {
            return new AttributeTypeImpl.Root(this.vertex);
        } else {
            throw new HypergraphException(INVALID_TYPE_CASTING.format(
                    this.label(), AttributeTypeImpl.Root.class.getCanonicalName()
            ));
        }
    }

    @Override
    public AttributeTypeImpl.Boolean asBoolean() {
        if (this.valueClass().equals(java.lang.Boolean.class) || this.isRoot()) {
            return AttributeTypeImpl.Boolean.of(this.vertex);
        } else {
            throw new HypergraphException(INVALID_TYPE_CASTING.format(
                    this.label(), AttributeTypeImpl.Boolean.class.getCanonicalName()
            ));
        }
    }

    @Override
    public AttributeTypeImpl.Long asLong() {
        if (this.valueClass().equals(java.lang.Long.class) || this.isRoot()) {
            return AttributeTypeImpl.Long.of(this.vertex);
        } else {
            throw new HypergraphException(INVALID_TYPE_CASTING.format(
                    this.label(), AttributeTypeImpl.Long.class.getCanonicalName()
            ));
        }
    }

    @Override
    public AttributeTypeImpl.Double asDouble() {
        if (this.valueClass().equals(java.lang.Double.class) || this.isRoot()) {
            return AttributeTypeImpl.Double.of(this.vertex);
        } else {
            throw new HypergraphException(INVALID_TYPE_CASTING.format(
                    this.label(), AttributeTypeImpl.Double.class.getCanonicalName()
            ));
        }
    }

    @Override
    public AttributeTypeImpl.String asString() {
        if (this.valueClass().equals(java.lang.String.class) || this.isRoot()) {
            return AttributeTypeImpl.String.of(this.vertex);
        } else {
            throw new HypergraphException(INVALID_TYPE_CASTING.format(
                    this.label(), AttributeTypeImpl.Long.class.getCanonicalName()
            ));
        }
    }

    @Override
    public AttributeTypeImpl.DateTime asDateTime() {
        if (this.valueClass().equals(LocalDateTime.class) || this.isRoot()) {
            return AttributeTypeImpl.DateTime.of(this.vertex);
        } else {
            throw new HypergraphException(INVALID_TYPE_CASTING.format(
                    this.label(), AttributeTypeImpl.DateTime.class.getCanonicalName()
            ));
        }
    }

    void validateSuperTypeValueClass(AttributeType superType) {
        if (!superType.isRoot() && !this.valueClass().equals(superType.valueClass())) {
            throw new HypergraphException(Error.TypeDefinition.INVALID_SUPERTYPE_VALUE_CLASS.format(
                    this.label(), this.valueClass().getSimpleName(),
                    superType.label(), superType.valueClass().getSimpleName())
            );
        }
    }

    @Override
    public boolean equals(java.lang.Object object) {
        if (this == object) return true;
        if (!(object instanceof AttributeTypeImpl)) return false;
        AttributeTypeImpl that = (AttributeTypeImpl) object;
        return this.vertex.equals(that.vertex);
    }

    private static class Root extends AttributeTypeImpl {

        private Root(TypeVertex vertex) {
            super(vertex);
            assert vertex.valueClass().equals(Schema.ValueClass.OBJECT);
            assert vertex.label().equals(Schema.Vertex.Type.Root.ATTRIBUTE.label());
        }

        public Class<java.lang.Object> valueClass() { return java.lang.Object.class; }

        @Override
        public boolean isRoot() { return true; }

        @Override
        public void label(java.lang.String label) { throw new HypergraphException(INVALID_ROOT_TYPE_MUTATION); }

        @Override
        public void isAbstract(boolean isAbstract) { throw new HypergraphException(INVALID_ROOT_TYPE_MUTATION); }

        @Override
        public void sup(AttributeType superType) { throw new HypergraphException(INVALID_ROOT_TYPE_MUTATION); }
    }

    public static class Boolean extends AttributeTypeImpl implements AttributeType.Boolean {

        public Boolean(Graph.Type graph, java.lang.String label) {
            super(graph, label, java.lang.Boolean.class);
        }

        private Boolean(TypeVertex vertex) {
            super(vertex);
            assert (vertex.label().equals(Schema.Vertex.Type.Root.ATTRIBUTE.label()) ||
                    vertex.valueClass().equals(Schema.ValueClass.BOOLEAN));
        }

        public static AttributeTypeImpl.Boolean of(TypeVertex vertex) {
            return vertex.label().equals(Schema.Vertex.Type.Root.ATTRIBUTE.label()) ?
                    new Root(vertex) :
                    new AttributeTypeImpl.Boolean(vertex);
        }

        @Override
        public void sup(AttributeType superType) {
            validateSuperTypeValueClass(superType);
            super.sup(superType);
        }

        @Override
        public Class<java.lang.Boolean> valueClass() { return java.lang.Boolean.class; }

        private static class Root extends AttributeTypeImpl.Boolean {

            private Root(TypeVertex vertex) {
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
        }
    }

    public static class Long extends AttributeTypeImpl implements AttributeType.Long {

        public Long(Graph.Type graph, java.lang.String label) {
            super(graph, label, java.lang.Long.class);
        }

        private Long(TypeVertex vertex) {
            super(vertex);
            assert (vertex.label().equals(Schema.Vertex.Type.Root.ATTRIBUTE.label()) ||
                    vertex.valueClass().equals(Schema.ValueClass.LONG));
        }

        public static AttributeTypeImpl.Long of(TypeVertex vertex) {
            return vertex.label().equals(Schema.Vertex.Type.Root.ATTRIBUTE.label()) ?
                    new Root(vertex) :
                    new AttributeTypeImpl.Long(vertex);
        }

        @Override
        public Class<java.lang.Long> valueClass() {
            return java.lang.Long.class;
        }

        private static class Root extends AttributeTypeImpl.Long {

            private Root(TypeVertex vertex) {
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
        }
    }

    public static class Double extends AttributeTypeImpl implements AttributeType.Double {

        public Double(Graph.Type graph, java.lang.String label) {
            super(graph, label, java.lang.Double.class);
        }

        private Double(TypeVertex vertex) {
            super(vertex);
            assert (vertex.label().equals(Schema.Vertex.Type.Root.ATTRIBUTE.label()) ||
                    vertex.valueClass().equals(Schema.ValueClass.DOUBLE));
        }

        public static AttributeTypeImpl.Double of(TypeVertex vertex) {
            return vertex.label().equals(Schema.Vertex.Type.Root.ATTRIBUTE.label()) ?
                    new Root(vertex) :
                    new AttributeTypeImpl.Double(vertex);
        }

        @Override
        public Class<java.lang.Double> valueClass() {
            return java.lang.Double.class;
        }

        private static class Root extends AttributeTypeImpl.Double {

            private Root(TypeVertex vertex) {
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
        }
    }

    public static class String extends AttributeTypeImpl implements AttributeType.String {

        public String(Graph.Type graph, java.lang.String label) {
            super(graph, label, java.lang.String.class);
        }

        private String(TypeVertex vertex) {
            super(vertex);
            assert (vertex.label().equals(Schema.Vertex.Type.Root.ATTRIBUTE.label()) ||
                    vertex.valueClass().equals(Schema.ValueClass.STRING));
        }

        public static AttributeTypeImpl.String of(TypeVertex vertex) {
            return vertex.label().equals(Schema.Vertex.Type.Root.ATTRIBUTE.label()) ?
                    new Root(vertex) :
                    new AttributeTypeImpl.String(vertex);
        }

        @Override
        public Class<java.lang.String> valueClass() {
            return java.lang.String.class;
        }

        private static class Root extends AttributeTypeImpl.String {

            private Root(TypeVertex vertex) {
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
        }
    }

    public static class DateTime extends AttributeTypeImpl implements AttributeType.DateTime {

        public DateTime(Graph.Type graph, java.lang.String label) {
            super(graph, label, LocalDateTime.class);
        }

        private DateTime(TypeVertex vertex) {
            super(vertex);
            assert (vertex.label().equals(Schema.Vertex.Type.Root.ATTRIBUTE.label()) ||
                    vertex.valueClass().equals(Schema.ValueClass.DATETIME));
        }

        public static AttributeTypeImpl.DateTime of(TypeVertex vertex) {
            return vertex.label().equals(Schema.Vertex.Type.Root.ATTRIBUTE.label()) ?
                    new Root(vertex) :
                    new AttributeTypeImpl.DateTime(vertex);
        }

        @Override
        public Class<LocalDateTime> valueClass() {
            return LocalDateTime.class;
        }

        private static class Root extends AttributeTypeImpl.DateTime {

            private Root(TypeVertex vertex) {
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
        }
    }
}
