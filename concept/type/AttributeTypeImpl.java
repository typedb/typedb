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

import hypergraph.common.exception.Error;
import hypergraph.common.exception.HypergraphException;
import hypergraph.graph.Graph;
import hypergraph.graph.Schema;
import hypergraph.graph.vertex.TypeVertex;

import java.time.LocalDateTime;
import java.util.stream.Stream;

import static hypergraph.common.exception.Error.TypeDefinition.INVALID_ROOT_TYPE_MUTATION;
import static hypergraph.common.exception.Error.TypeRetrieval.INVALID_TYPE_CASTING;

public abstract class AttributeTypeImpl<ATT_TYPE extends AttributeTypeImpl<ATT_TYPE>> extends ThingTypeImpl<ATT_TYPE> implements AttributeTypeInt {

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

    public static AttributeTypeImpl<? extends AttributeTypeInt> of(TypeVertex vertex) {
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
                return null; // unreachable
        }
    }

    @Override
    public ATT_TYPE sup() {
        return super.sup();
    }

    @Override
    public Stream<ATT_TYPE> sups() {
        return super.sups();
    }

    @Override
    public Stream<ATT_TYPE> subs() {
        return super.subs();
    }

    @Override
    @SuppressWarnings("unchecked")
    ATT_TYPE newInstance(TypeVertex vertex) {
        // This is only called by AttributeType.Object and ATT_TYPE is always AttributeType<?>
        return (ATT_TYPE) of(vertex);
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

    void validateSuperTypeValueClass(AttributeTypeInt superType) {
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
        AttributeTypeImpl<?> that = (AttributeTypeImpl<?>) object;
        return this.vertex.equals(that.vertex);
    }

    private static class Root extends AttributeTypeImpl<Root> {

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
        public AttributeTypeImpl.Root sup() { return null; }

        @Override
        public void sup(AttributeTypeInt superType) { throw new HypergraphException(INVALID_ROOT_TYPE_MUTATION); }
    }

    public static class Boolean extends AttributeTypeImpl<AttributeTypeImpl.Boolean> implements AttributeTypeInt.Boolean {

        public Boolean(Graph.Type graph, java.lang.String label) {
            super(graph, label, java.lang.Boolean.class);
        }

        private Boolean(TypeVertex vertex) {
            super(vertex);
            assert (vertex.label().equals(Schema.Vertex.Type.Root.ATTRIBUTE.label()) ||
                    vertex.valueClass().equals(Schema.ValueClass.BOOLEAN));
        }

        public static AttributeTypeImpl.Boolean of(TypeVertex vertex) {
            if (vertex.label().equals(Schema.Vertex.Type.Root.ATTRIBUTE.label())) return new AttributeTypeImpl.Boolean.Root(vertex);
            else return new AttributeTypeImpl.Boolean(vertex);
        }

        @Override
        AttributeTypeImpl.Boolean newInstance(TypeVertex vertex) { return of(vertex); }

        @Override
        public void sup(AttributeTypeInt superType) {
            validateSuperTypeValueClass(superType);
            super.sup((AttributeTypeImpl.Boolean) superType);
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
            public AttributeTypeImpl.Boolean sup() { return null; }

            @Override
            public void sup(AttributeTypeImpl.Boolean superType) { throw new HypergraphException(INVALID_ROOT_TYPE_MUTATION); }
        }
    }

    public static class Long extends AttributeTypeImpl<AttributeTypeImpl.Long> implements AttributeTypeInt.Long {

        public Long(Graph.Type graph, java.lang.String label) {
            super(graph, label, java.lang.Long.class);
        }

        private Long(TypeVertex vertex) {
            super(vertex);
            assert (vertex.label().equals(Schema.Vertex.Type.Root.ATTRIBUTE.label()) ||
                    vertex.valueClass().equals(Schema.ValueClass.LONG));
        }

        public static AttributeTypeImpl.Long of(TypeVertex vertex) {
            if (vertex.label().equals(Schema.Vertex.Type.Root.ATTRIBUTE.label())) return new AttributeTypeImpl.Long.Root(vertex);
            else return new AttributeTypeImpl.Long(vertex);
        }

        @Override
        AttributeTypeImpl.Long newInstance(TypeVertex vertex) {return of(vertex); }

        @Override
        public void sup(AttributeTypeInt superType) {
            validateSuperTypeValueClass(superType);
            super.sup((AttributeTypeImpl.Long) superType);
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
            public void label(java.lang.String label) {
                throw new HypergraphException(INVALID_ROOT_TYPE_MUTATION);
            }

            @Override
            public void isAbstract(boolean isAbstract) {
                throw new HypergraphException(INVALID_ROOT_TYPE_MUTATION);
            }

            @Override
            public void sup(AttributeTypeImpl.Long superType) {
                throw new HypergraphException(INVALID_ROOT_TYPE_MUTATION);
            }

            @Override
            public AttributeTypeImpl.Long sup() { return null; }
        }
    }

    public static class Double extends AttributeTypeImpl<AttributeTypeImpl.Double> implements AttributeTypeInt.Double {

        public Double(Graph.Type graph, java.lang.String label) {
            super(graph, label, java.lang.Double.class);
        }

        private Double(TypeVertex vertex) {
            super(vertex);
            assert (vertex.label().equals(Schema.Vertex.Type.Root.ATTRIBUTE.label()) ||
                    vertex.valueClass().equals(Schema.ValueClass.DOUBLE));
        }

        public static AttributeTypeImpl.Double of(TypeVertex vertex) {
            if (vertex.label().equals(Schema.Vertex.Type.Root.ATTRIBUTE.label())) return new AttributeTypeImpl.Double.Root(vertex);
            else return new AttributeTypeImpl.Double(vertex);
        }

        @Override
        AttributeTypeImpl.Double newInstance(TypeVertex vertex) {return of(vertex); }

        @Override
        public void sup(AttributeTypeInt superType) {
            validateSuperTypeValueClass(superType);
            super.sup((AttributeTypeImpl.Double) superType);
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
            public void label(java.lang.String label) {
                throw new HypergraphException(INVALID_ROOT_TYPE_MUTATION); }

            @Override
            public void isAbstract(boolean isAbstract) {
                throw new HypergraphException(INVALID_ROOT_TYPE_MUTATION); }

            @Override
            public void sup(AttributeTypeImpl.Double superType) {
                throw new HypergraphException(INVALID_ROOT_TYPE_MUTATION); }

            @Override
            public AttributeTypeImpl.Double sup() { return null; }
        }
    }

    public static class String extends AttributeTypeImpl<AttributeTypeImpl.String> implements AttributeTypeInt.String {

        public String(Graph.Type graph, java.lang.String label) {
            super(graph, label, java.lang.String.class);
        }

        private String(TypeVertex vertex) {
            super(vertex);
            assert (vertex.label().equals(Schema.Vertex.Type.Root.ATTRIBUTE.label()) ||
                    vertex.valueClass().equals(Schema.ValueClass.STRING));
        }

        public static AttributeTypeImpl.String of(TypeVertex vertex) {
            if (vertex.label().equals(Schema.Vertex.Type.Root.ATTRIBUTE.label())) return new AttributeTypeImpl.String.Root(vertex);
            else return new AttributeTypeImpl.String(vertex);
        }

        @Override
        AttributeTypeImpl.String newInstance(TypeVertex vertex) {return of(vertex); }

        @Override
        public void sup(AttributeTypeInt superType) {
            validateSuperTypeValueClass(superType);
            super.sup((AttributeTypeImpl.String) superType);
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
            public void label(java.lang.String label) {
                throw new HypergraphException(INVALID_ROOT_TYPE_MUTATION);
            }

            @Override
            public void isAbstract(boolean isAbstract) {
                throw new HypergraphException(INVALID_ROOT_TYPE_MUTATION);
            }

            @Override
            public void sup(AttributeTypeImpl.String superType) {
                throw new HypergraphException(INVALID_ROOT_TYPE_MUTATION);
            }

            @Override
            public AttributeTypeImpl.String sup() { return null; }
        }
    }

    public static class DateTime extends AttributeTypeImpl<AttributeTypeImpl.DateTime> implements AttributeTypeInt.DateTime {

        public DateTime(Graph.Type graph, java.lang.String label) {
            super(graph, label, LocalDateTime.class);
        }

        private DateTime(TypeVertex vertex) {
            super(vertex);
            assert (vertex.label().equals(Schema.Vertex.Type.Root.ATTRIBUTE.label()) ||
                    vertex.valueClass().equals(Schema.ValueClass.DATETIME));
        }

        public static AttributeTypeImpl.DateTime of(TypeVertex vertex) {
            if (vertex.label().equals(Schema.Vertex.Type.Root.ATTRIBUTE.label())) return new AttributeTypeImpl.DateTime.Root(vertex);
            else return new AttributeTypeImpl.DateTime(vertex);
        }

        @Override
        AttributeTypeImpl.DateTime newInstance(TypeVertex vertex) {return of(vertex); }

        @Override
        public void sup(AttributeTypeInt superType) {
            validateSuperTypeValueClass(superType);
            super.sup((AttributeTypeImpl.DateTime) superType);
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
            public void label(java.lang.String label) {
                throw new HypergraphException(INVALID_ROOT_TYPE_MUTATION);
            }

            @Override
            public void isAbstract(boolean isAbstract) {
                throw new HypergraphException(INVALID_ROOT_TYPE_MUTATION);
            }

            @Override
            public void sup(AttributeTypeImpl.DateTime superType) {
                throw new HypergraphException(INVALID_ROOT_TYPE_MUTATION);
            }

            @Override
            public AttributeTypeImpl.DateTime sup() { return null; }
        }
    }
}
