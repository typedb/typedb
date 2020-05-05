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

import static hypergraph.common.exception.Error.TypeDefinition.INVALID_ROOT_TYPE_MUTATION;
import static hypergraph.common.exception.Error.TypeRetrieval.INVALID_TYPE_CASTING;

public abstract class AttributeType<ATT_TYPE extends AttributeType<ATT_TYPE>> extends ThingType<ATT_TYPE> {

    private AttributeType(TypeVertex vertex) {
        super(vertex);
        if (vertex.schema() != Schema.Vertex.Type.ATTRIBUTE_TYPE) {
            throw new HypergraphException("Invalid Attribute Type: " + vertex.label() +
                                                  " subtypes " + vertex.schema().root().label());
        }
    }

    private AttributeType(Graph.Type graph, java.lang.String label, Class<?> valueClass) {
        super(graph, label, Schema.Vertex.Type.ATTRIBUTE_TYPE);
        vertex.valueClass(Schema.ValueClass.of(valueClass));
    }

    public static AttributeType<? extends AttributeType> of(TypeVertex vertex) {
        switch (vertex.valueClass()) {
            case OBJECT:
                return new Object(vertex);
            case BOOLEAN:
                return AttributeType.Boolean.of(vertex);
            case LONG:
                return AttributeType.Long.of(vertex);
            case DOUBLE:
                return AttributeType.Double.of(vertex);
            case STRING:
                return AttributeType.String.of(vertex);
            case DATETIME:
                return AttributeType.DateTime.of(vertex);
            default:
                return null; // unreachable
        }
    }

    public abstract Class<?> valueClass();

    @Override
    public ATT_TYPE sup() {
        return super.sup();
    }

    @Override
    public void sup(ATT_TYPE superType) {
        if (!superType.isRoot() && !this.valueClass().equals(superType.valueClass())) {
            throw new HypergraphException(Error.TypeDefinition.INVALID_SUPERTYPE_VALUE_CLASS.format(
                    this.label(), this.valueClass().getSimpleName(),
                    superType.label(), superType.valueClass().getSimpleName())
            );
        }

        super.sup(superType);
    }

    @Override
    @SuppressWarnings("unchecked")
    ATT_TYPE newInstance(TypeVertex vertex) {
        // This is only called by AttributeType.Object and ATT_TYPE is always AttributeType<?>
        return (ATT_TYPE) of(vertex);
    }

    public AttributeType.Object asObject() {
        if (this.valueClass().equals(java.lang.Object.class)) {
            return (AttributeType.Object) this;
        } else {
            throw new HypergraphException(INVALID_TYPE_CASTING.format(
                    this.label(), AttributeType.Object.class.getCanonicalName()
            ));
        }
    }

    public AttributeType.Boolean asBoolean() {
        if (this.valueClass().equals(java.lang.Boolean.class) || this.isRoot()) {
            return (Boolean) this;
        } else {
            throw new HypergraphException(INVALID_TYPE_CASTING.format(
                    this.label(), AttributeType.Boolean.class.getCanonicalName()
            ));
        }
    }

    public AttributeType.Long asLong() {
        if (this.valueClass().equals(java.lang.Long.class) || this.isRoot()) {
            return (Long) this;
        } else {
            throw new HypergraphException(INVALID_TYPE_CASTING.format(
                    this.label(), AttributeType.Long.class.getCanonicalName()
            ));
        }
    }

    public AttributeType.Double asDouble() {
        if (this.valueClass().equals(java.lang.Double.class) || this.isRoot()) {
            return (Double) this;
        } else {
            throw new HypergraphException(INVALID_TYPE_CASTING.format(
                    this.label(), AttributeType.Double.class.getCanonicalName()
            ));
        }
    }

    public AttributeType.String asString() {
        if (this.valueClass().equals(java.lang.String.class) || this.isRoot()) {
            return (String) this;
        } else {
            throw new HypergraphException(INVALID_TYPE_CASTING.format(
                    this.label(), AttributeType.Long.class.getCanonicalName()
            ));
        }
    }

    public AttributeType.DateTime asDateTime() {
        if (this.valueClass().equals(LocalDateTime.class) || this.isRoot()) {
            return (DateTime) this;
        } else {
            throw new HypergraphException(INVALID_TYPE_CASTING.format(
                    this.label(), AttributeType.DateTime.class.getCanonicalName()
            ));
        }
    }

    @Override
    public boolean equals(java.lang.Object object) {
        if (this == object) return true;
        if (!(object instanceof AttributeType)) return false;
        AttributeType<?> that = (AttributeType<?>) object;
        return this.vertex.equals(that.vertex);
    }

    public static class Object extends AttributeType<AttributeType.Object> {

        public Object(TypeVertex vertex) {
            super(vertex);
            assert vertex.valueClass().equals(Schema.ValueClass.OBJECT);
            assert vertex.label().equals(Schema.Vertex.Type.Root.ATTRIBUTE.label());
        }

        @Override
        AttributeType.Object getThis() { return this; }

        public Class<java.lang.Object> valueClass() { return java.lang.Object.class; }

        @Override
        public boolean isRoot() { return true; }

        @Override
        public void label(java.lang.String label) { throw new HypergraphException(INVALID_ROOT_TYPE_MUTATION); }

        @Override
        public void isAbstract(boolean isAbstract) { throw new HypergraphException(INVALID_ROOT_TYPE_MUTATION); }

        @Override
        public AttributeType.Object sup() { return null; }

        @Override
        public void sup(AttributeType.Object superType) { throw new HypergraphException(INVALID_ROOT_TYPE_MUTATION); }
    }

    public static class Boolean extends AttributeType<AttributeType.Boolean> {

        public Boolean(Graph.Type graph, java.lang.String label) {
            super(graph, label, java.lang.Boolean.class);
        }

        private Boolean(TypeVertex vertex) {
            super(vertex);
            assert (vertex.label().equals(Schema.Vertex.Type.Root.ATTRIBUTE.label()) ||
                    vertex.valueClass().equals(Schema.ValueClass.BOOLEAN));
        }

        public static AttributeType.Boolean of(TypeVertex vertex) {
            if (vertex.label().equals(Schema.Vertex.Type.Root.ATTRIBUTE.label())) return new Boolean.Root(vertex);
            else return new AttributeType.Boolean(vertex);
        }

        @Override
        AttributeType.Boolean getThis() { return this; }

        @Override
        AttributeType.Boolean newInstance(TypeVertex vertex) { return of(vertex); }

        @Override
        public Class<java.lang.Boolean> valueClass() { return java.lang.Boolean.class; }

        private static class Root extends AttributeType.Boolean {

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
            public AttributeType.Boolean sup() { return null; }

            @Override
            public void sup(AttributeType.Boolean superType) { throw new HypergraphException(INVALID_ROOT_TYPE_MUTATION); }
        }
    }

    public static class Long extends AttributeType<AttributeType.Long> {

        public Long(Graph.Type graph, java.lang.String label) {
            super(graph, label, java.lang.Long.class);
        }

        private Long(TypeVertex vertex) {
            super(vertex);
            assert (vertex.label().equals(Schema.Vertex.Type.Root.ATTRIBUTE.label()) ||
                    vertex.valueClass().equals(Schema.ValueClass.LONG));
        }

        public static AttributeType.Long of(TypeVertex vertex) {
            if (vertex.label().equals(Schema.Vertex.Type.Root.ATTRIBUTE.label())) return new Long.Root(vertex);
            else return new AttributeType.Long(vertex);
        }

        @Override
        AttributeType.Long getThis() { return this; }

        @Override
        AttributeType.Long newInstance(TypeVertex vertex) {return of(vertex); }

        @Override
        public Class<java.lang.Long> valueClass() {
            return java.lang.Long.class;
        }

        private static class Root extends AttributeType.Long {

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
            public AttributeType.Long sup() { return null; }

            @Override
            public void sup(AttributeType.Long superType) { throw new HypergraphException(INVALID_ROOT_TYPE_MUTATION); }
        }
    }

    public static class Double extends AttributeType<AttributeType.Double> {

        public Double(Graph.Type graph, java.lang.String label) {
            super(graph, label, java.lang.Double.class);
        }

        private Double(TypeVertex vertex) {
            super(vertex);
            assert (vertex.label().equals(Schema.Vertex.Type.Root.ATTRIBUTE.label()) ||
                    vertex.valueClass().equals(Schema.ValueClass.DOUBLE));
        }

        public static AttributeType.Double of(TypeVertex vertex) {
            if (vertex.label().equals(Schema.Vertex.Type.Root.ATTRIBUTE.label())) return new Double.Root(vertex);
            else return new AttributeType.Double(vertex);
        }

        @Override
        AttributeType.Double getThis() { return this; }

        @Override
        AttributeType.Double newInstance(TypeVertex vertex) {return of(vertex); }

        @Override
        public Class<java.lang.Double> valueClass() {
            return java.lang.Double.class;
        }

        private static class Root extends AttributeType.Double {

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
            public AttributeType.Double sup() { return null; }

            @Override
            public void sup(AttributeType.Double superType) { throw new HypergraphException(INVALID_ROOT_TYPE_MUTATION); }
        }
    }

    public static class String extends AttributeType<AttributeType.String> {

        public String(Graph.Type graph, java.lang.String label) {
            super(graph, label, java.lang.String.class);
        }

        private String(TypeVertex vertex) {
            super(vertex);
            assert (vertex.label().equals(Schema.Vertex.Type.Root.ATTRIBUTE.label()) ||
                    vertex.valueClass().equals(Schema.ValueClass.STRING));
        }

        public static AttributeType.String of(TypeVertex vertex) {
            if (vertex.label().equals(Schema.Vertex.Type.Root.ATTRIBUTE.label())) return new String.Root(vertex);
            else return new AttributeType.String(vertex);
        }

        @Override
        AttributeType.String getThis() { return this; }

        @Override
        AttributeType.String newInstance(TypeVertex vertex) {return of(vertex); }

        @Override
        public Class<java.lang.String> valueClass() {
            return java.lang.String.class;
        }

        private static class Root extends AttributeType.String {

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
            public AttributeType.String sup() { return null; }

            @Override
            public void sup(AttributeType.String superType) { throw new HypergraphException(INVALID_ROOT_TYPE_MUTATION); }
        }
    }

    public static class DateTime extends AttributeType<AttributeType.DateTime> {

        public DateTime(Graph.Type graph, java.lang.String label) {
            super(graph, label, LocalDateTime.class);
        }

        private DateTime(TypeVertex vertex) {
            super(vertex);
            assert (vertex.label().equals(Schema.Vertex.Type.Root.ATTRIBUTE.label()) ||
                    vertex.valueClass().equals(Schema.ValueClass.DATETIME));
        }

        public static AttributeType.DateTime of(TypeVertex vertex) {
            if (vertex.label().equals(Schema.Vertex.Type.Root.ATTRIBUTE.label())) return new DateTime.Root(vertex);
            else return new AttributeType.DateTime(vertex);
        }

        @Override
        AttributeType.DateTime getThis() { return this; }

        @Override
        AttributeType.DateTime newInstance(TypeVertex vertex) {return of(vertex); }

        @Override
        public Class<LocalDateTime> valueClass() {
            return LocalDateTime.class;
        }

        private static class Root extends AttributeType.DateTime {

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
            public AttributeType.DateTime sup() { return null; }

            @Override
            public void sup(AttributeType.DateTime superType) { throw new HypergraphException(INVALID_ROOT_TYPE_MUTATION); }
        }
    }
}
