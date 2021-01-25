/*
 * Copyright (C) 2021 Grakn Labs
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

package grakn.core.concept.type.impl;

import grakn.core.common.exception.GraknException;
import grakn.core.common.iterator.Iterators;
import grakn.core.common.iterator.ResourceIterator;
import grakn.core.concept.thing.Attribute;
import grakn.core.concept.thing.impl.AttributeImpl;
import grakn.core.concept.type.AttributeType;
import grakn.core.concept.type.RoleType;
import grakn.core.graph.GraphManager;
import grakn.core.graph.common.Encoding;
import grakn.core.graph.vertex.AttributeVertex;
import grakn.core.graph.vertex.TypeVertex;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static grakn.common.util.Objects.className;
import static grakn.core.common.exception.ErrorMessage.Internal.UNRECOGNISED_VALUE;
import static grakn.core.common.exception.ErrorMessage.ThingWrite.ATTRIBUTE_VALUE_UNSATISFIES_REGEX;
import static grakn.core.common.exception.ErrorMessage.TypeRead.INVALID_TYPE_CASTING;
import static grakn.core.common.exception.ErrorMessage.TypeRead.TYPE_ROOT_MISMATCH;
import static grakn.core.common.exception.ErrorMessage.TypeRead.VALUE_TYPE_MISMATCH;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.ATTRIBUTE_NEW_SUPERTYPE_NOT_ABSTRACT;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.ATTRIBUTE_REGEX_UNSATISFIES_INSTANCES;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.ATTRIBUTE_SUPERTYPE_VALUE_TYPE;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.ATTRIBUTE_UNSET_ABSTRACT_HAS_SUBTYPES;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.ROOT_TYPE_MUTATION;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.TYPE_HAS_INSTANCES;
import static grakn.core.common.iterator.Iterators.link;
import static grakn.core.graph.common.Encoding.Edge.Type.OWNS;
import static grakn.core.graph.common.Encoding.Edge.Type.OWNS_KEY;
import static grakn.core.graph.common.Encoding.Edge.Type.SUB;
import static grakn.core.graph.common.Encoding.ValueType.BOOLEAN;
import static grakn.core.graph.common.Encoding.ValueType.DATETIME;
import static grakn.core.graph.common.Encoding.ValueType.DOUBLE;
import static grakn.core.graph.common.Encoding.ValueType.LONG;
import static grakn.core.graph.common.Encoding.ValueType.STRING;
import static grakn.core.graph.common.Encoding.Vertex.Type.ATTRIBUTE_TYPE;
import static grakn.core.graph.common.Encoding.Vertex.Type.Root.ATTRIBUTE;

public abstract class AttributeTypeImpl extends ThingTypeImpl implements AttributeType {

    private AttributeTypeImpl(GraphManager graphMgr, TypeVertex vertex) {
        super(graphMgr, vertex);
        if (vertex.encoding() != ATTRIBUTE_TYPE) {
            throw exception(GraknException.of(TYPE_ROOT_MISMATCH, vertex.label(),
                                              ATTRIBUTE_TYPE.root().label(),
                                              vertex.encoding().root().label()));
        }
    }

    private AttributeTypeImpl(GraphManager graphMgr, java.lang.String label, Class<?> valueType) {
        super(graphMgr, label, ATTRIBUTE_TYPE);
        vertex.valueType(Encoding.ValueType.of(valueType));
    }

    public static AttributeTypeImpl of(GraphManager graphMgr, TypeVertex vertex) {
        switch (vertex.valueType()) {
            case OBJECT:
                return new AttributeTypeImpl.Root(graphMgr, vertex);
            case BOOLEAN:
                return AttributeTypeImpl.Boolean.of(graphMgr, vertex);
            case LONG:
                return AttributeTypeImpl.Long.of(graphMgr, vertex);
            case DOUBLE:
                return AttributeTypeImpl.Double.of(graphMgr, vertex);
            case STRING:
                return AttributeTypeImpl.String.of(graphMgr, vertex);
            case DATETIME:
                return AttributeTypeImpl.DateTime.of(graphMgr, vertex);
            default:
                throw graphMgr.exception(GraknException.of(UNRECOGNISED_VALUE));
        }
    }

    @Override
    public void setAbstract() {
        if (getInstances().findFirst().isPresent()) {
            throw exception(GraknException.of(TYPE_HAS_INSTANCES, getLabel()));
        }
        vertex.isAbstract(true);
    }

    @Override
    public void unsetAbstract() {
        if (getSubtypes().anyMatch(sub -> !sub.equals(this))) {
            throw exception(GraknException.of(ATTRIBUTE_UNSET_ABSTRACT_HAS_SUBTYPES, getLabel()));
        }
        vertex.isAbstract(false);
    }

    @Override
    public abstract Stream<? extends AttributeTypeImpl> getSubtypes();

    @Override
    public abstract Stream<? extends AttributeTypeImpl> getSubtypesExplicit();

    @Override
    public abstract Stream<? extends AttributeImpl<?>> getInstances();

    ResourceIterator<TypeVertex> getSubtypeVertices(Encoding.ValueType valueType) {
        return Iterators.tree(vertex, v -> v.ins().edge(SUB).from().filter(sv -> sv.valueType().equals(valueType)));
    }

    ResourceIterator<TypeVertex> getSubtypeVerticesDirect(Encoding.ValueType valueType) {
        return vertex.ins().edge(SUB).from().filter(sv -> sv.valueType().equals(valueType));
    }

    @Override
    public void setSupertype(AttributeType superType) {
        if (!superType.isRoot() && !Objects.equals(this.getValueType(), superType.getValueType())) {
            throw exception(GraknException.of(ATTRIBUTE_SUPERTYPE_VALUE_TYPE, getLabel(), getValueType().name(),
                                              superType.getLabel(), superType.getValueType().name()));
        } else if (!superType.isAbstract()) {
            throw exception(GraknException.of(ATTRIBUTE_NEW_SUPERTYPE_NOT_ABSTRACT, superType.getLabel()));
        }
        setSuperTypeVertex(((AttributeTypeImpl) superType).vertex);
    }

    @Override
    public boolean isKeyable() {
        return vertex.valueType().isKeyable();
    }

    @Override
    public abstract ValueType getValueType();

    @Override
    public Stream<? extends ThingTypeImpl> getOwners() {
        return getOwners(false);
    }

    @Override
    public Stream<? extends ThingTypeImpl> getOwners(boolean onlyKey) {
        if (isRoot()) return Stream.of();

        return directOwners(onlyKey)
                .flatMap(ThingTypeImpl::getSubtypes)
                .filter(t -> t.overriddenOwns(onlyKey, true).noneMatch(o -> o.equals(this)));
    }

    private Stream<? extends ThingTypeImpl> directOwners(boolean onlyKey) {
        if (isRoot()) return Stream.of();

        if (onlyKey) {
            return vertex.ins().edge(OWNS_KEY).from().map(v -> ThingTypeImpl.of(graphMgr, v)).stream();
        } else {
            return link(vertex.ins().edge(OWNS_KEY).from(), vertex.ins().edge(OWNS).from())
                    .map(v -> ThingTypeImpl.of(graphMgr, v)).stream();
        }
    }

    @Override
    public List<GraknException> validate() {
        return super.validate();
    }

    @Override
    public boolean isAttributeType() { return true; }

    @Override
    public AttributeTypeImpl asAttributeType() { return this; }

    @Override
    public boolean isBoolean() { return false; }

    @Override
    public boolean isLong() { return false; }

    @Override
    public boolean isDouble() { return false; }

    @Override
    public boolean isString() { return false; }

    @Override
    public boolean isDateTime() { return false; }

    @Override
    public AttributeTypeImpl.Boolean asBoolean() {
        throw exception(GraknException.of(INVALID_TYPE_CASTING, className(this.getClass()),
                                          className(AttributeType.Boolean.class)));
    }

    @Override
    public AttributeTypeImpl.Long asLong() {
        throw exception(GraknException.of(INVALID_TYPE_CASTING, className(this.getClass()),
                                          className(AttributeType.Long.class)));
    }

    @Override
    public AttributeTypeImpl.Double asDouble() {
        throw exception(GraknException.of(INVALID_TYPE_CASTING, className(this.getClass()),
                                          className(AttributeType.Double.class)));
    }

    @Override
    public AttributeTypeImpl.String asString() {
        throw exception(GraknException.of(INVALID_TYPE_CASTING, className(this.getClass()),
                                          className(AttributeType.String.class)));
    }

    @Override
    public AttributeTypeImpl.DateTime asDateTime() {
        throw exception(GraknException.of(INVALID_TYPE_CASTING, className(this.getClass()),
                                          className(AttributeType.DateTime.class)));
    }

    @Override
    public boolean equals(java.lang.Object object) {
        if (this == object) return true;
        if (!(object instanceof AttributeTypeImpl)) return false;
        // We do the above, as opposed to checking if (object == null || getClass() != object.getClass())
        // because it is possible to compare a attribute root types wrapped in different type classes
        // such as: root type wrapped in AttributeTypeImpl.Root and as in AttributeType.Boolean.Root

        AttributeTypeImpl that = (AttributeTypeImpl) object;
        return this.vertex.equals(that.vertex);
    }

    private static class Root extends AttributeTypeImpl {

        private Root(GraphManager graphMgr, TypeVertex vertex) {
            super(graphMgr, vertex);
            assert vertex.valueType().equals(Encoding.ValueType.OBJECT);
            assert vertex.label().equals(ATTRIBUTE.label());
        }

        public ValueType getValueType() { return ValueType.OBJECT; }

        @Override
        public AttributeTypeImpl.Boolean asBoolean() { return AttributeTypeImpl.Boolean.of(graphMgr, vertex); }

        @Override
        public AttributeTypeImpl.Long asLong() { return AttributeTypeImpl.Long.of(graphMgr, vertex); }

        @Override
        public AttributeTypeImpl.Double asDouble() { return AttributeTypeImpl.Double.of(graphMgr, vertex); }

        @Override
        public AttributeTypeImpl.String asString() { return AttributeTypeImpl.String.of(graphMgr, vertex); }

        @Override
        public AttributeTypeImpl.DateTime asDateTime() { return AttributeTypeImpl.DateTime.of(graphMgr, vertex); }

        @Override
        public boolean isRoot() { return true; }

        @Override
        public void setLabel(java.lang.String label) { throw exception(GraknException.of(ROOT_TYPE_MUTATION)); }

        @Override
        public void unsetAbstract() { throw exception(GraknException.of(ROOT_TYPE_MUTATION)); }

        @Override
        public void setSupertype(AttributeType superType) { throw exception(GraknException.of(ROOT_TYPE_MUTATION)); }

        @Override
        public Stream<AttributeTypeImpl> getSubtypes() {
            return getSubtypes(v -> {
                switch (v.valueType()) {
                    case OBJECT:
                        assert this.vertex == v;
                        return this;
                    case BOOLEAN:
                        return AttributeTypeImpl.Boolean.of(graphMgr, v);
                    case LONG:
                        return AttributeTypeImpl.Long.of(graphMgr, v);
                    case DOUBLE:
                        return AttributeTypeImpl.Double.of(graphMgr, v);
                    case STRING:
                        return AttributeTypeImpl.String.of(graphMgr, v);
                    case DATETIME:
                        return AttributeTypeImpl.DateTime.of(graphMgr, v);
                    default:
                        throw exception(GraknException.of(UNRECOGNISED_VALUE));
                }
            });
        }

        @Override
        public Stream<AttributeTypeImpl> getSubtypesExplicit() {
            return getSubtypesExplicit(v -> {
                switch (v.valueType()) {
                    case BOOLEAN:
                        return AttributeTypeImpl.Boolean.of(graphMgr, v);
                    case LONG:
                        return AttributeTypeImpl.Long.of(graphMgr, v);
                    case DOUBLE:
                        return AttributeTypeImpl.Double.of(graphMgr, v);
                    case STRING:
                        return AttributeTypeImpl.String.of(graphMgr, v);
                    case DATETIME:
                        return AttributeTypeImpl.DateTime.of(graphMgr, v);
                    default:
                        throw exception(GraknException.of(UNRECOGNISED_VALUE));
                }
            });
        }

        @Override
        public Stream<AttributeImpl<?>> getInstances() {
            return instances(v -> AttributeImpl.of(v.asAttribute()));
        }

        @Override
        public void setOwns(AttributeType attributeType, boolean isKey) {
            throw exception(GraknException.of(ROOT_TYPE_MUTATION));
        }

        @Override
        public void setOwns(AttributeType attributeType, AttributeType overriddenType, boolean isKey) {
            throw exception(GraknException.of(ROOT_TYPE_MUTATION));
        }

        @Override
        public void setPlays(RoleType roleType) {
            throw exception(GraknException.of(ROOT_TYPE_MUTATION));
        }

        @Override
        public void setPlays(RoleType roleType, RoleType overriddenType) {
            throw exception(GraknException.of(ROOT_TYPE_MUTATION));
        }

        @Override
        public void unsetPlays(RoleType roleType) {
            throw exception(GraknException.of(ROOT_TYPE_MUTATION));
        }
    }

    public static class Boolean extends AttributeTypeImpl implements AttributeType.Boolean {

        public Boolean(GraphManager graphMgr, java.lang.String label) {
            super(graphMgr, label, java.lang.Boolean.class);
        }

        private Boolean(GraphManager graphMgr, TypeVertex vertex) {
            super(graphMgr, vertex);
            if (!vertex.label().equals(ATTRIBUTE.label()) &&
                    !vertex.valueType().equals(BOOLEAN)) {
                throw exception(GraknException.of(VALUE_TYPE_MISMATCH, vertex.label(),
                                                  BOOLEAN.name(), vertex.valueType().name()));
            }
        }

        public static AttributeTypeImpl.Boolean of(GraphManager graphMgr, TypeVertex vertex) {
            return vertex.label().equals(ATTRIBUTE.label()) ?
                    new Root(graphMgr, vertex) :
                    new AttributeTypeImpl.Boolean(graphMgr, vertex);
        }

        @Override
        public Stream<AttributeTypeImpl.Boolean> getSubtypes() {
            return super.getSubtypes(v -> AttributeTypeImpl.Boolean.of(graphMgr, v));
        }

        @Override
        public Stream<AttributeTypeImpl.Boolean> getSubtypesExplicit() {
            return super.getSubtypesExplicit(v -> AttributeTypeImpl.Boolean.of(graphMgr, v));
        }

        @Override
        public Stream<AttributeImpl.Boolean> getInstances() {
            return instances(v -> new AttributeImpl.Boolean(v.asAttribute().asBoolean()));
        }

        @Override
        public ValueType getValueType() { return ValueType.BOOLEAN; }

        @Override
        public boolean isBoolean() { return true; }

        @Override
        public AttributeTypeImpl.Boolean asBoolean() { return this; }

        @Override
        public Attribute.Boolean put(boolean value) {
            return put(value, false);
        }

        @Override
        public Attribute.Boolean put(boolean value, boolean isInferred) {
            validateIsCommittedAndNotAbstract(Attribute.class);
            AttributeVertex<java.lang.Boolean> attVertex = graphMgr.data().put(vertex, value, isInferred);
            return new AttributeImpl.Boolean(attVertex);
        }

        @Override
        public Attribute.Boolean get(boolean value) {
            AttributeVertex<java.lang.Boolean> attVertex = graphMgr.data().get(vertex, value);
            if (attVertex != null) return new AttributeImpl.Boolean(attVertex);
            else return null;
        }

        private static class Root extends AttributeTypeImpl.Boolean {

            private Root(GraphManager graphMgr, TypeVertex vertex) {
                super(graphMgr, vertex);
                assert vertex.label().equals(ATTRIBUTE.label());
            }

            @Override
            public boolean isRoot() { return true; }

            @Override
            public Stream<AttributeTypeImpl.Boolean> getSubtypes() {
                return super.getSubtypeVertices(BOOLEAN).map(v -> AttributeTypeImpl.Boolean.of(graphMgr, v)).stream();
            }

            @Override
            public Stream<AttributeTypeImpl.Boolean> getSubtypesExplicit() {
                return super.getSubtypeVerticesDirect(BOOLEAN).map(v -> AttributeTypeImpl.Boolean.of(graphMgr, v)).stream();
            }

            @Override
            public void setLabel(java.lang.String label) {
                throw exception(GraknException.of(ROOT_TYPE_MUTATION));
            }

            @Override
            public void unsetAbstract() {
                throw exception(GraknException.of(ROOT_TYPE_MUTATION));
            }

            @Override
            public void setSupertype(AttributeType superType) {
                throw exception(GraknException.of(ROOT_TYPE_MUTATION));
            }

            @Override
            public void setOwns(AttributeType attributeType, boolean isKey) {
                throw exception(GraknException.of(ROOT_TYPE_MUTATION));
            }

            @Override
            public void setOwns(AttributeType attributeType, AttributeType overriddenType, boolean isKey) {
                throw exception(GraknException.of(ROOT_TYPE_MUTATION));
            }

            @Override
            public void setPlays(RoleType roleType) {
                throw exception(GraknException.of(ROOT_TYPE_MUTATION));
            }

            @Override
            public void setPlays(RoleType roleType, RoleType overriddenType) {
                throw exception(GraknException.of(ROOT_TYPE_MUTATION));
            }

            @Override
            public void unsetPlays(RoleType roleType) {
                throw exception(GraknException.of(ROOT_TYPE_MUTATION));
            }
        }
    }

    public static class Long extends AttributeTypeImpl implements AttributeType.Long {

        public Long(GraphManager graphMgr, java.lang.String label) {
            super(graphMgr, label, java.lang.Long.class);
        }

        private Long(GraphManager graphMgr, TypeVertex vertex) {
            super(graphMgr, vertex);
            if (!vertex.label().equals(ATTRIBUTE.label()) && !vertex.valueType().equals(LONG)) {
                throw exception(GraknException.of(VALUE_TYPE_MISMATCH, vertex.label(),
                                                  LONG.name(), vertex.valueType().name()));
            }
        }

        public static AttributeTypeImpl.Long of(GraphManager graphMgr, TypeVertex vertex) {
            return vertex.label().equals(ATTRIBUTE.label()) ?
                    new Root(graphMgr, vertex) :
                    new AttributeTypeImpl.Long(graphMgr, vertex);
        }

        @Override
        public Stream<AttributeTypeImpl.Long> getSubtypes() {
            return super.getSubtypes(v -> AttributeTypeImpl.Long.of(graphMgr, v));
        }

        @Override
        public Stream<AttributeTypeImpl.Long> getSubtypesExplicit() {
            return super.getSubtypesExplicit(v -> AttributeTypeImpl.Long.of(graphMgr, v));
        }

        @Override
        public Stream<AttributeImpl.Long> getInstances() {
            return instances(v -> new AttributeImpl.Long(v.asAttribute().asLong()));
        }

        @Override
        public ValueType getValueType() {
            return ValueType.LONG;
        }

        @Override
        public boolean isLong() { return true; }

        @Override
        public AttributeTypeImpl.Long asLong() { return this; }

        @Override
        public Attribute.Long put(long value) {
            return put(value, false);
        }

        @Override
        public Attribute.Long put(long value, boolean isInferred) {
            validateIsCommittedAndNotAbstract(Attribute.class);
            AttributeVertex<java.lang.Long> attVertex = graphMgr.data().put(vertex, value, isInferred);
            return new AttributeImpl.Long(attVertex);
        }

        @Override
        public Attribute.Long get(long value) {
            AttributeVertex<java.lang.Long> attVertex = graphMgr.data().get(vertex, value);
            if (attVertex != null) return new AttributeImpl.Long(attVertex);
            else return null;
        }

        private static class Root extends AttributeTypeImpl.Long {

            private Root(GraphManager graphMgr, TypeVertex vertex) {
                super(graphMgr, vertex);
                assert vertex.label().equals(ATTRIBUTE.label());
            }

            @Override
            public boolean isRoot() { return true; }

            @Override
            public Stream<AttributeTypeImpl.Long> getSubtypes() {
                return super.getSubtypeVertices(LONG).map(v -> AttributeTypeImpl.Long.of(graphMgr, v)).stream();
            }

            @Override
            public Stream<AttributeTypeImpl.Long> getSubtypesExplicit() {
                return super.getSubtypeVerticesDirect(LONG).map(v -> AttributeTypeImpl.Long.of(graphMgr, v)).stream();
            }

            @Override
            public void setLabel(java.lang.String label) {
                throw exception(GraknException.of(ROOT_TYPE_MUTATION));
            }

            @Override
            public void unsetAbstract() {
                throw exception(GraknException.of(ROOT_TYPE_MUTATION));
            }

            @Override
            public void setSupertype(AttributeType superType) {
                throw exception(GraknException.of(ROOT_TYPE_MUTATION));
            }

            @Override
            public void setOwns(AttributeType attributeType, boolean isKey) {
                throw exception(GraknException.of(ROOT_TYPE_MUTATION));
            }

            @Override
            public void setOwns(AttributeType attributeType, AttributeType overriddenType, boolean isKey) {
                throw exception(GraknException.of(ROOT_TYPE_MUTATION));
            }

            @Override
            public void setPlays(RoleType roleType) {
                throw exception(GraknException.of(ROOT_TYPE_MUTATION));
            }

            @Override
            public void setPlays(RoleType roleType, RoleType overriddenType) {
                throw exception(GraknException.of(ROOT_TYPE_MUTATION));
            }

            @Override
            public void unsetPlays(RoleType roleType) {
                throw exception(GraknException.of(ROOT_TYPE_MUTATION));
            }
        }
    }

    public static class Double extends AttributeTypeImpl implements AttributeType.Double {

        public Double(GraphManager graphMgr, java.lang.String label) {
            super(graphMgr, label, java.lang.Double.class);
        }

        private Double(GraphManager graphMgr, TypeVertex vertex) {
            super(graphMgr, vertex);
            if (!vertex.label().equals(ATTRIBUTE.label()) && !vertex.valueType().equals(DOUBLE)) {
                throw exception(GraknException.of(VALUE_TYPE_MISMATCH, vertex.label(),
                                                  DOUBLE.name(), vertex.valueType().name()));
            }
        }

        public static AttributeTypeImpl.Double of(GraphManager graphMgr, TypeVertex vertex) {
            return vertex.label().equals(ATTRIBUTE.label()) ?
                    new Root(graphMgr, vertex) :
                    new AttributeTypeImpl.Double(graphMgr, vertex);
        }

        @Override
        public Stream<AttributeTypeImpl.Double> getSubtypes() {
            return super.getSubtypes(v -> AttributeTypeImpl.Double.of(graphMgr, v));
        }

        @Override
        public Stream<AttributeTypeImpl.Double> getSubtypesExplicit() {
            return super.getSubtypesExplicit(v -> AttributeTypeImpl.Double.of(graphMgr, v));
        }

        @Override
        public Stream<AttributeImpl.Double> getInstances() {
            return instances(v -> new AttributeImpl.Double(v.asAttribute().asDouble()));
        }

        @Override
        public ValueType getValueType() {
            return ValueType.DOUBLE;
        }

        @Override
        public boolean isDouble() { return true; }

        @Override
        public AttributeTypeImpl.Double asDouble() { return this; }

        @Override
        public Attribute.Double put(double value) {
            return put(value, false);
        }

        @Override
        public Attribute.Double put(double value, boolean isInferred) {
            validateIsCommittedAndNotAbstract(Attribute.class);
            AttributeVertex<java.lang.Double> attVertex = graphMgr.data().put(vertex, value, isInferred);
            return new AttributeImpl.Double(attVertex);
        }

        @Override
        public Attribute.Double get(double value) {
            AttributeVertex<java.lang.Double> attVertex = graphMgr.data().get(vertex, value);
            if (attVertex != null) return new AttributeImpl.Double(attVertex);
            else return null;
        }

        private static class Root extends AttributeTypeImpl.Double {

            private Root(GraphManager graphMgr, TypeVertex vertex) {
                super(graphMgr, vertex);
                assert vertex.label().equals(ATTRIBUTE.label());
            }

            @Override
            public boolean isRoot() { return true; }

            @Override
            public Stream<AttributeTypeImpl.Double> getSubtypes() {
                return super.getSubtypeVertices(DOUBLE).map(v -> AttributeTypeImpl.Double.of(graphMgr, v)).stream();
            }

            @Override
            public Stream<AttributeTypeImpl.Double> getSubtypesExplicit() {
                return super.getSubtypeVerticesDirect(DOUBLE).map(v -> AttributeTypeImpl.Double.of(graphMgr, v)).stream();
            }

            @Override
            public void setLabel(java.lang.String label) {
                throw exception(GraknException.of(ROOT_TYPE_MUTATION));
            }

            @Override
            public void unsetAbstract() {
                throw exception(GraknException.of(ROOT_TYPE_MUTATION));
            }

            @Override
            public void setSupertype(AttributeType superType) {
                throw exception(GraknException.of(ROOT_TYPE_MUTATION));
            }

            @Override
            public void setOwns(AttributeType attributeType, boolean isKey) {
                throw exception(GraknException.of(ROOT_TYPE_MUTATION));
            }

            @Override
            public void setOwns(AttributeType attributeType, AttributeType overriddenType, boolean isKey) {
                throw exception(GraknException.of(ROOT_TYPE_MUTATION));
            }

            @Override
            public void setPlays(RoleType roleType) {
                throw exception(GraknException.of(ROOT_TYPE_MUTATION));
            }

            @Override
            public void setPlays(RoleType roleType, RoleType overriddenType) {
                throw exception(GraknException.of(ROOT_TYPE_MUTATION));
            }

            @Override
            public void unsetPlays(RoleType roleType) {
                throw exception(GraknException.of(ROOT_TYPE_MUTATION));
            }
        }
    }

    public static class String extends AttributeTypeImpl implements AttributeType.String {

        public String(GraphManager graphMgr, java.lang.String label) {
            super(graphMgr, label, java.lang.String.class);
        }

        private String(GraphManager graphMgr, TypeVertex vertex) {
            super(graphMgr, vertex);
            if (!vertex.label().equals(ATTRIBUTE.label()) && !vertex.valueType().equals(STRING)) {
                throw exception(GraknException.of(VALUE_TYPE_MISMATCH, vertex.label(),
                                                  STRING.name(), vertex.valueType().name()));
            }
        }

        public static AttributeTypeImpl.String of(GraphManager graphMgr, TypeVertex vertex) {
            return vertex.label().equals(ATTRIBUTE.label()) ?
                    new Root(graphMgr, vertex) :
                    new AttributeTypeImpl.String(graphMgr, vertex);
        }

        @Override
        public Stream<AttributeTypeImpl.String> getSubtypes() {
            return super.getSubtypes(v -> AttributeTypeImpl.String.of(graphMgr, v));
        }

        @Override
        public Stream<AttributeTypeImpl.String> getSubtypesExplicit() {
            return super.getSubtypesExplicit(v -> AttributeTypeImpl.String.of(graphMgr, v));
        }

        @Override
        public Stream<AttributeImpl.String> getInstances() {
            return instances(v -> new AttributeImpl.String(v.asAttribute().asString()));
        }

        @Override
        public boolean isString() { return true; }

        @Override
        public AttributeTypeImpl.String asString() { return this; }

        @Override
        public void setRegex(Pattern regex) {
            if (regex != null) {
                getInstances().parallel().forEach(attribute -> {
                    Matcher matcher = regex.matcher(attribute.getValue());
                    if (!matcher.matches()) {
                        throw exception(GraknException.of(ATTRIBUTE_REGEX_UNSATISFIES_INSTANCES,
                                                          getLabel(), regex, attribute.getValue()));
                    }
                });
            }
            vertex.regex(regex);
        }

        @Override
        public void unsetRegex() {
            vertex.regex(null);
        }

        @Override
        public Pattern getRegex() {
            return vertex.regex();
        }

        @Override
        public Attribute.String put(java.lang.String value) {
            return put(value, false);
        }

        @Override
        public Attribute.String put(java.lang.String value, boolean isInferred) {
            validateIsCommittedAndNotAbstract(Attribute.class);
            if (vertex.regex() != null && !getRegex().matcher(value).matches()) {
                throw exception(GraknException.of(ATTRIBUTE_VALUE_UNSATISFIES_REGEX, getLabel(), value, getRegex()));
            }
            AttributeVertex<java.lang.String> attVertex = graphMgr.data().put(vertex, value, isInferred);
            return new AttributeImpl.String(attVertex);
        }

        @Override
        public Attribute.String get(java.lang.String value) {
            AttributeVertex<java.lang.String> attVertex = graphMgr.data().get(vertex, value);
            if (attVertex != null) return new AttributeImpl.String(attVertex);
            else return null;
        }

        @Override
        public ValueType getValueType() {
            return ValueType.STRING;
        }

        private static class Root extends AttributeTypeImpl.String {

            private Root(GraphManager graphMgr, TypeVertex vertex) {
                super(graphMgr, vertex);
                assert vertex.label().equals(ATTRIBUTE.label());
            }

            @Override
            public boolean isRoot() { return true; }

            @Override
            public Stream<AttributeTypeImpl.String> getSubtypes() {
                return super.getSubtypeVertices(STRING).map(v -> AttributeTypeImpl.String.of(graphMgr, v)).stream();
            }

            @Override
            public Stream<AttributeTypeImpl.String> getSubtypesExplicit() {
                return super.getSubtypeVerticesDirect(STRING).map(v -> AttributeTypeImpl.String.of(graphMgr, v)).stream();
            }

            @Override
            public void setLabel(java.lang.String label) {
                throw exception(GraknException.of(ROOT_TYPE_MUTATION));
            }

            @Override
            public void unsetAbstract() {
                throw exception(GraknException.of(ROOT_TYPE_MUTATION));
            }

            @Override
            public void setSupertype(AttributeType superType) {
                throw exception(GraknException.of(ROOT_TYPE_MUTATION));
            }

            @Override
            public void setOwns(AttributeType attributeType, boolean isKey) {
                throw exception(GraknException.of(ROOT_TYPE_MUTATION));
            }

            @Override
            public void setOwns(AttributeType attributeType, AttributeType overriddenType, boolean isKey) {
                throw exception(GraknException.of(ROOT_TYPE_MUTATION));
            }

            @Override
            public void setPlays(RoleType roleType) {
                throw exception(GraknException.of(ROOT_TYPE_MUTATION));
            }

            @Override
            public void setPlays(RoleType roleType, RoleType overriddenType) {
                throw exception(GraknException.of(ROOT_TYPE_MUTATION));
            }

            @Override
            public void unsetPlays(RoleType roleType) {
                throw exception(GraknException.of(ROOT_TYPE_MUTATION));
            }

            @Override
            public void setRegex(Pattern regex) {
                throw exception(GraknException.of(ROOT_TYPE_MUTATION));
            }

            @Override
            public void unsetRegex() {
                throw exception(GraknException.of(ROOT_TYPE_MUTATION));
            }
        }
    }

    public static class DateTime extends AttributeTypeImpl implements AttributeType.DateTime {

        public DateTime(GraphManager graphMgr, java.lang.String label) {
            super(graphMgr, label, LocalDateTime.class);
        }

        private DateTime(GraphManager graphMgr, TypeVertex vertex) {
            super(graphMgr, vertex);
            if (!vertex.label().equals(ATTRIBUTE.label()) && !vertex.valueType().equals(DATETIME)) {
                throw exception(GraknException.of(VALUE_TYPE_MISMATCH, vertex.label(),
                                                  DATETIME.name(), vertex.valueType().name()));
            }
        }

        public static AttributeTypeImpl.DateTime of(GraphManager graphMgr, TypeVertex vertex) {
            return vertex.label().equals(ATTRIBUTE.label()) ?
                    new Root(graphMgr, vertex) :
                    new AttributeTypeImpl.DateTime(graphMgr, vertex);
        }

        @Override
        public Stream<AttributeTypeImpl.DateTime> getSubtypes() {
            return super.getSubtypes(v -> AttributeTypeImpl.DateTime.of(graphMgr, v));
        }

        @Override
        public Stream<AttributeTypeImpl.DateTime> getSubtypesExplicit() {
            return super.getSubtypesExplicit(v -> AttributeTypeImpl.DateTime.of(graphMgr, v));
        }

        @Override
        public Stream<AttributeImpl.DateTime> getInstances() {
            return instances(v -> new AttributeImpl.DateTime(v.asAttribute().asDateTime()));
        }

        @Override
        public ValueType getValueType() {
            return ValueType.DATETIME;
        }

        @Override
        public boolean isDateTime() { return true; }

        @Override
        public AttributeTypeImpl.DateTime asDateTime() { return this; }

        @Override
        public Attribute.DateTime put(LocalDateTime value) {
            return put(value, false);
        }

        @Override
        public Attribute.DateTime put(LocalDateTime value, boolean isInferred) {
            validateIsCommittedAndNotAbstract(Attribute.class);
            AttributeVertex<LocalDateTime> attVertex = graphMgr.data().put(vertex, value, isInferred);
            if (!isInferred && attVertex.isInferred()) attVertex.isInferred(false);
            return new AttributeImpl.DateTime(attVertex);
        }

        @Override
        public Attribute.DateTime get(LocalDateTime value) {
            AttributeVertex<java.time.LocalDateTime> attVertex = graphMgr.data().get(vertex, value);
            if (attVertex != null) return new AttributeImpl.DateTime(attVertex);
            else return null;
        }

        private static class Root extends AttributeTypeImpl.DateTime {

            private Root(GraphManager graphMgr, TypeVertex vertex) {
                super(graphMgr, vertex);
                assert vertex.label().equals(ATTRIBUTE.label());
            }

            @Override
            public boolean isRoot() { return true; }

            @Override
            public Stream<AttributeTypeImpl.DateTime> getSubtypes() {
                return super.getSubtypeVertices(DATETIME).map(v -> AttributeTypeImpl.DateTime.of(graphMgr, v)).stream();
            }

            @Override
            public Stream<AttributeTypeImpl.DateTime> getSubtypesExplicit() {
                return super.getSubtypeVerticesDirect(DATETIME).map(v -> AttributeTypeImpl.DateTime.of(graphMgr, v)).stream();
            }

            @Override
            public void setLabel(java.lang.String label) {
                throw exception(GraknException.of(ROOT_TYPE_MUTATION));
            }

            @Override
            public void unsetAbstract() {
                throw exception(GraknException.of(ROOT_TYPE_MUTATION));
            }

            @Override
            public void setSupertype(AttributeType superType) {
                throw exception(GraknException.of(ROOT_TYPE_MUTATION));
            }

            @Override
            public void setOwns(AttributeType attributeType, boolean isKey) {
                throw exception(GraknException.of(ROOT_TYPE_MUTATION));
            }

            @Override
            public void setOwns(AttributeType attributeType, AttributeType overriddenType, boolean isKey) {
                throw exception(GraknException.of(ROOT_TYPE_MUTATION));
            }

            @Override
            public void setPlays(RoleType roleType) {
                throw exception(GraknException.of(ROOT_TYPE_MUTATION));
            }

            @Override
            public void setPlays(RoleType roleType, RoleType overriddenType) {
                throw exception(GraknException.of(ROOT_TYPE_MUTATION));
            }

            @Override
            public void unsetPlays(RoleType roleType) {
                throw exception(GraknException.of(ROOT_TYPE_MUTATION));
            }
        }
    }
}
