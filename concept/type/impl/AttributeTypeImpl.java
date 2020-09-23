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

package grakn.core.concept.type.impl;

import grakn.core.common.exception.GraknException;
import grakn.core.common.iterator.Iterators;
import grakn.core.concept.thing.Attribute;
import grakn.core.concept.thing.impl.AttributeImpl;
import grakn.core.concept.type.AttributeType;
import grakn.core.concept.type.RoleType;
import grakn.core.graph.Graphs;
import grakn.core.graph.util.Encoding;
import grakn.core.graph.vertex.AttributeVertex;
import grakn.core.graph.vertex.TypeVertex;

import javax.annotation.Nullable;
import java.time.LocalDateTime;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static grakn.common.util.Objects.className;
import static grakn.core.common.exception.ErrorMessage.Internal.UNRECOGNISED_VALUE;
import static grakn.core.common.exception.ErrorMessage.ThingWrite.ATTRIBUTE_VALUE_UNSATISFIES_REGEX;
import static grakn.core.common.exception.ErrorMessage.ThingWrite.ILLEGAL_STRING_SIZE;
import static grakn.core.common.exception.ErrorMessage.TypeRead.INVALID_TYPE_CASTING;
import static grakn.core.common.exception.ErrorMessage.TypeRead.TYPE_ROOT_MISMATCH;
import static grakn.core.common.exception.ErrorMessage.TypeRead.VALUE_TYPE_MISMATCH;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.ATTRIBUTE_REGEX_UNSATISFIES_INSTANCES;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.ATTRIBUTE_SUBTYPE_NOT_ABSTRACT;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.ATTRIBUTE_SUPERTYPE_NOT_ABSTRACT;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.ATTRIBUTE_SUPERTYPE_VALUE_TYPE;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.ROOT_TYPE_MUTATION;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.SUPERTYPE_SELF;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.TYPE_HAS_INSTANCES;
import static grakn.core.common.iterator.Iterators.apply;
import static grakn.core.common.iterator.Iterators.link;
import static grakn.core.common.iterator.Iterators.stream;

public abstract class AttributeTypeImpl extends ThingTypeImpl implements AttributeType {

    private AttributeTypeImpl(Graphs graphs, TypeVertex vertex) {
        super(graphs, vertex);
        if (vertex.encoding() != Encoding.Vertex.Type.ATTRIBUTE_TYPE) {
            throw exception(TYPE_ROOT_MISMATCH.message(
                    vertex.label(),
                    Encoding.Vertex.Type.ATTRIBUTE_TYPE.root().label(),
                    vertex.encoding().root().label()
            ));
        }
    }

    private AttributeTypeImpl(Graphs graphs, java.lang.String label, Class<?> valueType) {
        super(graphs, label, Encoding.Vertex.Type.ATTRIBUTE_TYPE);
        vertex.valueType(Encoding.ValueType.of(valueType));
    }

    public static AttributeTypeImpl of(Graphs graphs, TypeVertex vertex) {
        switch (vertex.valueType()) {
            case OBJECT:
                return new AttributeTypeImpl.Root(graphs, vertex);
            case BOOLEAN:
                return AttributeTypeImpl.Boolean.of(graphs, vertex);
            case LONG:
                return AttributeTypeImpl.Long.of(graphs, vertex);
            case DOUBLE:
                return AttributeTypeImpl.Double.of(graphs, vertex);
            case STRING:
                return AttributeTypeImpl.String.of(graphs, vertex);
            case DATETIME:
                return AttributeTypeImpl.DateTime.of(graphs, vertex);
            default:
                throw new GraknException(UNRECOGNISED_VALUE);
        }
    }

    @Override
    public void setAbstract() {
        if (getSubtypes().filter(sub -> !sub.equals(this)).anyMatch(sub -> !sub.isAbstract())) {
            throw exception(ATTRIBUTE_SUBTYPE_NOT_ABSTRACT.message(getLabel()));
        } else if (getInstances().findFirst().isPresent()) {
            throw exception(TYPE_HAS_INSTANCES.message(getLabel()));
        }
        vertex.isAbstract(true);
    }

    @Nullable
    @Override
    public abstract AttributeTypeImpl getSupertype();

    @Override
    public abstract Stream<? extends AttributeTypeImpl> getSupertypes();

    @Override
    public abstract Stream<? extends AttributeTypeImpl> getSubtypes();

    @Override
    public abstract Stream<? extends AttributeImpl<?>> getInstances();

    Iterator<TypeVertex> subTypeVertices(Encoding.ValueType valueType) {
        return Iterators.tree(vertex, v -> Iterators.filter(v.ins().edge(Encoding.Edge.Type.SUB).from(),
                                                            sv -> sv.valueType().equals(valueType)));
    }

    @Override
    public void setSupertype(AttributeType superType) {
        if (!superType.isRoot() && !Objects.equals(this.getValueType(), superType.getValueType())) {
            throw exception(ATTRIBUTE_SUPERTYPE_VALUE_TYPE.message(
                    getLabel(), getValueType().name(), superType.getLabel(), superType.getValueType().name()
            ));
        } else if (this.equals(superType)) {
            throw exception(SUPERTYPE_SELF.message(getLabel()));
        } else if (!superType.isAbstract()) {
            throw exception(ATTRIBUTE_SUPERTYPE_NOT_ABSTRACT.message(superType.getLabel()));
        }
        vertex.outs().edge(Encoding.Edge.Type.SUB, getSupertype().vertex).delete();
        vertex.outs().put(Encoding.Edge.Type.SUB, ((AttributeTypeImpl) superType).vertex);
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
            return stream(apply(vertex.ins().edge(Encoding.Edge.Type.OWNS_KEY).from(), v -> ThingTypeImpl.of(graphs, v)));
        } else {
            return stream(apply(link(
                    vertex.ins().edge(Encoding.Edge.Type.OWNS_KEY).from(),
                    vertex.ins().edge(Encoding.Edge.Type.OWNS).from()
            ), v -> ThingTypeImpl.of(graphs, v)));
        }
    }

    @Override
    public List<GraknException> validate() {
        return super.validate();
    }

    @Override
    public AttributeTypeImpl asAttributeType() { return this; }

    @Override
    public AttributeTypeImpl.Boolean asBoolean() {
        throw exception(INVALID_TYPE_CASTING.message(className(AttributeType.Boolean.class)));
    }

    @Override
    public AttributeTypeImpl.Long asLong() {
        throw exception(INVALID_TYPE_CASTING.message(className(AttributeType.Long.class)));
    }

    @Override
    public AttributeTypeImpl.Double asDouble() {
        throw exception(INVALID_TYPE_CASTING.message(className(AttributeType.Double.class)));
    }

    @Override
    public AttributeTypeImpl.String asString() {
        throw exception(INVALID_TYPE_CASTING.message(className(AttributeType.String.class)));
    }

    @Override
    public AttributeTypeImpl.DateTime asDateTime() {
        throw exception(INVALID_TYPE_CASTING.message(className(AttributeType.DateTime.class)));
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

        private Root(Graphs graphs, TypeVertex vertex) {
            super(graphs, vertex);
            assert vertex.valueType().equals(Encoding.ValueType.OBJECT);
            assert vertex.label().equals(Encoding.Vertex.Type.Root.ATTRIBUTE.label());
        }

        public ValueType getValueType() { return ValueType.OBJECT; }

        @Override
        public AttributeTypeImpl.Boolean asBoolean() { return AttributeTypeImpl.Boolean.of(graphs, vertex); }

        @Override
        public AttributeTypeImpl.Long asLong() { return AttributeTypeImpl.Long.of(graphs, vertex); }

        @Override
        public AttributeTypeImpl.Double asDouble() { return AttributeTypeImpl.Double.of(graphs, vertex); }

        @Override
        public AttributeTypeImpl.String asString() { return AttributeTypeImpl.String.of(graphs, vertex); }

        @Override
        public AttributeTypeImpl.DateTime asDateTime() { return AttributeTypeImpl.DateTime.of(graphs, vertex); }

        @Override
        public boolean isRoot() { return true; }

        @Override
        public void setLabel(java.lang.String label) { throw exception(ROOT_TYPE_MUTATION.message()); }

        @Override
        public void unsetAbstract() { throw exception(ROOT_TYPE_MUTATION.message()); }

        @Override
        public void setSupertype(AttributeType superType) { throw exception(ROOT_TYPE_MUTATION.message()); }

        @Nullable
        @Override
        public AttributeTypeImpl getSupertype() {
            return null;
        }

        @Override
        public Stream<AttributeTypeImpl> getSupertypes() {
            return Stream.of(this);
        }

        @Override
        public Stream<AttributeTypeImpl> getSubtypes() {
            return getSubtypes(v -> {
                switch (v.valueType()) {
                    case OBJECT:
                        assert this.vertex == v;
                        return this;
                    case BOOLEAN:
                        return AttributeTypeImpl.Boolean.of(graphs, v);
                    case LONG:
                        return AttributeTypeImpl.Long.of(graphs, v);
                    case DOUBLE:
                        return AttributeTypeImpl.Double.of(graphs, v);
                    case STRING:
                        return AttributeTypeImpl.String.of(graphs, v);
                    case DATETIME:
                        return AttributeTypeImpl.DateTime.of(graphs, v);
                    default:
                        throw exception(UNRECOGNISED_VALUE.message());
                }
            });
        }

        @Override
        public Stream<AttributeImpl<?>> getInstances() {
            return super.instances(v -> AttributeImpl.of(v.asAttribute()));
        }

        @Override
        public void setOwns(AttributeType attributeType, boolean isKey) {
            throw exception(ROOT_TYPE_MUTATION.message());
        }

        @Override
        public void setOwns(AttributeType attributeType, AttributeType overriddenType, boolean isKey) {
            throw exception(ROOT_TYPE_MUTATION.message());
        }

        @Override
        public void setPlays(RoleType roleType) {
            throw exception(ROOT_TYPE_MUTATION.message());
        }

        @Override
        public void setPlays(RoleType roleType, RoleType overriddenType) {
            throw exception(ROOT_TYPE_MUTATION.message());
        }

        @Override
        public void unsetPlays(RoleType roleType) {
            throw exception(ROOT_TYPE_MUTATION.message());
        }
    }

    public static class Boolean extends AttributeTypeImpl implements AttributeType.Boolean {

        public Boolean(Graphs graphs, java.lang.String label) {
            super(graphs, label, java.lang.Boolean.class);
        }

        private Boolean(Graphs graphs, TypeVertex vertex) {
            super(graphs, vertex);
            if (!vertex.label().equals(Encoding.Vertex.Type.Root.ATTRIBUTE.label()) &&
                    !vertex.valueType().equals(Encoding.ValueType.BOOLEAN)) {
                throw exception(VALUE_TYPE_MISMATCH.message(
                        vertex.label(),
                        Encoding.ValueType.BOOLEAN.name(),
                        vertex.valueType().name()
                ));
            }
        }

        public static AttributeTypeImpl.Boolean of(Graphs graphs, TypeVertex vertex) {
            return vertex.label().equals(Encoding.Vertex.Type.Root.ATTRIBUTE.label()) ?
                    new Root(graphs, vertex) :
                    new AttributeTypeImpl.Boolean(graphs, vertex);
        }

        @Nullable
        @Override
        public AttributeTypeImpl.Boolean getSupertype() {
            return super.getSupertype(v -> AttributeTypeImpl.Boolean.of(graphs, v));
        }

        @Override
        public Stream<AttributeTypeImpl.Boolean> getSupertypes() {
            return super.getSupertypes(v -> AttributeTypeImpl.Boolean.of(graphs, v));
        }

        @Override
        public Stream<AttributeTypeImpl.Boolean> getSubtypes() {
            return super.getSubtypes(v -> AttributeTypeImpl.Boolean.of(graphs, v));
        }

        @Override
        public Stream<AttributeImpl.Boolean> getInstances() {
            return super.instances(v -> new AttributeImpl.Boolean(v.asAttribute().asBoolean()));
        }

        @Override
        public ValueType getValueType() { return ValueType.BOOLEAN; }

        @Override
        public AttributeTypeImpl.Boolean asBoolean() { return this; }

        @Override
        public Attribute.Boolean put(boolean value) {
            return put(value, false);
        }

        @Override
        public Attribute.Boolean put(boolean value, boolean isInferred) {
            validateIsCommittedAndNotAbstract(Attribute.class);
            AttributeVertex<java.lang.Boolean> attVertex = graphs.data().put(vertex, value, isInferred);
            return new AttributeImpl.Boolean(attVertex);
        }

        @Override
        public Attribute.Boolean get(boolean value) {
            AttributeVertex<java.lang.Boolean> attVertex = graphs.data().get(vertex, value);
            if (attVertex != null) return new AttributeImpl.Boolean(attVertex);
            else return null;
        }

        private static class Root extends AttributeTypeImpl.Boolean {

            private Root(Graphs graphs, TypeVertex vertex) {
                super(graphs, vertex);
                assert vertex.label().equals(Encoding.Vertex.Type.Root.ATTRIBUTE.label());
            }

            @Override
            public boolean isRoot() { return true; }

            @Override
            public Stream<AttributeTypeImpl.Boolean> getSubtypes() {
                return stream(apply(
                        super.subTypeVertices(Encoding.ValueType.BOOLEAN),
                        v -> AttributeTypeImpl.Boolean.of(graphs, v)
                ));
            }

            @Override
            public void setLabel(java.lang.String label) {
                throw exception(ROOT_TYPE_MUTATION.message());
            }

            @Override
            public void unsetAbstract() {
                throw exception(ROOT_TYPE_MUTATION.message());
            }

            @Override
            public void setSupertype(AttributeType superType) {
                throw exception(ROOT_TYPE_MUTATION.message());
            }

            @Override
            public void setOwns(AttributeType attributeType, boolean isKey) {
                throw exception(ROOT_TYPE_MUTATION.message());
            }

            @Override
            public void setOwns(AttributeType attributeType, AttributeType overriddenType, boolean isKey) {
                throw exception(ROOT_TYPE_MUTATION.message());
            }

            @Override
            public void setPlays(RoleType roleType) {
                throw exception(ROOT_TYPE_MUTATION.message());
            }

            @Override
            public void setPlays(RoleType roleType, RoleType overriddenType) {
                throw exception(ROOT_TYPE_MUTATION.message());
            }

            @Override
            public void unsetPlays(RoleType roleType) {
                throw exception(ROOT_TYPE_MUTATION.message());
            }
        }
    }

    public static class Long extends AttributeTypeImpl implements AttributeType.Long {

        public Long(Graphs graphs, java.lang.String label) {
            super(graphs, label, java.lang.Long.class);
        }

        private Long(Graphs graphs, TypeVertex vertex) {
            super(graphs, vertex);
            if (!vertex.label().equals(Encoding.Vertex.Type.Root.ATTRIBUTE.label()) &&
                    !vertex.valueType().equals(Encoding.ValueType.LONG)) {
                throw exception(VALUE_TYPE_MISMATCH.message(
                        vertex.label(),
                        Encoding.ValueType.LONG.name(),
                        vertex.valueType().name()
                ));
            }
        }

        public static AttributeTypeImpl.Long of(Graphs graphs, TypeVertex vertex) {
            return vertex.label().equals(Encoding.Vertex.Type.Root.ATTRIBUTE.label()) ?
                    new Root(graphs, vertex) :
                    new AttributeTypeImpl.Long(graphs, vertex);
        }

        @Nullable
        @Override
        public AttributeTypeImpl.Long getSupertype() {
            return super.getSupertype(v -> AttributeTypeImpl.Long.of(graphs, v));
        }

        @Override
        public Stream<AttributeTypeImpl.Long> getSupertypes() {
            return super.getSupertypes(v -> AttributeTypeImpl.Long.of(graphs, v));
        }

        @Override
        public Stream<AttributeTypeImpl.Long> getSubtypes() {
            return super.getSubtypes(v -> AttributeTypeImpl.Long.of(graphs, v));
        }

        @Override
        public Stream<AttributeImpl.Long> getInstances() {
            return super.instances(v -> new AttributeImpl.Long(v.asAttribute().asLong()));
        }

        @Override
        public ValueType getValueType() {
            return ValueType.LONG;
        }

        @Override
        public AttributeTypeImpl.Long asLong() { return this; }

        @Override
        public Attribute.Long put(long value) {
            return put(value, false);
        }

        @Override
        public Attribute.Long put(long value, boolean isInferred) {
            validateIsCommittedAndNotAbstract(Attribute.class);
            AttributeVertex<java.lang.Long> attVertex = graphs.data().put(vertex, value, isInferred);
            return new AttributeImpl.Long(attVertex);
        }

        @Override
        public Attribute.Long get(long value) {
            AttributeVertex<java.lang.Long> attVertex = graphs.data().get(vertex, value);
            if (attVertex != null) return new AttributeImpl.Long(attVertex);
            else return null;
        }

        private static class Root extends AttributeTypeImpl.Long {

            private Root(Graphs graphs, TypeVertex vertex) {
                super(graphs, vertex);
                assert vertex.label().equals(Encoding.Vertex.Type.Root.ATTRIBUTE.label());
            }

            @Override
            public boolean isRoot() { return true; }

            @Override
            public Stream<AttributeTypeImpl.Long> getSubtypes() {
                return stream(apply(
                        super.subTypeVertices(Encoding.ValueType.LONG),
                        v -> AttributeTypeImpl.Long.of(graphs, v)
                ));
            }

            @Override
            public void setLabel(java.lang.String label) {
                throw exception(ROOT_TYPE_MUTATION.message());
            }

            @Override
            public void unsetAbstract() {
                throw exception(ROOT_TYPE_MUTATION.message());
            }

            @Override
            public void setSupertype(AttributeType superType) {
                throw exception(ROOT_TYPE_MUTATION.message());
            }

            @Override
            public void setOwns(AttributeType attributeType, boolean isKey) {
                throw exception(ROOT_TYPE_MUTATION.message());
            }

            @Override
            public void setOwns(AttributeType attributeType, AttributeType overriddenType, boolean isKey) {
                throw exception(ROOT_TYPE_MUTATION.message());
            }

            @Override
            public void setPlays(RoleType roleType) {
                throw exception(ROOT_TYPE_MUTATION.message());
            }

            @Override
            public void setPlays(RoleType roleType, RoleType overriddenType) {
                throw exception(ROOT_TYPE_MUTATION.message());
            }

            @Override
            public void unsetPlays(RoleType roleType) {
                throw exception(ROOT_TYPE_MUTATION.message());
            }
        }
    }

    public static class Double extends AttributeTypeImpl implements AttributeType.Double {

        public Double(Graphs graphs, java.lang.String label) {
            super(graphs, label, java.lang.Double.class);
        }

        private Double(Graphs graphs, TypeVertex vertex) {
            super(graphs, vertex);
            if (!vertex.label().equals(Encoding.Vertex.Type.Root.ATTRIBUTE.label()) &&
                    !vertex.valueType().equals(Encoding.ValueType.DOUBLE)) {
                throw exception(VALUE_TYPE_MISMATCH.message(
                        vertex.label(),
                        Encoding.ValueType.DOUBLE.name(),
                        vertex.valueType().name()
                ));
            }
        }

        public static AttributeTypeImpl.Double of(Graphs graphs, TypeVertex vertex) {
            return vertex.label().equals(Encoding.Vertex.Type.Root.ATTRIBUTE.label()) ?
                    new Root(graphs, vertex) :
                    new AttributeTypeImpl.Double(graphs, vertex);
        }

        @Nullable
        @Override
        public AttributeTypeImpl.Double getSupertype() {
            return super.getSupertype(v -> AttributeTypeImpl.Double.of(graphs, v));
        }

        @Override
        public Stream<AttributeTypeImpl.Double> getSupertypes() {
            return super.getSupertypes(v -> AttributeTypeImpl.Double.of(graphs, v));
        }

        @Override
        public Stream<AttributeTypeImpl.Double> getSubtypes() {
            return super.getSubtypes(v -> AttributeTypeImpl.Double.of(graphs, v));
        }

        @Override
        public Stream<AttributeImpl.Double> getInstances() {
            return super.instances(v -> new AttributeImpl.Double(v.asAttribute().asDouble()));
        }

        @Override
        public ValueType getValueType() {
            return ValueType.DOUBLE;
        }

        @Override
        public AttributeTypeImpl.Double asDouble() { return this; }

        @Override
        public Attribute.Double put(double value) {
            return put(value, false);
        }

        @Override
        public Attribute.Double put(double value, boolean isInferred) {
            validateIsCommittedAndNotAbstract(Attribute.class);
            AttributeVertex<java.lang.Double> attVertex = graphs.data().put(vertex, value, isInferred);
            return new AttributeImpl.Double(attVertex);
        }

        @Override
        public Attribute.Double get(double value) {
            AttributeVertex<java.lang.Double> attVertex = graphs.data().get(vertex, value);
            if (attVertex != null) return new AttributeImpl.Double(attVertex);
            else return null;
        }

        private static class Root extends AttributeTypeImpl.Double {

            private Root(Graphs graphs, TypeVertex vertex) {
                super(graphs, vertex);
                assert vertex.label().equals(Encoding.Vertex.Type.Root.ATTRIBUTE.label());
            }

            @Override
            public boolean isRoot() { return true; }

            @Override
            public Stream<AttributeTypeImpl.Double> getSubtypes() {
                return stream(apply(
                        super.subTypeVertices(Encoding.ValueType.DOUBLE),
                        v -> AttributeTypeImpl.Double.of(graphs, v)
                ));
            }

            @Override
            public void setLabel(java.lang.String label) {
                throw exception(ROOT_TYPE_MUTATION.message());
            }

            @Override
            public void unsetAbstract() {
                throw exception(ROOT_TYPE_MUTATION.message());
            }

            @Override
            public void setSupertype(AttributeType superType) {
                throw exception(ROOT_TYPE_MUTATION.message());
            }

            @Override
            public void setOwns(AttributeType attributeType, boolean isKey) {
                throw exception(ROOT_TYPE_MUTATION.message());
            }

            @Override
            public void setOwns(AttributeType attributeType, AttributeType overriddenType, boolean isKey) {
                throw exception(ROOT_TYPE_MUTATION.message());
            }

            @Override
            public void setPlays(RoleType roleType) {
                throw exception(ROOT_TYPE_MUTATION.message());
            }

            @Override
            public void setPlays(RoleType roleType, RoleType overriddenType) {
                throw exception(ROOT_TYPE_MUTATION.message());
            }

            @Override
            public void unsetPlays(RoleType roleType) {
                throw exception(ROOT_TYPE_MUTATION.message());
            }
        }
    }

    public static class String extends AttributeTypeImpl implements AttributeType.String {

        private Pattern regexPattern;

        public String(Graphs graphs, java.lang.String label) {
            super(graphs, label, java.lang.String.class);
        }

        private String(Graphs graphs, TypeVertex vertex) {
            super(graphs, vertex);
            if (!vertex.label().equals(Encoding.Vertex.Type.Root.ATTRIBUTE.label()) &&
                    !vertex.valueType().equals(Encoding.ValueType.STRING)) {
                throw exception(VALUE_TYPE_MISMATCH.message(
                        vertex.label(),
                        Encoding.ValueType.STRING.name(),
                        vertex.valueType().name()
                ));
            }
        }

        public static AttributeTypeImpl.String of(Graphs graphs, TypeVertex vertex) {
            return vertex.label().equals(Encoding.Vertex.Type.Root.ATTRIBUTE.label()) ?
                    new Root(graphs, vertex) :
                    new AttributeTypeImpl.String(graphs, vertex);
        }

        @Nullable
        @Override
        public AttributeTypeImpl.String getSupertype() {
            return super.getSupertype(v -> AttributeTypeImpl.String.of(graphs, v));
        }

        @Override
        public Stream<AttributeTypeImpl.String> getSupertypes() {
            return super.getSupertypes(v -> AttributeTypeImpl.String.of(graphs, v));
        }

        @Override
        public Stream<AttributeTypeImpl.String> getSubtypes() {
            return super.getSubtypes(v -> AttributeTypeImpl.String.of(graphs, v));
        }

        @Override
        public Stream<AttributeImpl.String> getInstances() {
            return super.instances(v -> new AttributeImpl.String(v.asAttribute().asString()));
        }

        @Override
        public AttributeTypeImpl.String asString() { return this; }

        @Override
        public void setRegex(Pattern regex) {
            if (regex != null) {
                getInstances().parallel().forEach(attribute -> {
                    Matcher matcher = regex.matcher(attribute.getValue());
                    if (!matcher.matches()) {
                        throw exception(ATTRIBUTE_REGEX_UNSATISFIES_INSTANCES.message(getLabel(), regex, attribute.getValue()));
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
                throw exception(ATTRIBUTE_VALUE_UNSATISFIES_REGEX.message(getLabel(), value, getRegex()));
            } else if (value.length() > Encoding.STRING_MAX_LENGTH) {
                throw exception(ILLEGAL_STRING_SIZE.message());
            }
            AttributeVertex<java.lang.String> attVertex = graphs.data().put(vertex, value, isInferred);
            return new AttributeImpl.String(attVertex);
        }

        @Override
        public Attribute.String get(java.lang.String value) {
            AttributeVertex<java.lang.String> attVertex = graphs.data().get(vertex, value);
            if (attVertex != null) return new AttributeImpl.String(attVertex);
            else return null;
        }

        @Override
        public ValueType getValueType() {
            return ValueType.STRING;
        }

        private static class Root extends AttributeTypeImpl.String {

            private Root(Graphs graphs, TypeVertex vertex) {
                super(graphs, vertex);
                assert vertex.label().equals(Encoding.Vertex.Type.Root.ATTRIBUTE.label());
            }

            @Override
            public boolean isRoot() { return true; }

            @Override
            public Stream<AttributeTypeImpl.String> getSubtypes() {
                return stream(apply(
                        super.subTypeVertices(Encoding.ValueType.STRING),
                        v -> AttributeTypeImpl.String.of(graphs, v)
                ));
            }

            @Override
            public void setLabel(java.lang.String label) {
                throw exception(ROOT_TYPE_MUTATION.message());
            }

            @Override
            public void unsetAbstract() {
                throw exception(ROOT_TYPE_MUTATION.message());
            }

            @Override
            public void setSupertype(AttributeType superType) {
                throw exception(ROOT_TYPE_MUTATION.message());
            }

            @Override
            public void setOwns(AttributeType attributeType, boolean isKey) {
                throw exception(ROOT_TYPE_MUTATION.message());
            }

            @Override
            public void setOwns(AttributeType attributeType, AttributeType overriddenType, boolean isKey) {
                throw exception(ROOT_TYPE_MUTATION.message());
            }

            @Override
            public void setPlays(RoleType roleType) {
                throw exception(ROOT_TYPE_MUTATION.message());
            }

            @Override
            public void setPlays(RoleType roleType, RoleType overriddenType) {
                throw exception(ROOT_TYPE_MUTATION.message());
            }

            @Override
            public void unsetPlays(RoleType roleType) {
                throw exception(ROOT_TYPE_MUTATION.message());
            }

            @Override
            public void setRegex(Pattern regex) {
                throw exception(ROOT_TYPE_MUTATION.message());
            }

            @Override
            public void unsetRegex() {
                throw exception(ROOT_TYPE_MUTATION.message());
            }
        }
    }

    public static class DateTime extends AttributeTypeImpl implements AttributeType.DateTime {

        public DateTime(Graphs graphs, java.lang.String label) {
            super(graphs, label, LocalDateTime.class);
        }

        private DateTime(Graphs graphs, TypeVertex vertex) {
            super(graphs, vertex);
            if (!vertex.label().equals(Encoding.Vertex.Type.Root.ATTRIBUTE.label()) &&
                    !vertex.valueType().equals(Encoding.ValueType.DATETIME)) {
                throw exception(VALUE_TYPE_MISMATCH.message(
                        vertex.label(),
                        Encoding.ValueType.DATETIME.name(),
                        vertex.valueType().name()
                ));
            }
        }

        public static AttributeTypeImpl.DateTime of(Graphs graphs, TypeVertex vertex) {
            return vertex.label().equals(Encoding.Vertex.Type.Root.ATTRIBUTE.label()) ?
                    new Root(graphs, vertex) :
                    new AttributeTypeImpl.DateTime(graphs, vertex);
        }

        @Nullable
        @Override
        public AttributeTypeImpl.DateTime getSupertype() {
            return super.getSupertype(v -> AttributeTypeImpl.DateTime.of(graphs, v));
        }

        @Override
        public Stream<AttributeTypeImpl.DateTime> getSupertypes() {
            return super.getSupertypes(v -> AttributeTypeImpl.DateTime.of(graphs, v));
        }

        @Override
        public Stream<AttributeTypeImpl.DateTime> getSubtypes() {
            return super.getSubtypes(v -> AttributeTypeImpl.DateTime.of(graphs, v));
        }

        @Override
        public Stream<AttributeImpl.DateTime> getInstances() {
            return super.instances(v -> new AttributeImpl.DateTime(v.asAttribute().asDateTime()));
        }

        @Override
        public ValueType getValueType() {
            return ValueType.DATETIME;
        }

        @Override
        public AttributeTypeImpl.DateTime asDateTime() { return this; }

        @Override
        public Attribute.DateTime put(LocalDateTime value) {
            return put(value, false);
        }

        @Override
        public Attribute.DateTime put(LocalDateTime value, boolean isInferred) {
            validateIsCommittedAndNotAbstract(Attribute.class);
            AttributeVertex<LocalDateTime> attVertex = graphs.data().put(vertex, value, isInferred);
            if (!isInferred && attVertex.isInferred()) attVertex.isInferred(false);
            return new AttributeImpl.DateTime(attVertex);
        }

        @Override
        public Attribute.DateTime get(LocalDateTime value) {
            AttributeVertex<java.time.LocalDateTime> attVertex = graphs.data().get(vertex, value);
            if (attVertex != null) return new AttributeImpl.DateTime(attVertex);
            else return null;
        }

        private static class Root extends AttributeTypeImpl.DateTime {

            private Root(Graphs graphs, TypeVertex vertex) {
                super(graphs, vertex);
                assert vertex.label().equals(Encoding.Vertex.Type.Root.ATTRIBUTE.label());
            }

            @Override
            public boolean isRoot() { return true; }

            @Override
            public Stream<AttributeTypeImpl.DateTime> getSubtypes() {
                return stream(apply(
                        super.subTypeVertices(Encoding.ValueType.DATETIME),
                        v -> AttributeTypeImpl.DateTime.of(graphs, v)
                ));
            }

            @Override
            public void setLabel(java.lang.String label) {
                throw exception(ROOT_TYPE_MUTATION.message());
            }

            @Override
            public void unsetAbstract() {
                throw exception(ROOT_TYPE_MUTATION.message());
            }

            @Override
            public void setSupertype(AttributeType superType) {
                throw exception(ROOT_TYPE_MUTATION.message());
            }

            @Override
            public void setOwns(AttributeType attributeType, boolean isKey) {
                throw exception(ROOT_TYPE_MUTATION.message());
            }

            @Override
            public void setOwns(AttributeType attributeType, AttributeType overriddenType, boolean isKey) {
                throw exception(ROOT_TYPE_MUTATION.message());
            }

            @Override
            public void setPlays(RoleType roleType) {
                throw exception(ROOT_TYPE_MUTATION.message());
            }

            @Override
            public void setPlays(RoleType roleType, RoleType overriddenType) {
                throw exception(ROOT_TYPE_MUTATION.message());
            }

            @Override
            public void unsetPlays(RoleType roleType) {
                throw exception(ROOT_TYPE_MUTATION.message());
            }
        }
    }
}
