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
import grakn.core.graph.TypeGraph;
import grakn.core.graph.util.Schema;
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

import static grakn.core.common.exception.ErrorMessage.Internal.UNRECOGNISED_VALUE;
import static grakn.core.common.exception.ErrorMessage.ThingWrite.ATTRIBUTE_VALUE_UNSATISFIES_REGEX;
import static grakn.core.common.exception.ErrorMessage.ThingWrite.ILLEGAL_STRING_SIZE;
import static grakn.core.common.exception.ErrorMessage.TypeRead.INVALID_TYPE_CASTING;
import static grakn.core.common.exception.ErrorMessage.TypeRead.TYPE_ROOT_MISMATCH;
import static grakn.core.common.exception.ErrorMessage.TypeRead.VALUE_TYPE_MISMATCH;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.ATTRIBUTE_REGEX_UNSATISFIES_INSTANCES;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.ATTRIBUTE_SUPERTYPE_NOT_ABSTRACT;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.ATTRIBUTE_SUPERTYPE_VALUE_TYPE;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.ROOT_TYPE_MUTATION;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.SUPERTYPE_SELF;
import static grakn.core.common.iterator.Iterators.apply;
import static grakn.core.common.iterator.Iterators.link;
import static grakn.core.common.iterator.Iterators.stream;

public abstract class AttributeTypeImpl extends ThingTypeImpl implements AttributeType {

    private AttributeTypeImpl(TypeVertex vertex) {
        super(vertex);
        if (vertex.schema() != Schema.Vertex.Type.ATTRIBUTE_TYPE) {
            throw new GraknException(TYPE_ROOT_MISMATCH.message(
                    vertex.label(),
                    Schema.Vertex.Type.ATTRIBUTE_TYPE.root().label(),
                    vertex.schema().root().label()
            ));
        }
    }

    private AttributeTypeImpl(TypeGraph graph, java.lang.String label, Class<?> valueType) {
        super(graph, label, Schema.Vertex.Type.ATTRIBUTE_TYPE);
        vertex.valueType(Schema.ValueType.of(valueType));
    }

    public static AttributeTypeImpl of(TypeVertex vertex) {
        switch (vertex.valueType()) {
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
                throw new GraknException(UNRECOGNISED_VALUE);
        }
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

    Iterator<TypeVertex> subTypeVertices(Schema.ValueType valueType) {
        return Iterators.tree(vertex, v -> Iterators.filter(v.ins().edge(Schema.Edge.Type.SUB).from(),
                                                            sv -> sv.valueType().equals(valueType)));
    }

    @Override
    public void setSupertype(AttributeType superType) {
        if (!superType.isRoot() && !Objects.equals(this.getValueType(), superType.getValueType())) {
            throw new GraknException(ATTRIBUTE_SUPERTYPE_VALUE_TYPE.message(
                    getLabel(), getValueType().name(), superType.getLabel(), superType.getValueType().name()
            ));
        } else if (this.equals(superType)) {
            throw new GraknException(SUPERTYPE_SELF.message(getLabel()));
        } else if (!superType.isAbstract()) {
            throw new GraknException(ATTRIBUTE_SUPERTYPE_NOT_ABSTRACT.message(superType.getLabel()));
        }
        vertex.outs().edge(Schema.Edge.Type.SUB, getSupertype().vertex).delete();
        vertex.outs().put(Schema.Edge.Type.SUB, ((AttributeTypeImpl) superType).vertex);
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
            return stream(apply(vertex.ins().edge(Schema.Edge.Type.OWNS_KEY).from(), ThingTypeImpl::of));
        } else {
            return stream(apply(link(
                    vertex.ins().edge(Schema.Edge.Type.OWNS_KEY).from(),
                    vertex.ins().edge(Schema.Edge.Type.OWNS).from()
            ), ThingTypeImpl::of));
        }
    }

    @Override
    public List<GraknException> validate() {
        return super.validate();
    }

    @Override
    public AttributeTypeImpl.Root asObject() {
        if (this.getValueType() == ValueType.OBJECT) return new AttributeTypeImpl.Root(this.vertex);
        else throw new GraknException(INVALID_TYPE_CASTING.message(AttributeType.class.getCanonicalName()));
    }

    @Override
    public AttributeTypeImpl.Boolean asBoolean() {
        throw new GraknException(INVALID_TYPE_CASTING.message(AttributeType.Boolean.class.getCanonicalName()));
    }

    @Override
    public AttributeTypeImpl.Long asLong() {
        throw new GraknException(INVALID_TYPE_CASTING.message(AttributeType.Long.class.getCanonicalName()));
    }

    @Override
    public AttributeTypeImpl.Double asDouble() {
        throw new GraknException(INVALID_TYPE_CASTING.message(AttributeType.Double.class.getCanonicalName()));
    }

    @Override
    public AttributeTypeImpl.String asString() {
        throw new GraknException(INVALID_TYPE_CASTING.message(AttributeType.String.class.getCanonicalName()));
    }

    @Override
    public AttributeTypeImpl.DateTime asDateTime() {
        throw new GraknException(INVALID_TYPE_CASTING.message(AttributeType.DateTime.class.getCanonicalName()));
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

        private Root(TypeVertex vertex) {
            super(vertex);
            assert vertex.valueType().equals(Schema.ValueType.OBJECT);
            assert vertex.label().equals(Schema.Vertex.Type.Root.ATTRIBUTE.label());
        }

        public ValueType getValueType() { return ValueType.OBJECT; }

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
        public void setLabel(java.lang.String label) { throw new GraknException(ROOT_TYPE_MUTATION); }

        @Override
        public void unsetAbstract() { throw new GraknException(ROOT_TYPE_MUTATION); }

        @Override
        public void setSupertype(AttributeType superType) { throw new GraknException(ROOT_TYPE_MUTATION); }

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
            return subs(v -> {
                switch (v.valueType()) {
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
                        throw new GraknException(UNRECOGNISED_VALUE);
                }
            });
        }

        @Override
        public Stream<AttributeImpl<?>> getInstances() {
            return super.instances(v -> AttributeImpl.of(v.asAttribute()));
        }

        @Override
        public void setOwns(AttributeType attributeType, boolean isKey) {
            throw new GraknException(ROOT_TYPE_MUTATION);
        }

        @Override
        public void setOwns(AttributeType attributeType, AttributeType overriddenType, boolean isKey) {
            throw new GraknException(ROOT_TYPE_MUTATION);
        }

        @Override
        public void setPlays(RoleType roleType) {
            throw new GraknException(ROOT_TYPE_MUTATION);
        }

        @Override
        public void setPlays(RoleType roleType, RoleType overriddenType) {
            throw new GraknException(ROOT_TYPE_MUTATION);
        }

        @Override
        public void unsetPlays(RoleType roleType) {
            throw new GraknException(ROOT_TYPE_MUTATION);
        }
    }

    public static class Boolean extends AttributeTypeImpl implements AttributeType.Boolean {

        public Boolean(TypeGraph graph, java.lang.String label) {
            super(graph, label, java.lang.Boolean.class);
        }

        private Boolean(TypeVertex vertex) {
            super(vertex);
            if (!vertex.label().equals(Schema.Vertex.Type.Root.ATTRIBUTE.label()) &&
                    !vertex.valueType().equals(Schema.ValueType.BOOLEAN)) {
                throw new GraknException(VALUE_TYPE_MISMATCH.message(
                        vertex.label(),
                        Schema.ValueType.BOOLEAN.name(),
                        vertex.valueType().name()
                ));
            }
        }

        public static AttributeTypeImpl.Boolean of(TypeVertex vertex) {
            return vertex.label().equals(Schema.Vertex.Type.Root.ATTRIBUTE.label()) ?
                    new Root(vertex) :
                    new AttributeTypeImpl.Boolean(vertex);
        }

        @Nullable
        @Override
        public AttributeTypeImpl.Boolean getSupertype() {
            return super.sup(AttributeTypeImpl.Boolean::of);
        }

        @Override
        public Stream<AttributeTypeImpl.Boolean> getSupertypes() {
            return super.sups(AttributeTypeImpl.Boolean::of);
        }

        @Override
        public Stream<AttributeTypeImpl.Boolean> getSubtypes() {
            return super.subs(AttributeTypeImpl.Boolean::of);
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
            validateIsCommitedAndNotAbstract(Attribute.class);
            AttributeVertex<java.lang.Boolean> attVertex = vertex.graph().thing().put(vertex, value, isInferred);
            return new AttributeImpl.Boolean(attVertex);
        }

        @Override
        public Attribute.Boolean get(boolean value) {
            AttributeVertex<java.lang.Boolean> attVertex = vertex.graph().thing().get(vertex, value);
            if (attVertex != null) return new AttributeImpl.Boolean(attVertex);
            else return null;
        }

        private static class Root extends AttributeTypeImpl.Boolean {

            private Root(TypeVertex vertex) {
                super(vertex);
                assert vertex.label().equals(Schema.Vertex.Type.Root.ATTRIBUTE.label());
            }

            @Override
            public boolean isRoot() { return true; }

            @Override
            public Stream<AttributeTypeImpl.Boolean> getSubtypes() {
                return stream(apply(
                        super.subTypeVertices(Schema.ValueType.BOOLEAN),
                        AttributeTypeImpl.Boolean::of
                ));
            }

            @Override
            public void setLabel(java.lang.String label) {
                throw new GraknException(ROOT_TYPE_MUTATION);
            }

            @Override
            public void unsetAbstract() {
                throw new GraknException(ROOT_TYPE_MUTATION);
            }

            @Override
            public void setSupertype(AttributeType superType) {
                throw new GraknException(ROOT_TYPE_MUTATION);
            }

            @Override
            public void setOwns(AttributeType attributeType, boolean isKey) {
                throw new GraknException(ROOT_TYPE_MUTATION);
            }

            @Override
            public void setOwns(AttributeType attributeType, AttributeType overriddenType, boolean isKey) {
                throw new GraknException(ROOT_TYPE_MUTATION);
            }

            @Override
            public void setPlays(RoleType roleType) {
                throw new GraknException(ROOT_TYPE_MUTATION);
            }

            @Override
            public void setPlays(RoleType roleType, RoleType overriddenType) {
                throw new GraknException(ROOT_TYPE_MUTATION);
            }

            @Override
            public void unsetPlays(RoleType roleType) {
                throw new GraknException(ROOT_TYPE_MUTATION);
            }
        }
    }

    public static class Long extends AttributeTypeImpl implements AttributeType.Long {

        public Long(TypeGraph graph, java.lang.String label) {
            super(graph, label, java.lang.Long.class);
        }

        private Long(TypeVertex vertex) {
            super(vertex);
            if (!vertex.label().equals(Schema.Vertex.Type.Root.ATTRIBUTE.label()) &&
                    !vertex.valueType().equals(Schema.ValueType.LONG)) {
                throw new GraknException(VALUE_TYPE_MISMATCH.message(
                        vertex.label(),
                        Schema.ValueType.LONG.name(),
                        vertex.valueType().name()
                ));
            }
        }

        public static AttributeTypeImpl.Long of(TypeVertex vertex) {
            return vertex.label().equals(Schema.Vertex.Type.Root.ATTRIBUTE.label()) ?
                    new Root(vertex) :
                    new AttributeTypeImpl.Long(vertex);
        }

        @Nullable
        @Override
        public AttributeTypeImpl.Long getSupertype() {
            return super.sup(AttributeTypeImpl.Long::of);
        }

        @Override
        public Stream<AttributeTypeImpl.Long> getSupertypes() {
            return super.sups(AttributeTypeImpl.Long::of);
        }

        @Override
        public Stream<AttributeTypeImpl.Long> getSubtypes() {
            return super.subs(AttributeTypeImpl.Long::of);
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
            validateIsCommitedAndNotAbstract(Attribute.class);
            AttributeVertex<java.lang.Long> attVertex = vertex.graph().thing().put(vertex, value, isInferred);
            return new AttributeImpl.Long(attVertex);
        }

        @Override
        public Attribute.Long get(long value) {
            AttributeVertex<java.lang.Long> attVertex = vertex.graph().thing().get(vertex, value);
            if (attVertex != null) return new AttributeImpl.Long(attVertex);
            else return null;
        }

        private static class Root extends AttributeTypeImpl.Long {

            private Root(TypeVertex vertex) {
                super(vertex);
                assert vertex.label().equals(Schema.Vertex.Type.Root.ATTRIBUTE.label());
            }

            @Override
            public boolean isRoot() { return true; }

            @Override
            public Stream<AttributeTypeImpl.Long> getSubtypes() {
                return stream(apply(
                        super.subTypeVertices(Schema.ValueType.LONG),
                        AttributeTypeImpl.Long::of
                ));
            }

            @Override
            public void setLabel(java.lang.String label) {
                throw new GraknException(ROOT_TYPE_MUTATION);
            }

            @Override
            public void unsetAbstract() {
                throw new GraknException(ROOT_TYPE_MUTATION);
            }

            @Override
            public void setSupertype(AttributeType superType) {
                throw new GraknException(ROOT_TYPE_MUTATION);
            }

            @Override
            public void setOwns(AttributeType attributeType, boolean isKey) {
                throw new GraknException(ROOT_TYPE_MUTATION);
            }

            @Override
            public void setOwns(AttributeType attributeType, AttributeType overriddenType, boolean isKey) {
                throw new GraknException(ROOT_TYPE_MUTATION);
            }

            @Override
            public void setPlays(RoleType roleType) {
                throw new GraknException(ROOT_TYPE_MUTATION);
            }

            @Override
            public void setPlays(RoleType roleType, RoleType overriddenType) {
                throw new GraknException(ROOT_TYPE_MUTATION);
            }

            @Override
            public void unsetPlays(RoleType roleType) {
                throw new GraknException(ROOT_TYPE_MUTATION);
            }
        }
    }

    public static class Double extends AttributeTypeImpl implements AttributeType.Double {

        public Double(TypeGraph graph, java.lang.String label) {
            super(graph, label, java.lang.Double.class);
        }

        private Double(TypeVertex vertex) {
            super(vertex);
            if (!vertex.label().equals(Schema.Vertex.Type.Root.ATTRIBUTE.label()) &&
                    !vertex.valueType().equals(Schema.ValueType.DOUBLE)) {
                throw new GraknException(VALUE_TYPE_MISMATCH.message(
                        vertex.label(),
                        Schema.ValueType.DOUBLE.name(),
                        vertex.valueType().name()
                ));
            }
        }

        public static AttributeTypeImpl.Double of(TypeVertex vertex) {
            return vertex.label().equals(Schema.Vertex.Type.Root.ATTRIBUTE.label()) ?
                    new Root(vertex) :
                    new AttributeTypeImpl.Double(vertex);
        }

        @Nullable
        @Override
        public AttributeTypeImpl.Double getSupertype() {
            return super.sup(AttributeTypeImpl.Double::of);
        }

        @Override
        public Stream<AttributeTypeImpl.Double> getSupertypes() {
            return super.sups(AttributeTypeImpl.Double::of);
        }

        @Override
        public Stream<AttributeTypeImpl.Double> getSubtypes() {
            return super.subs(AttributeTypeImpl.Double::of);
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
            validateIsCommitedAndNotAbstract(Attribute.class);
            AttributeVertex<java.lang.Double> attVertex = vertex.graph().thing().put(vertex, value, isInferred);
            return new AttributeImpl.Double(attVertex);
        }

        @Override
        public Attribute.Double get(double value) {
            AttributeVertex<java.lang.Double> attVertex = vertex.graph().thing().get(vertex, value);
            if (attVertex != null) return new AttributeImpl.Double(attVertex);
            else return null;
        }

        private static class Root extends AttributeTypeImpl.Double {

            private Root(TypeVertex vertex) {
                super(vertex);
                assert vertex.label().equals(Schema.Vertex.Type.Root.ATTRIBUTE.label());
            }

            @Override
            public boolean isRoot() { return true; }

            @Override
            public Stream<AttributeTypeImpl.Double> getSubtypes() {
                return stream(apply(
                        super.subTypeVertices(Schema.ValueType.DOUBLE),
                        AttributeTypeImpl.Double::of
                ));
            }

            @Override
            public void setLabel(java.lang.String label) {
                throw new GraknException(ROOT_TYPE_MUTATION);
            }

            @Override
            public void unsetAbstract() {
                throw new GraknException(ROOT_TYPE_MUTATION);
            }

            @Override
            public void setSupertype(AttributeType superType) {
                throw new GraknException(ROOT_TYPE_MUTATION);
            }

            @Override
            public void setOwns(AttributeType attributeType, boolean isKey) {
                throw new GraknException(ROOT_TYPE_MUTATION);
            }

            @Override
            public void setOwns(AttributeType attributeType, AttributeType overriddenType, boolean isKey) {
                throw new GraknException(ROOT_TYPE_MUTATION);
            }

            @Override
            public void setPlays(RoleType roleType) {
                throw new GraknException(ROOT_TYPE_MUTATION);
            }

            @Override
            public void setPlays(RoleType roleType, RoleType overriddenType) {
                throw new GraknException(ROOT_TYPE_MUTATION);
            }

            @Override
            public void unsetPlays(RoleType roleType) {
                throw new GraknException(ROOT_TYPE_MUTATION);
            }
        }
    }

    public static class String extends AttributeTypeImpl implements AttributeType.String {

        private Pattern regexPattern;

        public String(TypeGraph graph, java.lang.String label) {
            super(graph, label, java.lang.String.class);
        }

        private String(TypeVertex vertex) {
            super(vertex);
            if (!vertex.label().equals(Schema.Vertex.Type.Root.ATTRIBUTE.label()) &&
                    !vertex.valueType().equals(Schema.ValueType.STRING)) {
                throw new GraknException(VALUE_TYPE_MISMATCH.message(
                        vertex.label(),
                        Schema.ValueType.STRING.name(),
                        vertex.valueType().name()
                ));
            }
        }

        public static AttributeTypeImpl.String of(TypeVertex vertex) {
            return vertex.label().equals(Schema.Vertex.Type.Root.ATTRIBUTE.label()) ?
                    new Root(vertex) :
                    new AttributeTypeImpl.String(vertex);
        }

        @Nullable
        @Override
        public AttributeTypeImpl.String getSupertype() {
            return super.sup(AttributeTypeImpl.String::of);
        }

        @Override
        public Stream<AttributeTypeImpl.String> getSupertypes() {
            return super.sups(AttributeTypeImpl.String::of);
        }

        @Override
        public Stream<AttributeTypeImpl.String> getSubtypes() {
            return super.subs(AttributeTypeImpl.String::of);
        }

        @Override
        public Stream<AttributeImpl.String> getInstances() {
            return super.instances(v -> new AttributeImpl.String(v.asAttribute().asString()));
        }

        @Override
        public AttributeTypeImpl.String asString() { return this; }

        @Override
        public void setRegex(java.lang.String regex) {
            if (regex != null) {
                Pattern pattern = Pattern.compile(regex);
                getInstances().parallel().forEach(attribute -> {
                    Matcher matcher = pattern.matcher(attribute.getValue());
                    if (!matcher.matches()) {
                        throw new GraknException(ATTRIBUTE_REGEX_UNSATISFIES_INSTANCES.message(getLabel(), regex, attribute.getValue()));
                    }
                });
            }
            vertex.regex(regex);
        }

        @Override
        public java.lang.String getRegex() {
            return vertex.regex();
        }

        private Pattern regexPattern() {
            if (vertex.regex() == null || regexPattern != null) return regexPattern;
            return (regexPattern = Pattern.compile(vertex.regex()));
        }

        @Override
        public Attribute.String put(java.lang.String value) {
            return put(value, false);
        }

        @Override
        public Attribute.String put(java.lang.String value, boolean isInferred) {
            validateIsCommitedAndNotAbstract(Attribute.class);
            if (vertex.regex() != null && !regexPattern().matcher(value).matches()) {
                throw new GraknException(ATTRIBUTE_VALUE_UNSATISFIES_REGEX.message(getLabel(), value, getRegex()));
            }
            if (value.length() > Schema.STRING_MAX_LENGTH) {
                throw new GraknException(ILLEGAL_STRING_SIZE);
            }
            AttributeVertex<java.lang.String> attVertex = vertex.graph().thing().put(vertex, value, isInferred);
            return new AttributeImpl.String(attVertex);
        }

        @Override
        public Attribute.String get(java.lang.String value) {
            AttributeVertex<java.lang.String> attVertex = vertex.graph().thing().get(vertex, value);
            if (attVertex != null) return new AttributeImpl.String(attVertex);
            else return null;
        }

        @Override
        public ValueType getValueType() {
            return ValueType.STRING;
        }

        private static class Root extends AttributeTypeImpl.String {

            private Root(TypeVertex vertex) {
                super(vertex);
                assert vertex.label().equals(Schema.Vertex.Type.Root.ATTRIBUTE.label());
            }

            @Override
            public boolean isRoot() { return true; }

            @Override
            public Stream<AttributeTypeImpl.String> getSubtypes() {
                return stream(apply(
                        super.subTypeVertices(Schema.ValueType.STRING),
                        AttributeTypeImpl.String::of
                ));
            }

            @Override
            public void setLabel(java.lang.String label) {
                throw new GraknException(ROOT_TYPE_MUTATION);
            }

            @Override
            public void unsetAbstract() {
                throw new GraknException(ROOT_TYPE_MUTATION);
            }

            @Override
            public void setSupertype(AttributeType superType) {
                throw new GraknException(ROOT_TYPE_MUTATION);
            }

            @Override
            public void setOwns(AttributeType attributeType, boolean isKey) {
                throw new GraknException(ROOT_TYPE_MUTATION);
            }

            @Override
            public void setOwns(AttributeType attributeType, AttributeType overriddenType, boolean isKey) {
                throw new GraknException(ROOT_TYPE_MUTATION);
            }

            @Override
            public void setPlays(RoleType roleType) {
                throw new GraknException(ROOT_TYPE_MUTATION);
            }

            @Override
            public void setPlays(RoleType roleType, RoleType overriddenType) {
                throw new GraknException(ROOT_TYPE_MUTATION);
            }

            @Override
            public void unsetPlays(RoleType roleType) {
                throw new GraknException(ROOT_TYPE_MUTATION);
            }

            @Override
            public void setRegex(java.lang.String regex) {
                throw new GraknException(ROOT_TYPE_MUTATION);
            }
        }
    }

    public static class DateTime extends AttributeTypeImpl implements AttributeType.DateTime {

        public DateTime(TypeGraph graph, java.lang.String label) {
            super(graph, label, LocalDateTime.class);
        }

        private DateTime(TypeVertex vertex) {
            super(vertex);
            if (!vertex.label().equals(Schema.Vertex.Type.Root.ATTRIBUTE.label()) &&
                    !vertex.valueType().equals(Schema.ValueType.DATETIME)) {
                throw new GraknException(VALUE_TYPE_MISMATCH.message(
                        vertex.label(),
                        Schema.ValueType.DATETIME.name(),
                        vertex.valueType().name()
                ));
            }
        }

        public static AttributeTypeImpl.DateTime of(TypeVertex vertex) {
            return vertex.label().equals(Schema.Vertex.Type.Root.ATTRIBUTE.label()) ?
                    new Root(vertex) :
                    new AttributeTypeImpl.DateTime(vertex);
        }

        @Nullable
        @Override
        public AttributeTypeImpl.DateTime getSupertype() {
            return super.sup(AttributeTypeImpl.DateTime::of);
        }

        @Override
        public Stream<AttributeTypeImpl.DateTime> getSupertypes() {
            return super.sups(AttributeTypeImpl.DateTime::of);
        }

        @Override
        public Stream<AttributeTypeImpl.DateTime> getSubtypes() {
            return super.subs(AttributeTypeImpl.DateTime::of);
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
            validateIsCommitedAndNotAbstract(Attribute.class);
            AttributeVertex<LocalDateTime> attVertex = vertex.graph().thing().put(vertex, value, isInferred);
            if (!isInferred && attVertex.isInferred()) attVertex.isInferred(false);
            return new AttributeImpl.DateTime(attVertex);
        }

        @Override
        public Attribute.DateTime get(LocalDateTime value) {
            AttributeVertex<java.time.LocalDateTime> attVertex = vertex.graph().thing().get(vertex, value);
            if (attVertex != null) return new AttributeImpl.DateTime(attVertex);
            else return null;
        }

        private static class Root extends AttributeTypeImpl.DateTime {

            private Root(TypeVertex vertex) {
                super(vertex);
                assert vertex.label().equals(Schema.Vertex.Type.Root.ATTRIBUTE.label());
            }

            @Override
            public boolean isRoot() { return true; }

            @Override
            public Stream<AttributeTypeImpl.DateTime> getSubtypes() {
                return stream(apply(
                        super.subTypeVertices(Schema.ValueType.DATETIME),
                        AttributeTypeImpl.DateTime::of
                ));
            }

            @Override
            public void setLabel(java.lang.String label) {
                throw new GraknException(ROOT_TYPE_MUTATION);
            }

            @Override
            public void unsetAbstract() {
                throw new GraknException(ROOT_TYPE_MUTATION);
            }

            @Override
            public void setSupertype(AttributeType superType) {
                throw new GraknException(ROOT_TYPE_MUTATION);
            }

            @Override
            public void setOwns(AttributeType attributeType, boolean isKey) {
                throw new GraknException(ROOT_TYPE_MUTATION);
            }

            @Override
            public void setOwns(AttributeType attributeType, AttributeType overriddenType, boolean isKey) {
                throw new GraknException(ROOT_TYPE_MUTATION);
            }

            @Override
            public void setPlays(RoleType roleType) {
                throw new GraknException(ROOT_TYPE_MUTATION);
            }

            @Override
            public void setPlays(RoleType roleType, RoleType overriddenType) {
                throw new GraknException(ROOT_TYPE_MUTATION);
            }

            @Override
            public void unsetPlays(RoleType roleType) {
                throw new GraknException(ROOT_TYPE_MUTATION);
            }
        }
    }
}
