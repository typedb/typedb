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
import grakn.core.concept.thing.impl.AttributeImpl;
import grakn.core.concept.thing.impl.EntityImpl;
import grakn.core.concept.thing.impl.RelationImpl;
import grakn.core.concept.thing.impl.ThingImpl;
import grakn.core.concept.type.AttributeType;
import grakn.core.concept.type.RoleType;
import grakn.core.concept.type.ThingType;
import grakn.core.concept.type.Type;
import grakn.core.graph.TypeGraph;
import grakn.core.graph.edge.TypeEdge;
import grakn.core.graph.util.Schema;
import grakn.core.graph.vertex.ThingVertex;
import grakn.core.graph.vertex.TypeVertex;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static grakn.core.common.collection.Streams.compareSize;
import static grakn.core.common.exception.ErrorMessage.Internal.UNRECOGNISED_VALUE;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.HAS_ABSTRACT_ATT_TYPE;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.HAS_ATT_NOT_AVAILABLE;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.HAS_KEY_NOT_AVAILABLE;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.HAS_KEY_PRECONDITION_OWNERSHIP;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.HAS_KEY_PRECONDITION_UNIQUENESS;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.HAS_KEY_VALUE_TYPE;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.OVERRIDDEN_NOT_SUPERTYPE;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.OVERRIDE_NOT_AVAILABLE;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.PLAYS_ABSTRACT_ROLE_TYPE;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.PLAYS_ROLE_NOT_AVAILABLE;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.ROOT_TYPE_MUTATION;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.TYPE_HAS_INSTANCES;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.TYPE_HAS_SUBTYPES;
import static grakn.core.common.iterator.Iterators.apply;
import static grakn.core.common.iterator.Iterators.distinct;
import static grakn.core.common.iterator.Iterators.filter;
import static grakn.core.common.iterator.Iterators.link;
import static grakn.core.common.iterator.Iterators.stream;
import static java.util.stream.Stream.concat;

public abstract class ThingTypeImpl extends TypeImpl implements ThingType {

    ThingTypeImpl(TypeVertex vertex) {
        super(vertex);
    }

    ThingTypeImpl(TypeGraph graph, String label, Schema.Vertex.Type schema) {
        super(graph, label, schema);
    }

    public static ThingTypeImpl of(TypeVertex vertex) {
        switch (vertex.schema()) {
            case ENTITY_TYPE:
                return EntityTypeImpl.of(vertex);
            case ATTRIBUTE_TYPE:
                return AttributeTypeImpl.of(vertex);
            case RELATION_TYPE:
                return RelationTypeImpl.of(vertex);
            case THING_TYPE:
                return new ThingTypeImpl.Root(vertex);
            default:
                throw new GraknException(UNRECOGNISED_VALUE);
        }
    }

    @Override
    public void setAbstract() {
        if (getInstances().findFirst().isPresent()) {
            throw new GraknException(TYPE_HAS_INSTANCES.message(getLabel()));
        }
        vertex.isAbstract(true);
    }

    @Override
    public void unsetAbstract() {
        vertex.isAbstract(false);
    }

    @Nullable
    public abstract ThingTypeImpl getSupertype();

    <THING> Stream<THING> instances(Function<ThingVertex, THING> thingConstructor) {
        return getSubtypes().flatMap(t -> stream(((TypeImpl) t).vertex.instances())).map(thingConstructor);
    }

    @Override
    public void setOwns(AttributeType attributeType) {
        setOwns(attributeType, false);
    }

    @Override
    public void setOwns(AttributeType attributeType, boolean isKey) {
        if (isKey) hasKey((AttributeTypeImpl) attributeType);
        else hasAttribute((AttributeTypeImpl) attributeType);
    }

    @Override
    public void setOwns(AttributeType attributeType, AttributeType overriddenType) {
        setOwns(attributeType, overriddenType, false);
    }

    @Override
    public void setOwns(AttributeType attributeType, AttributeType overriddenType, boolean isKey) {
        if (isKey) hasKey((AttributeTypeImpl) attributeType, (AttributeTypeImpl) overriddenType);
        else hasAttribute((AttributeTypeImpl) attributeType, (AttributeTypeImpl) overriddenType);
    }

    @Override
    public void unsetOwns(AttributeType attributeType) {
        TypeEdge edge;
        TypeVertex attVertex = ((AttributeTypeImpl) attributeType).vertex;
        if ((edge = vertex.outs().edge(Schema.Edge.Type.HAS, attVertex)) != null) edge.delete();
        if ((edge = vertex.outs().edge(Schema.Edge.Type.KEY, attVertex)) != null) edge.delete();
    }

    private <T extends Type> void override(Schema.Edge.Type schema, T type, T overriddenType,
                                           Stream<? extends TypeImpl> overridable, Stream<? extends TypeImpl> notOverridable) {
        if (type.getSupertypes().noneMatch(t -> t.equals(overriddenType))) {
            throw new GraknException(OVERRIDDEN_NOT_SUPERTYPE.message(type.getLabel(), overriddenType.getLabel()));
        } else if (notOverridable.anyMatch(t -> t.equals(overriddenType)) || overridable.noneMatch(t -> t.equals(overriddenType))) {
            throw new GraknException(OVERRIDE_NOT_AVAILABLE.message(type.getLabel(), overriddenType.getLabel()));
        }

        vertex.outs().edge(schema, ((TypeImpl) type).vertex).overridden(((TypeImpl) overriddenType).vertex);
    }

    private Stream<AttributeType> overriddenAttributes() {
        if (isRoot()) return Stream.empty();

        Iterator<TypeVertex> overriddenAttributes = distinct(link(
                vertex.outs().edge(Schema.Edge.Type.KEY).overridden(),
                vertex.outs().edge(Schema.Edge.Type.HAS).overridden()
        ));

        return concat(stream(link(overriddenAttributes).apply(AttributeTypeImpl::of)), getSupertype().overriddenAttributes());
    }

    private void hasKey(AttributeTypeImpl attributeType) {
        if (!attributeType.isKeyable()) {
            throw new GraknException(HAS_KEY_VALUE_TYPE.message(attributeType.getLabel(), attributeType.getValueType().name()));
        } else if (concat(getSupertype().getOwns(attributeType.getValueType(), true), getSupertype().overriddenAttributes()).anyMatch(a -> a.equals(attributeType))) {
            throw new GraknException(HAS_KEY_NOT_AVAILABLE.message(attributeType.getLabel()));
        }

        TypeVertex attVertex = ((AttributeTypeImpl) attributeType).vertex;
        TypeEdge hasEdge, keyEdge;

        if ((hasEdge = vertex.outs().edge(Schema.Edge.Type.HAS, attVertex)) != null) {
            // TODO: These ownership and uniqueness checks should be parallelised to scale better
            if (getInstances().anyMatch(thing -> compareSize(thing.getHas(attributeType), 1) != 0)) {
                throw new GraknException(HAS_KEY_PRECONDITION_OWNERSHIP.message(vertex.label(), attVertex.label()));
            } else if (attributeType.getInstances().anyMatch(att -> compareSize(att.getOwners(this), 1) != 0)) {
                throw new GraknException(HAS_KEY_PRECONDITION_UNIQUENESS.message(attVertex.label(), vertex.label()));
            }
            hasEdge.delete();
        }
        keyEdge = vertex.outs().put(Schema.Edge.Type.KEY, attVertex);
        if (getSupertype().declaredAttributes().anyMatch(a -> a.equals(attributeType))) keyEdge.overridden(attVertex);
    }

    private void hasKey(AttributeTypeImpl attributeType, AttributeTypeImpl overriddenType) {
        this.hasKey(attributeType);
        override(Schema.Edge.Type.KEY, attributeType, overriddenType,
                 getSupertype().getOwns(attributeType.getValueType()),
                 declaredAttributes());
    }

    private void hasAttribute(AttributeTypeImpl attributeType) {
        if (getSupertypes().filter(t -> !t.equals(this)).flatMap(ThingType::getOwns).anyMatch(a -> a.equals(attributeType))) {
            throw new GraknException(HAS_ATT_NOT_AVAILABLE.message(attributeType.getLabel()));
        }

        TypeVertex attVertex = attributeType.vertex;
        TypeEdge keyEdge;
        if ((keyEdge = vertex.outs().edge(Schema.Edge.Type.KEY, attVertex)) != null) keyEdge.delete();
        vertex.outs().put(Schema.Edge.Type.HAS, attVertex);
    }

    private void hasAttribute(AttributeTypeImpl attributeType, AttributeTypeImpl overriddenType) {
        this.hasAttribute(attributeType);
        override(Schema.Edge.Type.HAS, attributeType, overriddenType,
                 getSupertype().getOwns(attributeType.getValueType()),
                 concat(getSupertype().getOwns(true), declaredAttributes()));
    }

    private Stream<AttributeTypeImpl> declaredAttributes() {
        return stream(link(
                vertex.outs().edge(Schema.Edge.Type.KEY).to(),
                vertex.outs().edge(Schema.Edge.Type.HAS).to()
        ).apply(AttributeTypeImpl::of));
    }

    private Stream<AttributeTypeImpl> declaredKeys() {
        return stream(apply(vertex.outs().edge(Schema.Edge.Type.KEY).to(), AttributeTypeImpl::of));
    }

    @Override
    public Stream<AttributeTypeImpl> getOwns() {
        return getOwns(false);
    }

    @Override
    public Stream<AttributeTypeImpl> getOwns(AttributeType.ValueType valueType) {
        return getOwns(valueType, false);
    }

    @Override
    public Stream<AttributeTypeImpl> getOwns(boolean onlyKey) {
        if (onlyKey && isRoot()) {
            return declaredKeys();
        } else if (onlyKey) {
            Set<TypeVertex> overridden = new HashSet<>();
            filter(vertex.outs().edge(Schema.Edge.Type.KEY).overridden(), Objects::nonNull).forEachRemaining(overridden::add);
            return concat(declaredKeys(), getSupertype().getOwns(true).filter(key -> !overridden.contains(key.vertex)));
        } else if (isRoot()) {
            return declaredAttributes();
        } else {
            Set<TypeVertex> overridden = new HashSet<>();
            link(vertex.outs().edge(Schema.Edge.Type.KEY).overridden(),
                 vertex.outs().edge(Schema.Edge.Type.HAS).overridden()
            ).filter(Objects::nonNull).forEachRemaining(overridden::add);
            return concat(declaredAttributes(), getSupertype().getOwns().filter(att -> !overridden.contains(att.vertex)));
        }
    }

    public Stream<AttributeTypeImpl> getOwns(AttributeType.ValueType valueType, boolean onlyKey) {
        return getOwns(onlyKey).filter(att -> att.getValueType().equals(valueType));
    }

    @Override
    public void setPlays(RoleType roleType) {
        if (getSupertypes().filter(t -> !t.equals(this)).flatMap(ThingType::getPlays).anyMatch(a -> a.equals(roleType))) {
            throw new GraknException(PLAYS_ROLE_NOT_AVAILABLE.message(roleType.getLabel()));
        }
        vertex.outs().put(Schema.Edge.Type.PLAYS, ((RoleTypeImpl) roleType).vertex);
    }

    @Override
    public void setPlays(RoleType roleType, RoleType overriddenType) {
        setPlays(roleType);
        override(Schema.Edge.Type.PLAYS, roleType, overriddenType, getSupertype().getPlays(),
                 stream(apply(vertex.outs().edge(Schema.Edge.Type.PLAYS).to(), RoleTypeImpl::of)));
    }

    @Override
    public void unsetPlays(RoleType roleType) {
        vertex.outs().edge(Schema.Edge.Type.PLAYS, ((RoleTypeImpl) roleType).vertex).delete();
    }

    @Override
    public Stream<RoleTypeImpl> getPlays() {
        Stream<RoleTypeImpl> declared = stream(apply(vertex.outs().edge(Schema.Edge.Type.PLAYS).to(), RoleTypeImpl::of));
        if (isRoot()) {
            return declared;
        } else {
            Set<TypeVertex> overridden = new HashSet<>();
            filter(vertex.outs().edge(Schema.Edge.Type.PLAYS).overridden(), Objects::nonNull).forEachRemaining(overridden::add);
            return concat(declared, getSupertype().getPlays().filter(att -> !overridden.contains(att.vertex)));
        }
    }

    @Override
    public void delete() {
        if (getSubtypes().anyMatch(s -> !s.equals(this))) {
            throw new GraknException(TYPE_HAS_SUBTYPES.message(getLabel()));
        } else if (getSubtypes().flatMap(ThingType::getInstances).findFirst().isPresent()) {
            throw new GraknException(TYPE_HAS_INSTANCES.message(getLabel()));
        } else {
            vertex.delete();
        }
    }

    @Override
    public List<GraknException> validate() {
        List<GraknException> exceptions = super.validate();
        if (!isAbstract()) {
            exceptions.addAll(exceptions_hasAbstractAttType());
            exceptions.addAll(exceptions_playsAbstractRoleType());
        }
        return exceptions;
    }

    private List<GraknException> exceptions_hasAbstractAttType() {
        return getOwns().filter(TypeImpl::isAbstract)
                .map(attType -> new GraknException(HAS_ABSTRACT_ATT_TYPE.message(getLabel(), attType.getLabel())))
                .collect(Collectors.toList());
    }

    private List<GraknException> exceptions_playsAbstractRoleType() {
        return getPlays().filter(TypeImpl::isAbstract)
                .map(roleType -> new GraknException(PLAYS_ABSTRACT_ROLE_TYPE.message(getLabel(), roleType.getLabel())))
                .collect(Collectors.toList());
    }

    public static class Root extends ThingTypeImpl {

        public Root(TypeVertex vertex) {
            super(vertex);
            assert vertex.label().equals(Schema.Vertex.Type.Root.THING.label());
        }

        @Override
        public boolean isRoot() { return true; }

        @Override
        public void setLabel(String label) { throw new GraknException(ROOT_TYPE_MUTATION); }

        @Override
        public void unsetAbstract() { throw new GraknException(ROOT_TYPE_MUTATION); }

        @Override
        public ThingTypeImpl getSupertype() { return null; }

        @Override
        public Stream<ThingTypeImpl> getSupertypes() {
            return Stream.of(this);
        }

        @Override
        public Stream<ThingTypeImpl> getSubtypes() {
            return subs(v -> {
                switch (v.schema()) {
                    case THING_TYPE:
                        assert this.vertex == v;
                        return this;
                    case ENTITY_TYPE:
                        return EntityTypeImpl.of(v);
                    case ATTRIBUTE_TYPE:
                        return AttributeTypeImpl.of(v);
                    case RELATION_TYPE:
                        return RelationTypeImpl.of(v);
                    default:
                        throw new GraknException(UNRECOGNISED_VALUE);
                }
            });
        }

        @Override
        public Stream<ThingImpl> getInstances() {
            return super.instances(v -> {
                switch (v.schema()) {
                    case ENTITY:
                        return EntityImpl.of(v);
                    case ATTRIBUTE:
                        return AttributeImpl.of(v);
                    case RELATION:
                        return RelationImpl.of(v);
                    default:
                        assert false;
                        throw new GraknException(UNRECOGNISED_VALUE);
                }
            });
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

        /**
         * No-op validation method of the root type 'thing'.
         *
         * There's nothing to validate for the root type 'thing'.
         */
        @Override
        public List<GraknException> validate() {
            return Collections.emptyList();
        }
    }
}
