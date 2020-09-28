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
import grakn.core.common.iterator.ResourceIterator;
import grakn.core.concept.thing.impl.AttributeImpl;
import grakn.core.concept.thing.impl.EntityImpl;
import grakn.core.concept.thing.impl.RelationImpl;
import grakn.core.concept.thing.impl.ThingImpl;
import grakn.core.concept.type.AttributeType;
import grakn.core.concept.type.RoleType;
import grakn.core.concept.type.ThingType;
import grakn.core.graph.Graphs;
import grakn.core.graph.edge.SchemaEdge;
import grakn.core.graph.util.Encoding;
import grakn.core.graph.vertex.ThingVertex;
import grakn.core.graph.vertex.TypeVertex;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static grakn.core.common.collection.Streams.compareSize;
import static grakn.core.common.exception.ErrorMessage.Internal.UNRECOGNISED_VALUE;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.OVERRIDDEN_NOT_SUPERTYPE;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.OVERRIDE_NOT_AVAILABLE;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.OWNS_ABSTRACT_ATT_TYPE;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.OWNS_ATT_NOT_AVAILABLE;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.OWNS_KEY_NOT_AVAILABLE;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.OWNS_KEY_PRECONDITION_OWNERSHIP;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.OWNS_KEY_PRECONDITION_UNIQUENESS;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.OWNS_KEY_VALUE_TYPE;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.PLAYS_ABSTRACT_ROLE_TYPE;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.PLAYS_ROLE_NOT_AVAILABLE;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.ROOT_TYPE_MUTATION;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.TYPE_HAS_INSTANCES;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.TYPE_HAS_SUBTYPES;
import static grakn.core.common.iterator.Iterators.link;
import static grakn.core.graph.util.Encoding.Edge.Type.OWNS;
import static grakn.core.graph.util.Encoding.Edge.Type.OWNS_KEY;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.Stream.concat;

public abstract class ThingTypeImpl extends TypeImpl implements ThingType {

    ThingTypeImpl(Graphs graphs, TypeVertex vertex) {
        super(graphs, vertex);
    }

    ThingTypeImpl(Graphs graphs, String label, Encoding.Vertex.Type encoding) {
        super(graphs, label, encoding);
    }

    public static ThingTypeImpl of(Graphs graphs, TypeVertex vertex) {
        switch (vertex.encoding()) {
            case ENTITY_TYPE:
                return EntityTypeImpl.of(graphs, vertex);
            case ATTRIBUTE_TYPE:
                return AttributeTypeImpl.of(graphs, vertex);
            case RELATION_TYPE:
                return RelationTypeImpl.of(graphs, vertex);
            case THING_TYPE:
                return new ThingTypeImpl.Root(graphs, vertex);
            default:
                throw new GraknException(UNRECOGNISED_VALUE);
        }
    }

    @Override
    public void setAbstract() {
        if (getInstances().findFirst().isPresent()) {
            throw exception(TYPE_HAS_INSTANCES.message(getLabel()));
        }
        vertex.isAbstract(true);
    }

    @Override
    public void unsetAbstract() {
        vertex.isAbstract(false);
    }

    @Nullable
    @Override
    public abstract ThingTypeImpl getSupertype();

    @Override
    public abstract Stream<? extends ThingTypeImpl> getSubtypes();

    <THING> Stream<THING> instances(Function<ThingVertex, THING> thingConstructor) {
        return getSubtypes().flatMap(t -> graphs.data().get(t.vertex).stream()).map(thingConstructor);
    }

    @Override
    public void setOwns(AttributeType attributeType) {
        setOwns(attributeType, false);
    }

    @Override
    public void setOwns(AttributeType attributeType, boolean isKey) {
        if (isKey) ownsKey((AttributeTypeImpl) attributeType);
        else ownsAttribute((AttributeTypeImpl) attributeType);
    }

    @Override
    public void setOwns(AttributeType attributeType, AttributeType overriddenType) {
        setOwns(attributeType, overriddenType, false);
    }

    @Override
    public void setOwns(AttributeType attributeType, AttributeType overriddenType, boolean isKey) {
        if (isKey) ownsKey((AttributeTypeImpl) attributeType, (AttributeTypeImpl) overriddenType);
        else ownsAttribute((AttributeTypeImpl) attributeType, (AttributeTypeImpl) overriddenType);
    }

    @Override
    public void unsetOwns(AttributeType attributeType) {
        SchemaEdge edge;
        TypeVertex attVertex = ((AttributeTypeImpl) attributeType).vertex;
        if ((edge = vertex.outs().edge(OWNS, attVertex)) != null) edge.delete();
        if ((edge = vertex.outs().edge(OWNS_KEY, attVertex)) != null) edge.delete();
    }

    private <T extends grakn.core.concept.type.Type> void override(Encoding.Edge.Type encoding, T type, T overriddenType,
                                                                   Stream<? extends TypeImpl> overridable, Stream<? extends TypeImpl> notOverridable) {
        if (type.getSupertypes().noneMatch(t -> t.equals(overriddenType))) {
            throw exception(OVERRIDDEN_NOT_SUPERTYPE.message(type.getLabel(), overriddenType.getLabel()));
        } else if (notOverridable.anyMatch(t -> t.equals(overriddenType)) || overridable.noneMatch(t -> t.equals(overriddenType))) {
            throw exception(OVERRIDE_NOT_AVAILABLE.message(type.getLabel(), overriddenType.getLabel()));
        }

        vertex.outs().edge(encoding, ((TypeImpl) type).vertex).overridden(((TypeImpl) overriddenType).vertex);
    }

    private void ownsKey(AttributeTypeImpl attributeType) {
        if (!attributeType.isKeyable()) {
            throw exception(OWNS_KEY_VALUE_TYPE.message(attributeType.getLabel(), attributeType.getValueType().name()));
        } else if (concat(getSupertype().getOwns(attributeType.getValueType(), true), getSupertype().overriddenOwns(false, true)).anyMatch(a -> a.equals(attributeType))) {
            throw exception(OWNS_KEY_NOT_AVAILABLE.message(attributeType.getLabel()));
        }

        TypeVertex attVertex = attributeType.vertex;
        SchemaEdge ownsEdge, ownsKeyEdge;

        if ((ownsEdge = vertex.outs().edge(OWNS, attVertex)) != null) {
            // TODO: These ownership and uniqueness checks should be parallelised to scale better
            if (getInstances().anyMatch(thing -> compareSize(thing.getHas(attributeType), 1) != 0)) {
                throw exception(OWNS_KEY_PRECONDITION_OWNERSHIP.message(vertex.label(), attVertex.label()));
            } else if (attributeType.getInstances().anyMatch(att -> compareSize(att.getOwners(this), 1) != 0)) {
                throw exception(OWNS_KEY_PRECONDITION_UNIQUENESS.message(attVertex.label(), vertex.label()));
            }
            ownsEdge.delete();
        }
        ownsKeyEdge = vertex.outs().put(OWNS_KEY, attVertex);
        if (getSupertype().declaredOwns(false).anyMatch(a -> a.equals(attributeType)))
            ownsKeyEdge.overridden(attVertex);
    }

    private void ownsKey(AttributeTypeImpl attributeType, AttributeTypeImpl overriddenType) {
        this.ownsKey(attributeType);
        override(OWNS_KEY, attributeType, overriddenType,
                 getSupertype().getOwns(attributeType.getValueType()),
                 declaredOwns(false));
    }

    private void ownsAttribute(AttributeTypeImpl attributeType) {
        if (getSupertypes().filter(t -> !t.equals(this)).flatMap(ThingType::getOwns).anyMatch(a -> a.equals(attributeType))) {
            throw exception(OWNS_ATT_NOT_AVAILABLE.message(attributeType.getLabel()));
        }

        TypeVertex attVertex = attributeType.vertex;
        SchemaEdge keyEdge;
        if ((keyEdge = vertex.outs().edge(OWNS_KEY, attVertex)) != null) keyEdge.delete();
        vertex.outs().put(OWNS, attVertex);
    }

    private void ownsAttribute(AttributeTypeImpl attributeType, AttributeTypeImpl overriddenType) {
        this.ownsAttribute(attributeType);
        override(OWNS, attributeType, overriddenType,
                 getSupertype().getOwns(attributeType.getValueType()),
                 concat(getSupertype().getOwns(true), declaredOwns(false)));
    }

    private Stream<AttributeTypeImpl> declaredOwns(boolean onlyKey) {
        if (isRoot()) return Stream.of();
        ResourceIterator<TypeVertex> iterator;
        if (onlyKey) iterator = vertex.outs().edge(OWNS_KEY).to();
        else iterator = link(vertex.outs().edge(OWNS_KEY).to(), vertex.outs().edge(OWNS).to());
        return iterator.apply(v -> AttributeTypeImpl.of(graphs, v)).stream();
    }

    Stream<AttributeTypeImpl> overriddenOwns(boolean onlyKey, boolean transitive) {
        if (isRoot()) return Stream.empty();
        Stream<AttributeTypeImpl> overriddenOwns;
        if (onlyKey) {
            overriddenOwns = vertex.outs().edge(OWNS_KEY).overridden().filter(Objects::nonNull)
                    .apply(v -> AttributeTypeImpl.of(graphs, v)).stream();
        } else {
            overriddenOwns = link(
                    vertex.outs().edge(OWNS_KEY).overridden(),
                    vertex.outs().edge(OWNS).overridden()
            ).filter(Objects::nonNull).distinct().apply(v -> AttributeTypeImpl.of(graphs, v)).stream();
        }

        if (transitive) return concat(overriddenOwns, getSupertype().overriddenOwns(onlyKey, true));
        else return overriddenOwns;
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
        if (isRoot()) return Stream.of();
        Set<AttributeTypeImpl> overridden = overriddenOwns(onlyKey, false).collect(toSet());
        return concat(declaredOwns(onlyKey), getSupertype().getOwns(onlyKey).filter(key -> !overridden.contains(key)));
    }

    public Stream<AttributeTypeImpl> getOwns(AttributeType.ValueType valueType, boolean onlyKey) {
        return getOwns(onlyKey).filter(att -> att.getValueType().equals(valueType));
    }

    @Override
    public void setPlays(RoleType roleType) {
        if (getSupertypes().filter(t -> !t.equals(this)).flatMap(ThingType::getPlays).anyMatch(a -> a.equals(roleType))) {
            throw exception(PLAYS_ROLE_NOT_AVAILABLE.message(roleType.getLabel()));
        }
        vertex.outs().put(Encoding.Edge.Type.PLAYS, ((RoleTypeImpl) roleType).vertex);
    }

    @Override
    public void setPlays(RoleType roleType, RoleType overriddenType) {
        setPlays(roleType);
        override(Encoding.Edge.Type.PLAYS, roleType, overriddenType, getSupertype().getPlays(),
                 vertex.outs().edge(Encoding.Edge.Type.PLAYS).to().apply(v -> RoleTypeImpl.of(graphs, v)).stream());
    }

    @Override
    public void unsetPlays(RoleType roleType) {
        vertex.outs().edge(Encoding.Edge.Type.PLAYS, ((RoleTypeImpl) roleType).vertex).delete();
    }

    @Override
    public Stream<RoleTypeImpl> getPlays() {
        if (isRoot()) return Stream.of();

        Set<TypeVertex> overridden = new HashSet<>();
        vertex.outs().edge(Encoding.Edge.Type.PLAYS).overridden().filter(Objects::nonNull).forEachRemaining(overridden::add);
        return concat(
                vertex.outs().edge(Encoding.Edge.Type.PLAYS).to().apply(v -> RoleTypeImpl.of(graphs, v)).stream(),
                getSupertype().getPlays().filter(att -> !overridden.contains(att.vertex))
        );
    }

    @Override
    public void delete() {
        if (getSubtypes().anyMatch(s -> !s.equals(this))) {
            throw exception(TYPE_HAS_SUBTYPES.message(getLabel()));
        } else if (getSubtypes().flatMap(ThingType::getInstances).findFirst().isPresent()) {
            throw exception(TYPE_HAS_INSTANCES.message(getLabel()));
        } else {
            vertex.delete();
        }
    }

    @Override
    public List<GraknException> validate() {
        List<GraknException> exceptions = super.validate();
        if (!isAbstract()) {
            exceptions.addAll(exceptions_ownsAbstractAttType());
            exceptions.addAll(exceptions_playsAbstractRoleType());
        }
        return exceptions;
    }

    @Override
    public ThingTypeImpl asThingType() { return this; }

    private List<GraknException> exceptions_ownsAbstractAttType() {
        return getOwns().filter(TypeImpl::isAbstract)
                .map(attType -> new GraknException(OWNS_ABSTRACT_ATT_TYPE.message(getLabel(), attType.getLabel())))
                .collect(Collectors.toList());
    }

    private List<GraknException> exceptions_playsAbstractRoleType() {
        return getPlays().filter(TypeImpl::isAbstract)
                .map(roleType -> new GraknException(PLAYS_ABSTRACT_ROLE_TYPE.message(getLabel(), roleType.getLabel())))
                .collect(Collectors.toList());
    }

    public static class Root extends ThingTypeImpl {

        public Root(Graphs graphs, TypeVertex vertex) {
            super(graphs, vertex);
            assert vertex.label().equals(Encoding.Vertex.Type.Root.THING.label());
        }

        @Override
        public boolean isRoot() { return true; }

        @Override
        public void setLabel(String label) { throw exception(ROOT_TYPE_MUTATION.message()); }

        @Override
        public void unsetAbstract() { throw exception(ROOT_TYPE_MUTATION.message()); }

        @Override
        public ThingTypeImpl getSupertype() { return null; }

        @Override
        public Stream<ThingTypeImpl> getSupertypes() {
            return Stream.of(this);
        }

        @Override
        public Stream<ThingTypeImpl> getSubtypes() {
            return getSubtypes(v -> {
                switch (v.encoding()) {
                    case THING_TYPE:
                        assert this.vertex == v;
                        return this;
                    case ENTITY_TYPE:
                        return EntityTypeImpl.of(graphs, v);
                    case ATTRIBUTE_TYPE:
                        return AttributeTypeImpl.of(graphs, v);
                    case RELATION_TYPE:
                        return RelationTypeImpl.of(graphs, v);
                    default:
                        throw exception(UNRECOGNISED_VALUE.message());
                }
            });
        }

        @Override
        public Stream<ThingImpl> getInstances() {
            return super.instances(v -> {
                switch (v.encoding()) {
                    case ENTITY:
                        return EntityImpl.of(v);
                    case ATTRIBUTE:
                        return AttributeImpl.of(v);
                    case RELATION:
                        return RelationImpl.of(v);
                    default:
                        assert false;
                        throw exception(UNRECOGNISED_VALUE.message());
                }
            });
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
