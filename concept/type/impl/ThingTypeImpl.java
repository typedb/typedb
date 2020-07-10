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

package grakn.concept.type.impl;

import grakn.common.exception.GraknException;
import grakn.concept.thing.Thing;
import grakn.concept.thing.impl.AttributeImpl;
import grakn.concept.thing.impl.EntityImpl;
import grakn.concept.thing.impl.RelationImpl;
import grakn.concept.type.AttributeType;
import grakn.concept.type.RoleType;
import grakn.concept.type.ThingType;
import grakn.concept.type.Type;
import grakn.graph.TypeGraph;
import grakn.graph.edge.TypeEdge;
import grakn.graph.util.Schema;
import grakn.graph.vertex.TypeVertex;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static grakn.common.collection.Streams.compareSize;
import static grakn.common.exception.Error.Internal.UNRECOGNISED_VALUE;
import static grakn.common.exception.Error.TypeWrite.HAS_ABSTRACT_ATT_TYPE;
import static grakn.common.exception.Error.TypeWrite.HAS_ATT_NOT_AVAILABLE;
import static grakn.common.exception.Error.TypeWrite.HAS_KEY_NOT_AVAILABLE;
import static grakn.common.exception.Error.TypeWrite.HAS_KEY_PRECONDITION_OWNERSHIP;
import static grakn.common.exception.Error.TypeWrite.HAS_KEY_PRECONDITION_UNIQUENESS;
import static grakn.common.exception.Error.TypeWrite.HAS_KEY_VALUE_TYPE;
import static grakn.common.exception.Error.TypeWrite.OVERRIDE_NOT_AVAILABLE;
import static grakn.common.exception.Error.TypeWrite.OVERRIDDEN_NOT_SUPERTYPE;
import static grakn.common.exception.Error.TypeWrite.PLAYS_ABSTRACT_ROLE_TYPE;
import static grakn.common.exception.Error.TypeWrite.PLAYS_ROLE_NOT_AVAILABLE;
import static grakn.common.exception.Error.TypeWrite.ROOT_TYPE_MUTATION;
import static grakn.common.exception.Error.TypeWrite.TYPE_HAS_INSTANCES;
import static grakn.common.exception.Error.TypeWrite.TYPE_HAS_SUBTYPES;
import static grakn.common.iterator.Iterators.apply;
import static grakn.common.iterator.Iterators.distinct;
import static grakn.common.iterator.Iterators.filter;
import static grakn.common.iterator.Iterators.link;
import static grakn.common.iterator.Iterators.stream;
import static java.util.stream.Stream.concat;

public abstract class ThingTypeImpl extends TypeImpl implements ThingType {

    ThingTypeImpl(TypeVertex vertex) {
        super(vertex);
    }

    ThingTypeImpl(TypeGraph graph, String label, Schema.Vertex.Type schema) {
        super(graph, label, schema);
    }

    @Override
    public void isAbstract(boolean isAbstract) {
        if (isAbstract && instances().findFirst().isPresent()) {
            throw new GraknException(TYPE_HAS_INSTANCES.format(label()));
        }
        vertex.isAbstract(isAbstract);
    }

    @Nullable
    public abstract ThingTypeImpl sup();

    private <T extends Type> void override(Schema.Edge.Type schema, T type, T overriddenType,
                                           Stream<? extends Type> overridable, Stream<? extends Type> notOverridable) {
        if (type.sups().noneMatch(t -> t.equals(overriddenType))) {
            throw new GraknException(OVERRIDDEN_NOT_SUPERTYPE.format(type.label(), overriddenType.label()));
        } else if (notOverridable.anyMatch(t -> t.equals(overriddenType)) || overridable.noneMatch(t -> t.equals(overriddenType))) {
            throw new GraknException(OVERRIDE_NOT_AVAILABLE.format(type.label(), overriddenType.label()));
        }

        vertex.outs().edge(schema, ((TypeImpl) type).vertex).overridden(((TypeImpl) overriddenType).vertex);
    }

    @Override
    public void has(AttributeType attributeType) {
        has(attributeType, false);
    }

    @Override
    public void has(AttributeType attributeType, boolean isKey) {
        if (isKey) hasKey(attributeType);
        else hasAttribute(attributeType);
    }

    @Override
    public void has(AttributeType attributeType, AttributeType overriddenType) {
        has(attributeType, overriddenType, false);
    }

    @Override
    public void has(AttributeType attributeType, AttributeType overriddenType, boolean isKey) {
        if (isKey) hasKey(attributeType, overriddenType);
        else hasAttribute(attributeType, overriddenType);
    }

    @Override
    public void unhas(AttributeType attributeType) {
        TypeEdge edge;
        TypeVertex attVertex = ((AttributeTypeImpl) attributeType).vertex;
        if ((edge = vertex.outs().edge(Schema.Edge.Type.HAS, attVertex)) != null) edge.delete();
        if ((edge = vertex.outs().edge(Schema.Edge.Type.KEY, attVertex)) != null) edge.delete();
    }

    private Stream<AttributeType> overriddenAttributes() {
        if (isRoot()) return Stream.empty();

        Iterator<TypeVertex> overriddenAttributes = distinct(link(
                vertex.outs().edge(Schema.Edge.Type.KEY).overridden(),
                vertex.outs().edge(Schema.Edge.Type.HAS).overridden()
        ));

        return concat(stream(link(overriddenAttributes).apply(AttributeTypeImpl::of)), sup().overriddenAttributes());
    }

    private void hasKey(AttributeType attributeType) {
        if (!attributeType.isKeyable()) {
            throw new GraknException(HAS_KEY_VALUE_TYPE.format(attributeType.label(), attributeType.valueType().getSimpleName()));
        } else if (concat(sup().keys(attributeType.valueType()), sup().overriddenAttributes()).anyMatch(a -> a.equals(attributeType))) {
            throw new GraknException(HAS_KEY_NOT_AVAILABLE.format(attributeType.label()));
        }

        TypeVertex attVertex = ((AttributeTypeImpl) attributeType).vertex;
        TypeEdge hasEdge, keyEdge;

        if ((hasEdge = vertex.outs().edge(Schema.Edge.Type.HAS, attVertex)) != null) {
            // TODO: These ownership and uniqueness checks should be parallelised to scale better
            if (instances().anyMatch(thing -> compareSize(thing.attributes(attributeType), 1) != 0)) {
                throw new GraknException(HAS_KEY_PRECONDITION_OWNERSHIP.format(vertex.label(), attVertex.label()));
            } else if (attributeType.instances().anyMatch(att -> compareSize(att.owners(this), 1) != 0)) {
                throw new GraknException(HAS_KEY_PRECONDITION_UNIQUENESS.format(attVertex.label(), vertex.label()));
            }
            hasEdge.delete();
        }
        keyEdge = vertex.outs().put(Schema.Edge.Type.KEY, attVertex);
        if (sup().declaredAttributes().anyMatch(a -> a.equals(attributeType))) keyEdge.overridden(attVertex);
    }

    private void hasKey(AttributeType attributeType, AttributeType overriddenType) {
        this.hasKey(attributeType);
        override(Schema.Edge.Type.KEY, attributeType, overriddenType,
                 sup().attributes(attributeType.valueType()),
                 declaredAttributes());
    }

    private void hasAttribute(AttributeType attributeType) {
        if (sups().filter(t -> !t.equals(this)).flatMap(ThingType::attributes).anyMatch(a -> a.equals(attributeType))) {
            throw new GraknException(HAS_ATT_NOT_AVAILABLE.format(attributeType.label()));
        }

        TypeVertex attVertex = ((AttributeTypeImpl) attributeType).vertex;
        TypeEdge keyEdge;
        if ((keyEdge = vertex.outs().edge(Schema.Edge.Type.KEY, attVertex)) != null) keyEdge.delete();
        vertex.outs().put(Schema.Edge.Type.HAS, attVertex);
    }

    private void hasAttribute(AttributeType attributeType, AttributeType overriddenType) {
        this.hasAttribute(attributeType);
        override(Schema.Edge.Type.HAS, attributeType, overriddenType,
                 sup().attributes(attributeType.valueType()),
                 concat(sup().keys(), declaredAttributes()));
    }

    private Stream<AttributeTypeImpl> declaredAttributes() {
        return stream(link(
                vertex.outs().edge(Schema.Edge.Type.KEY).to(),
                vertex.outs().edge(Schema.Edge.Type.HAS).to()
        ).apply(AttributeTypeImpl::of));
    }

    @Override
    public Stream<AttributeTypeImpl> keys(Class<?> valueType) {
        return keys().filter(att -> att.valueType().equals(valueType));
    }

    @Override
    public Stream<AttributeTypeImpl> keys() {
        Stream<AttributeTypeImpl> keys = stream(apply(vertex.outs().edge(Schema.Edge.Type.KEY).to(), AttributeTypeImpl::of));
        if (isRoot()) {
            return keys;
        } else {
            Set<TypeVertex> overridden = new HashSet<>();
            filter(vertex.outs().edge(Schema.Edge.Type.KEY).overridden(), Objects::nonNull).forEachRemaining(overridden::add);
            return concat(keys, sup().keys().filter(key -> !overridden.contains(key.vertex)));
        }
    }

    @Override
    public Stream<AttributeTypeImpl> attributes(Class<?> valueType) {
        return attributes().filter(att -> att.valueType().equals(valueType));
    }

    @Override
    public Stream<AttributeTypeImpl> attributes() {
        if (isRoot()) {
            return declaredAttributes();
        } else {
            Set<TypeVertex> overridden = new HashSet<>();
            link(vertex.outs().edge(Schema.Edge.Type.KEY).overridden(),
                 vertex.outs().edge(Schema.Edge.Type.HAS).overridden()
            ).filter(Objects::nonNull).forEachRemaining(overridden::add);
            return concat(declaredAttributes(), sup().attributes().filter(att -> !overridden.contains(att.vertex)));
        }
    }

    @Override
    public void plays(RoleType roleType) {
        if (sups().filter(t -> !t.equals(this)).flatMap(ThingType::plays).anyMatch(a -> a.equals(roleType))) {
            throw new GraknException(PLAYS_ROLE_NOT_AVAILABLE.format(roleType.label()));
        }
        vertex.outs().put(Schema.Edge.Type.PLAYS, ((RoleTypeImpl) roleType).vertex);
    }

    @Override
    public void plays(RoleType roleType, RoleType overriddenType) {
        plays(roleType);
        override(Schema.Edge.Type.PLAYS, roleType, overriddenType, sup().plays(),
                 stream(apply(vertex.outs().edge(Schema.Edge.Type.PLAYS).to(), RoleTypeImpl::of)));
    }

    @Override
    public void unplay(RoleType roleType) {
        vertex.outs().edge(Schema.Edge.Type.PLAYS, ((RoleTypeImpl) roleType).vertex).delete();
    }

    @Override
    public Stream<RoleTypeImpl> plays() {
        Stream<RoleTypeImpl> declared = stream(apply(vertex.outs().edge(Schema.Edge.Type.PLAYS).to(), RoleTypeImpl::of));
        if (isRoot()) {
            return declared;
        } else {
            Set<TypeVertex> overridden = new HashSet<>();
            filter(vertex.outs().edge(Schema.Edge.Type.PLAYS).overridden(), Objects::nonNull).forEachRemaining(overridden::add);
            return concat(declared, sup().plays().filter(att -> !overridden.contains(att.vertex)));
        }
    }

    @Override
    public void delete() {
        if (subs().anyMatch(s -> !s.equals(this))) {
            throw new GraknException(TYPE_HAS_SUBTYPES.format(label()));
        } else if (subs().flatMap(ThingType::instances).findFirst().isPresent()) {
            throw new GraknException(TYPE_HAS_INSTANCES.format(label()));
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
        return attributes().filter(TypeImpl::isAbstract)
                .map(attType -> new GraknException(HAS_ABSTRACT_ATT_TYPE.format(label(), attType.label())))
                .collect(Collectors.toList());
    }

    private List<GraknException> exceptions_playsAbstractRoleType() {
        return plays().filter(TypeImpl::isAbstract)
                .map(roleType -> new GraknException(PLAYS_ABSTRACT_ROLE_TYPE.format(label(), roleType.label())))
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
        public void label(String label) { throw new GraknException(ROOT_TYPE_MUTATION); }

        @Override
        public void isAbstract(boolean isAbstract) { throw new GraknException(ROOT_TYPE_MUTATION); }

        @Override
        public ThingTypeImpl sup() { return null; }

        @Override
        public Stream<? extends ThingTypeImpl> sups() {
            return Stream.of(this);
        }

        @Override
        public Stream<? extends ThingTypeImpl> subs() {
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
        public Stream<? extends Thing> instances() {
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
        public void has(AttributeType attributeType, boolean isKey) {
            throw new GraknException(ROOT_TYPE_MUTATION);
        }

        @Override
        public void has(AttributeType attributeType, AttributeType overriddenType, boolean isKey) {
            throw new GraknException(ROOT_TYPE_MUTATION);
        }

        @Override
        public void plays(RoleType roleType) {
            throw new GraknException(ROOT_TYPE_MUTATION);
        }

        @Override
        public void plays(RoleType roleType, RoleType overriddenType) {
            throw new GraknException(ROOT_TYPE_MUTATION);
        }

        @Override
        public void unplay(RoleType roleType) {
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
