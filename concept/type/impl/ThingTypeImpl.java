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

import hypergraph.common.exception.HypergraphException;
import hypergraph.concept.type.AttributeType;
import hypergraph.concept.type.RoleType;
import hypergraph.concept.type.ThingType;
import hypergraph.concept.type.Type;
import hypergraph.graph.Graph;
import hypergraph.graph.Schema;
import hypergraph.graph.vertex.TypeVertex;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

import static hypergraph.common.exception.Error.TypeDefinition.INVALID_ROOT_TYPE_MUTATION;
import static hypergraph.common.iterator.Iterators.apply;
import static hypergraph.common.iterator.Iterators.filter;
import static hypergraph.common.iterator.Iterators.link;
import static java.util.Spliterator.IMMUTABLE;
import static java.util.Spliterator.ORDERED;
import static java.util.Spliterators.spliteratorUnknownSize;
import static java.util.stream.Stream.concat;
import static java.util.stream.StreamSupport.stream;

public abstract class ThingTypeImpl extends TypeImpl implements ThingType {

    ThingTypeImpl(TypeVertex vertex) {
        super(vertex);
    }

    ThingTypeImpl(Graph.Type graph, String label, Schema.Vertex.Type schema) {
        super(graph, label, schema);
    }

    @Override
    public void isAbstract(boolean isAbstract) {
        vertex.isAbstract(isAbstract);
    }

    @Nullable
    public abstract ThingTypeImpl sup();

    private <T extends Type> void override(Schema.Edge.Type schema, T type, T overriddenType,
                                           Stream<? extends Type> overridable, Stream<? extends Type> notOverridable) {
        if (notOverridable.anyMatch(t -> t.equals(overriddenType)) || overridable.noneMatch(t -> t.equals(overriddenType))) {
            throw new HypergraphException("Invalid Type Overriding: " + overriddenType.label() + " cannot be overridden");
        } else if (type.sups().noneMatch(t -> t.equals(overriddenType))) {
            throw new HypergraphException("Invalid Type Overriding: " + overriddenType.label() + " is not a supertype");
        }

        vertex.outs().edge(schema, ((TypeImpl) type).vertex).overridden(((TypeImpl) overriddenType).vertex);
    }

    @Override
    public void key(AttributeType attributeTypeInt) {
        AttributeTypeImpl attributeTypeImpl = (AttributeTypeImpl) attributeTypeInt;
        if (filter(vertex.outs().edge(Schema.Edge.Type.HAS).to(), v -> v.equals(attributeTypeImpl.vertex)).hasNext()) {
            throw new HypergraphException("Invalid Key Assignment: " + attributeTypeImpl.label() + " is already used as an attribute");
        } else if (sups().flatMap(ThingType::attributes).anyMatch(a -> a.equals(attributeTypeImpl))) {
            throw new HypergraphException("Invalid Attribute Assignment: " + attributeTypeImpl.label() + " is already inherited and/or overridden ");
        }

        vertex.outs().put(Schema.Edge.Type.KEY, attributeTypeImpl.vertex);
    }

    @Override
    public void key(AttributeType attributeType, AttributeType overriddenType) {
        this.key(attributeType);
        Stream<AttributeTypeImpl> overridable = sup().attributes();
        Stream<AttributeTypeImpl> notOverridable = declaredAttributes();
        override(Schema.Edge.Type.KEY, attributeType, overriddenType, overridable, notOverridable);
    }

    @Override
    public void unkey(AttributeType attributeType) {
        vertex.outs().delete(Schema.Edge.Type.KEY, ((AttributeTypeImpl) attributeType).vertex);
    }

    @Override
    public Stream<AttributeTypeImpl> keys(Class<?> valueClass) {
        return keys().filter(att -> att.valueClass().equals(valueClass));
    }

    @Override
    public Stream<AttributeTypeImpl> keys() {
        Iterator<AttributeTypeImpl> keys = apply(vertex.outs().edge(Schema.Edge.Type.KEY).to(), AttributeTypeImpl::of);
        if (isRoot()) {
            return stream(spliteratorUnknownSize(keys, ORDERED | IMMUTABLE), false);
        } else {
            Set<AttributeTypeImpl> direct = new HashSet<>(), overridden = new HashSet<>();
            keys.forEachRemaining(direct::add);
            filter(vertex.outs().edge(Schema.Edge.Type.KEY).overridden(), Objects::nonNull)
                    .apply(AttributeTypeImpl::of)
                    .forEachRemaining(overridden::add);
            return concat(direct.stream(), sup().keys().filter(key -> !overridden.contains(key)));
        }
    }

    @Override
    public void has(AttributeType attributeTypeInt) {
        AttributeTypeImpl attributeTypeImpl = (AttributeTypeImpl) attributeTypeInt;
        if (filter(vertex.outs().edge(Schema.Edge.Type.KEY).to(), v -> v.equals(attributeTypeImpl.vertex)).hasNext()) {
            throw new HypergraphException("Invalid Attribute Assignment: " + attributeTypeImpl.label() + " is already used as a key");
        } else if (sups().flatMap(ThingType::attributes).anyMatch(a -> a.equals(attributeTypeImpl))) {
            throw new HypergraphException("Invalid Attribute Assignment: " + attributeTypeImpl.label() + " is already inherited or overridden ");
        }

        vertex.outs().put(Schema.Edge.Type.HAS, attributeTypeImpl.vertex);
    }

    @Override
    public void has(AttributeType attributeType, AttributeType overriddenType) {
        this.has(attributeType);
        Stream<AttributeTypeImpl> overridable = ThingTypeImpl.this.sup().attributes(attributeType.valueClass());
        Stream<AttributeTypeImpl> notOverridable = concat(ThingTypeImpl.this.sup().keys(), ThingTypeImpl.this.declaredAttributes());
        override(Schema.Edge.Type.HAS, attributeType, overriddenType, overridable, notOverridable);
    }

    @Override
    public void unhas(AttributeType attributeType) {
        vertex.outs().delete(Schema.Edge.Type.HAS, ((AttributeTypeImpl) attributeType).vertex);
    }

    private Stream<AttributeTypeImpl> declaredAttributes() {
        Iterator<AttributeTypeImpl> attributes = link(vertex.outs().edge(Schema.Edge.Type.KEY).to(),
                                                      vertex.outs().edge(Schema.Edge.Type.HAS).to()).apply(AttributeTypeImpl::of);
        return stream(spliteratorUnknownSize(attributes, ORDERED | IMMUTABLE), false);
    }

    @Override
    public Stream<AttributeTypeImpl> attributes(Class<?> valueClass) {
        return attributes().filter(att -> att.valueClass().equals(valueClass));
    }

    @Override
    public Stream<AttributeTypeImpl> attributes() {
        Iterator<AttributeTypeImpl> attributes = link(vertex.outs().edge(Schema.Edge.Type.KEY).to(),
                                                      vertex.outs().edge(Schema.Edge.Type.HAS).to()).apply(AttributeTypeImpl::of);
        if (isRoot()) {
            return stream(spliteratorUnknownSize(attributes, ORDERED | IMMUTABLE), false);
        } else {
            Set<AttributeTypeImpl> direct = new HashSet<>(), overridden = new HashSet<>();
            attributes.forEachRemaining(direct::add);
            link(vertex.outs().edge(Schema.Edge.Type.KEY).overridden(),
                 vertex.outs().edge(Schema.Edge.Type.HAS).overridden()
            ).filter(Objects::nonNull).apply(AttributeTypeImpl::of).forEachRemaining(overridden::add);
            return concat(direct.stream(), sup().attributes().filter(att -> !overridden.contains(att)));
        }
    }

    @Override
    public void plays(RoleType roleType) {
        RoleTypeImpl roleTypeImpl = (RoleTypeImpl) roleType;
        if (sups().flatMap(ThingType::plays).anyMatch(a -> a.equals(roleTypeImpl))) {
            throw new HypergraphException("Invalid Attribute Assignment: " + roleTypeImpl.label() + " is already inherited or overridden ");
        }
        vertex.outs().put(Schema.Edge.Type.PLAYS, roleTypeImpl.vertex);
    }

    @Override
    public void plays(RoleType roleType, RoleType overriddenType) {
        RoleTypeImpl roleTypeImpl = (RoleTypeImpl) roleType;
        RoleTypeImpl overriddenTypeImpl = (RoleTypeImpl) overriddenType;
        this.plays(roleTypeImpl);
        Stream<RoleTypeImpl> overridable = this.sup().plays();
        Stream<RoleTypeImpl> notOverridable = this.declaredPlays();
        override(Schema.Edge.Type.PLAYS, roleTypeImpl, overriddenTypeImpl, overridable, notOverridable);
    }

    @Override
    public void unplay(RoleType roleType) {
        RoleTypeImpl roleTypeImpl = (RoleTypeImpl) roleType;
        vertex.outs().delete(Schema.Edge.Type.PLAYS, roleTypeImpl.vertex);
    }

    private Stream<RoleTypeImpl> declaredPlays() {
        Iterator<RoleTypeImpl> roles = apply(vertex.outs().edge(Schema.Edge.Type.PLAYS).to(), RoleTypeImpl::of);
        return stream(spliteratorUnknownSize(roles, ORDERED | IMMUTABLE), false);
    }

    @Override
    public Stream<RoleTypeImpl> plays() {
        Iterator<RoleTypeImpl> roles = apply(vertex.outs().edge(Schema.Edge.Type.PLAYS).to(), RoleTypeImpl::of);
        if (isRoot()) {
            return stream(spliteratorUnknownSize(roles, ORDERED | IMMUTABLE), false);
        } else {
            Set<RoleTypeImpl> direct = new HashSet<>(), overridden = new HashSet<>();
            roles.forEachRemaining(direct::add);
            filter(vertex.outs().edge(Schema.Edge.Type.PLAYS).overridden(), Objects::nonNull)
                    .apply(RoleTypeImpl::of).forEachRemaining(overridden::add);
            return concat(direct.stream(), sup().plays().filter(att -> !overridden.contains(att)));
        }
    }

    public static class Root extends ThingTypeImpl {

        public Root(TypeVertex vertex) {
            super(vertex);
            assert vertex.label().equals(Schema.Vertex.Type.Root.THING.label());
        }

        @Override
        public boolean isRoot() { return true; }

        @Override
        public void label(String label) { throw new HypergraphException(INVALID_ROOT_TYPE_MUTATION); }

        @Override
        public void isAbstract(boolean isAbstract) { throw new HypergraphException(INVALID_ROOT_TYPE_MUTATION); }

        @Override
        public ThingTypeImpl sup() { return null; }

        @Override
        public Stream<? extends ThingTypeImpl> sups() {
            return Stream.empty();
        }

        @Override
        public Stream<? extends ThingTypeImpl> subs() {
            Stream<TypeVertex> directSubTypeVertices = stream(spliteratorUnknownSize(
                    vertex.ins().edge(Schema.Edge.Type.SUB).from(), ORDERED | IMMUTABLE
            ), false);

            return directSubTypeVertices.flatMap(vertex -> {
                switch (vertex.schema()) {
                    case ENTITY_TYPE:
                        return EntityTypeImpl.of(vertex).subs();
                    case ATTRIBUTE_TYPE:
                        return AttributeTypeImpl.of(vertex).subs();
                    case RELATION_TYPE:
                        return RelationTypeImpl.of(vertex).subs();
                    default:
                        throw new HypergraphException("Unreachable");
                }
            });
        }
    }
}
