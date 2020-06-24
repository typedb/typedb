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
import hypergraph.concept.thing.Thing;
import hypergraph.concept.type.AttributeType;
import hypergraph.concept.type.RoleType;
import hypergraph.concept.type.ThingType;
import hypergraph.concept.type.Type;
import hypergraph.graph.TypeGraph;
import hypergraph.graph.util.Schema;
import hypergraph.graph.vertex.TypeVertex;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

import static hypergraph.common.exception.Error.TypeWrite.INVALID_KEY_ATTRIBUTE;
import static hypergraph.common.exception.Error.TypeWrite.INVALID_ROOT_TYPE_MUTATION;
import static hypergraph.common.iterator.Iterators.apply;
import static hypergraph.common.iterator.Iterators.filter;
import static hypergraph.common.iterator.Iterators.link;
import static hypergraph.common.iterator.Iterators.stream;
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
    public void key(AttributeType attributeType) {
        AttributeTypeImpl attributeTypeImpl = (AttributeTypeImpl) attributeType;
        if (!attributeType.isKeyable()) {
            throw new HypergraphException(INVALID_KEY_ATTRIBUTE.format(attributeTypeImpl.label(), attributeTypeImpl.valueType().getSimpleName()));
        } else if (filter(vertex.outs().edge(Schema.Edge.Type.HAS).to(), v -> v.equals(attributeTypeImpl.vertex)).hasNext()) {
            throw new HypergraphException("Invalid Key Assignment: " + attributeTypeImpl.label() + " is already used as an attribute");
        } else if (sups().filter(s -> !s.equals(this)).flatMap(ThingType::attributes).anyMatch(a -> a.equals(attributeTypeImpl))) {
            // TODO: should this be relaxed to just .flatMap(ThingType::keys) ?
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
        vertex.outs().edge(Schema.Edge.Type.KEY, ((AttributeTypeImpl) attributeType).vertex).delete();
    }

    @Override
    public Stream<AttributeTypeImpl> keys(Class<?> valueType) {
        return keys().filter(att -> att.valueType().equals(valueType));
    }

    @Override
    public Stream<AttributeTypeImpl> keys() {
        Iterator<AttributeTypeImpl> keys = apply(vertex.outs().edge(Schema.Edge.Type.KEY).to(), AttributeTypeImpl::of);
        if (isRoot()) {
            return stream(keys);
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
    public void has(AttributeType attributeType) {
        AttributeTypeImpl attributeTypeImpl = (AttributeTypeImpl) attributeType;
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
        Stream<AttributeTypeImpl> overridable = ThingTypeImpl.this.sup().attributes(attributeType.valueType());
        Stream<AttributeTypeImpl> notOverridable = concat(ThingTypeImpl.this.sup().keys(), ThingTypeImpl.this.declaredAttributes());
        override(Schema.Edge.Type.HAS, attributeType, overriddenType, overridable, notOverridable);
    }

    @Override
    public void unhas(AttributeType attributeType) {
        vertex.outs().edge(Schema.Edge.Type.HAS, ((AttributeTypeImpl) attributeType).vertex).delete();
    }

    private Stream<AttributeTypeImpl> declaredAttributes() {
        return stream(link(
                vertex.outs().edge(Schema.Edge.Type.KEY).to(),
                vertex.outs().edge(Schema.Edge.Type.HAS).to()
        ).apply(AttributeTypeImpl::of));
    }

    @Override
    public Stream<AttributeTypeImpl> attributes(Class<?> valueType) {
        return attributes().filter(att -> att.valueType().equals(valueType));
    }

    @Override
    public Stream<AttributeTypeImpl> attributes() {
        Iterator<AttributeTypeImpl> attributes = link(
                vertex.outs().edge(Schema.Edge.Type.KEY).to(),
                vertex.outs().edge(Schema.Edge.Type.HAS).to()
        ).apply(AttributeTypeImpl::of);

        if (isRoot()) {
            return stream(attributes);
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
        vertex.outs().edge(Schema.Edge.Type.PLAYS, roleTypeImpl.vertex).delete();
    }

    private Stream<RoleTypeImpl> declaredPlays() {
        return stream(apply(vertex.outs().edge(Schema.Edge.Type.PLAYS).to(), RoleTypeImpl::of));
    }

    @Override
    public Stream<RoleTypeImpl> plays() {
        Iterator<RoleTypeImpl> roles = apply(vertex.outs().edge(Schema.Edge.Type.PLAYS).to(), RoleTypeImpl::of);
        if (isRoot()) {
            return stream(roles);
        } else {
            Set<RoleTypeImpl> direct = new HashSet<>(), overridden = new HashSet<>();
            roles.forEachRemaining(direct::add);
            filter(vertex.outs().edge(Schema.Edge.Type.PLAYS).overridden(), Objects::nonNull)
                    .apply(RoleTypeImpl::of).forEachRemaining(overridden::add);
            return concat(direct.stream(), sup().plays().filter(att -> !overridden.contains(att)));
        }
    }

    @Override
    public void delete() {
        if (subs().anyMatch(s -> !s.equals(this))) {
            throw new HypergraphException("Invalid Type Removal: " + label() + " has subtypes");
        } else if (subs().flatMap(ThingType::instances).findAny().isPresent()) {
            throw new HypergraphException("Invalid Type Removal: " + label() + " has instances");
        } else {
            vertex.delete();
        }
    }

    @Override
    public void validate() {
        super.validate();
        // TODO: Add any validation that would apply to all ThingTypes here
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
                        throw new HypergraphException("Unreachable");
                }
            });
        }

        @Override
        public Stream<? extends Thing> instances() {
            return subs().filter(t -> !t.isAbstract()).flatMap(ThingType::instances);
        }

        /**
         * No-op validation method of the root type 'thing'.
         *
         * There's nothing to validate for the root type 'thing'.
         */
        @Override
        public void validate() {}
    }
}
