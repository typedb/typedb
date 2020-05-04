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

import hypergraph.common.exception.HypergraphException;
import hypergraph.graph.Graph;
import hypergraph.graph.Schema;
import hypergraph.graph.vertex.TypeVertex;

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

public abstract class ThingType<TYPE extends ThingType<TYPE>> extends Type<TYPE> {

    ThingType(TypeVertex vertex) {
        super(vertex);
    }

    ThingType(Graph.Type graph, String label, Schema.Vertex.Type schema) {
        super(graph, label, schema);
    }

    @Override
    public void isAbstract(boolean isAbstract) {
        vertex.isAbstract(isAbstract);
    }

    @Override
    public void sup(TYPE superType) {
        super.sup(superType);
    }

    private <T extends Type<T>> void override(Schema.Edge.Type schema, T type, T overriddenType,
                                              Stream<? extends Type> overridable, Stream<? extends Type> notOverridable) {
        if (notOverridable.anyMatch(t -> t.equals(overriddenType)) || overridable.noneMatch(t -> t.equals(overriddenType))) {
            throw new HypergraphException("Invalid Type Overriding: " + overriddenType.label() + " cannot be overridden");
        } else if (type.sups().noneMatch(t -> t.equals(overriddenType))) {
            throw new HypergraphException("Invalid Type Overriding: " + overriddenType.label() + " is not a supertype");
        }

        vertex.outs().edge(schema, type.vertex).overridden(overriddenType.vertex);
    }

    public TYPE key(AttributeType attributeType) {
        if (filter(vertex.outs().edge(Schema.Edge.Type.HAS).to(), v -> v.equals(attributeType.vertex)).hasNext()) {
            throw new HypergraphException("Invalid Key Assignment: " + attributeType.label() + " is already used as an attribute");
        } else if (sups().flatMap(ThingType::attributes).anyMatch(a -> a.equals(attributeType))) {
            throw new HypergraphException("Invalid Attribute Assignment: " + attributeType.label() + " is already inherited and/or overridden ");
        }

        vertex.outs().put(Schema.Edge.Type.KEY, attributeType.vertex);
        return getThis();
    }

    public <ATT_TYPE extends AttributeType<ATT_TYPE>> TYPE key(ATT_TYPE attributeType, ATT_TYPE overriddenType) {
        this.key(attributeType);
        Stream<AttributeType> overridable = sup().attributes();
        Stream<AttributeType> notOverridable = declaredAttributes();
        override(Schema.Edge.Type.KEY, attributeType, overriddenType, overridable, notOverridable);
        return getThis();
    }

    public void unkey(AttributeType attributeType) {
        vertex.outs().delete(Schema.Edge.Type.KEY, attributeType.vertex);
    }

    public Stream<AttributeType> keys(Class<?> valueClass) {
        return keys().filter(att -> att.valueClass().equals(valueClass));
    }

    public Stream<AttributeType> keys() {
        Iterator<AttributeType> keys = apply(vertex.outs().edge(Schema.Edge.Type.KEY).to(), AttributeType::of);
        if (isRoot()) {
            return stream(spliteratorUnknownSize(keys, ORDERED | IMMUTABLE), false);
        } else {
            Set<AttributeType> direct = new HashSet<>(), overridden = new HashSet<>();
            keys.forEachRemaining(direct::add);
            filter(vertex.outs().edge(Schema.Edge.Type.KEY).overridden(), Objects::nonNull)
                    .apply(AttributeType::of)
                    .forEachRemaining(overridden::add);
            return concat(direct.stream(), sup().keys().filter(key -> !overridden.contains(key)));
        }
    }

    public TYPE has(AttributeType attributeType) {
        if (filter(vertex.outs().edge(Schema.Edge.Type.KEY).to(), v -> v.equals(attributeType.vertex)).hasNext()) {
            throw new HypergraphException("Invalid Attribute Assignment: " + attributeType.label() + " is already used as a key");
        } else if (sups().flatMap(ThingType::attributes).anyMatch(a -> a.equals(attributeType))) {
            throw new HypergraphException("Invalid Attribute Assignment: " + attributeType.label() + " is already inherited or overridden ");
        }

        vertex.outs().put(Schema.Edge.Type.HAS, attributeType.vertex);
        return getThis();
    }

    public <ATT_TYPE extends AttributeType<ATT_TYPE>> TYPE has(ATT_TYPE attributeType, ATT_TYPE overriddenType) {
        this.has(attributeType);
        Stream<AttributeType> overridable = ThingType.this.sup().attributes(attributeType.valueClass());
        Stream<AttributeType> notOverridable = concat(ThingType.this.sup().keys(), ThingType.this.declaredAttributes());
        override(Schema.Edge.Type.HAS, attributeType, overriddenType, overridable, notOverridable);
        return getThis();
    }

    public void unhas(AttributeType attributeType) {
        vertex.outs().delete(Schema.Edge.Type.HAS, attributeType.vertex);
    }

    private Stream<AttributeType> declaredAttributes() {
        Iterator<AttributeType> attributes = link(vertex.outs().edge(Schema.Edge.Type.KEY).to(),
                                                  vertex.outs().edge(Schema.Edge.Type.HAS).to()).apply(AttributeType::of);
        return stream(spliteratorUnknownSize(attributes, ORDERED | IMMUTABLE), false);
    }

    public Stream<AttributeType> attributes(Class<?> valueClass) {
        return attributes().filter(att -> att.valueClass().equals(valueClass));
    }

    public Stream<AttributeType> attributes() {
        Iterator<AttributeType> attributes = link(vertex.outs().edge(Schema.Edge.Type.KEY).to(),
                                                  vertex.outs().edge(Schema.Edge.Type.HAS).to()).apply(AttributeType::of);
        if (isRoot()) {
            return stream(spliteratorUnknownSize(attributes, ORDERED | IMMUTABLE), false);
        } else {
            Set<AttributeType> direct = new HashSet<>(), overridden = new HashSet<>();
            attributes.forEachRemaining(direct::add);
            link(vertex.outs().edge(Schema.Edge.Type.KEY).overridden(),
                 vertex.outs().edge(Schema.Edge.Type.HAS).overridden()
            ).filter(Objects::nonNull).apply(AttributeType::of).forEachRemaining(overridden::add);
            return concat(direct.stream(), sup().attributes().filter(att -> !overridden.contains(att)));
        }
    }

    public TYPE plays(RoleType roleType) {
        if (sups().flatMap(ThingType::plays).anyMatch(a -> a.equals(roleType))) {
            throw new HypergraphException("Invalid Attribute Assignment: " + roleType.label() + " is already inherited or overridden ");
        }
        vertex.outs().put(Schema.Edge.Type.PLAYS, roleType.vertex);
        return getThis();
    }

    public TYPE plays(RoleType roleType, RoleType overriddenType) {
        this.plays(roleType);
        Stream<RoleType> overridable = this.sup().plays();
        Stream<RoleType> notOverridable = this.declaredPlays();
        override(Schema.Edge.Type.PLAYS, roleType, overriddenType, overridable, notOverridable);
        return getThis();
    }

    public void unplay(RoleType roleType) {
        vertex.outs().delete(Schema.Edge.Type.PLAYS, roleType.vertex);
    }

    private Stream<RoleType> declaredPlays() {
        Iterator<RoleType> roles = apply(vertex.outs().edge(Schema.Edge.Type.PLAYS).to(), RoleType::of);
        return stream(spliteratorUnknownSize(roles, ORDERED | IMMUTABLE), false);
    }

    public Stream<RoleType> plays() {
        Iterator<RoleType> roles = apply(vertex.outs().edge(Schema.Edge.Type.PLAYS).to(), RoleType::of);
        if (isRoot()) {
            return stream(spliteratorUnknownSize(roles, ORDERED | IMMUTABLE), false);
        } else {
            Set<RoleType> direct = new HashSet<>(), overridden = new HashSet<>();
            roles.forEachRemaining(direct::add);
            filter(vertex.outs().edge(Schema.Edge.Type.PLAYS).overridden(), Objects::nonNull)
                    .apply(RoleType::of).forEachRemaining(overridden::add);
            return concat(direct.stream(), sup().plays().filter(att -> !overridden.contains(att)));
        }
    }

    public static class Root extends ThingType<ThingType.Root> {

        public Root(TypeVertex vertex) {
            super(vertex);
            assert vertex.label().equals(Schema.Vertex.Type.Root.THING.label());
        }

        @Override
        ThingType.Root getThis() { return this; }

        @Override
        ThingType.Root newInstance(TypeVertex vertex) { return new ThingType.Root(vertex); }

        public boolean isRoot() { return true; }

        public void label(String label) { throw new HypergraphException(INVALID_ROOT_TYPE_MUTATION); }

        public void isAbstract(boolean isAbstract) { throw new HypergraphException(INVALID_ROOT_TYPE_MUTATION); }

        public ThingType.Root sup() { return null; }

        public void sup(ThingType.Root superType) { throw new HypergraphException(INVALID_ROOT_TYPE_MUTATION); }
    }
}
