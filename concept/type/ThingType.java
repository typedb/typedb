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
    public void setAbstract(boolean isAbstract) {
        vertex.setAbstract(isAbstract);
    }

    @Override
    public void sup(TYPE superType) {
        super.sup(superType);
    }

    public KeyOverrider key(AttributeType attributeType) {
        if (filter(vertex.outs().edge(Schema.Edge.Type.HAS).to(), v -> v.equals(attributeType.vertex)).hasNext()) {
            throw new HypergraphException("Invalid Key Assignment: " + attributeType.label() + " is already used as an attribute");
        } else if (sups().flatMap(ThingType::attributes).anyMatch(a -> a.equals(attributeType))) {
            throw new HypergraphException("Invalid Attribute Assignment: " + attributeType.label() + " is already inherited and/or overridden ");
        }

        vertex.outs().put(Schema.Edge.Type.KEY, attributeType.vertex);
        return new KeyOverrider(attributeType);
    }

    public void unkey(AttributeType attributeType) {
        vertex.outs().delete(Schema.Edge.Type.KEY, attributeType.vertex);
    }

    private Stream<AttributeType> declaredKeys() {
        Iterator<AttributeType> keys = apply(vertex.outs().edge(Schema.Edge.Type.KEY).to(), AttributeType::of);
        return stream(spliteratorUnknownSize(keys, ORDERED | IMMUTABLE), false);
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

    public HasOverrider has(AttributeType attributeType) {
        if (filter(vertex.outs().edge(Schema.Edge.Type.KEY).to(), v -> v.equals(attributeType.vertex)).hasNext()) {
            throw new HypergraphException("Invalid Attribute Assignment: " + attributeType.label() + " is already used as a key");
        } else if (sups().flatMap(ThingType::attributes).anyMatch(a -> a.equals(attributeType))) {
            throw new HypergraphException("Invalid Attribute Assignment: " + attributeType.label() + " is already inherited or overridden ");
        }

        vertex.outs().put(Schema.Edge.Type.HAS, attributeType.vertex);
        return new HasOverrider(attributeType);
    }

    public void unhas(AttributeType attributeType) {
        vertex.outs().delete(Schema.Edge.Type.HAS, attributeType.vertex);
    }

    private Stream<AttributeType> declaredAttributes() {
        Iterator<AttributeType> attributes = link(vertex.outs().edge(Schema.Edge.Type.KEY).to(),
                                                  vertex.outs().edge(Schema.Edge.Type.HAS).to()).apply(AttributeType::of);
        return stream(spliteratorUnknownSize(attributes, ORDERED | IMMUTABLE), false);
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

    public PlaysOverrider plays(RoleType roleType) {
        if (sups().flatMap(ThingType::plays).anyMatch(a -> a.equals(roleType))) {
            throw new HypergraphException("Invalid Attribute Assignment: " + roleType.label() + " is already inherited or overridden ");
        }
        vertex.outs().put(Schema.Edge.Type.PLAYS, roleType.vertex);
        return new PlaysOverrider(roleType);
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

    public static class Root extends ThingType {

        public Root(TypeVertex vertex) {
            super(vertex);
            assert vertex.label().equals(Schema.Vertex.Type.Root.THING.label());
        }

        @Override
        ThingType<?> newInstance(TypeVertex vertex) {
            if (vertex.schema().equals(Schema.Vertex.Type.ATTRIBUTE_TYPE)) return AttributeType.of(vertex);
            if (vertex.schema().equals(Schema.Vertex.Type.ENTITY_TYPE)) return EntityType.of(vertex);
            if (vertex.schema().equals(Schema.Vertex.Type.RELATION_TYPE)) return RelationType.of(vertex);
            return null;
        }

        @Override
        boolean isRoot() { return true; }

        @Override
        public void label(String label) {
            throw new HypergraphException("Invalid Operation Exception: root types are immutable");
        }

        @Override
        public void setAbstract(boolean isAbstract) {
            throw new HypergraphException("Invalid Operation Exception: root types are immutable");
        }

        @Override
        public ThingType sup() {
            return null;
        }

        @Override
        public void sup(ThingType superType) {
            throw new HypergraphException("Invalid Operation Exception: root types are immutable");
        }
    }

    public class KeyOverrider extends Overrider<AttributeType> {
        KeyOverrider(AttributeType attributeType) {
            super(Schema.Edge.Type.KEY, attributeType,
                  ThingType.this.sup().attributes(),
                  ThingType.this.declaredAttributes()
            );
        }
    }

    public class HasOverrider extends Overrider<AttributeType> {
        HasOverrider(AttributeType attributeType) {
            super(Schema.Edge.Type.HAS, attributeType,
                  ThingType.this.sup().attributes(),
                  concat(ThingType.this.sup().keys(), ThingType.this.declaredAttributes())
            );
        }
    }

    public class PlaysOverrider extends Overrider<RoleType> {
        PlaysOverrider(RoleType roleType) {
            super(Schema.Edge.Type.PLAYS, roleType,
                  ThingType.this.sup().plays(),
                  ThingType.this.declaredPlays()
            );
        }
    }

    public class Overrider<P extends Type<P>> {

        private final P type;
        private final Stream<P> overridable;
        private final Stream<P> notOverridable;
        private final Schema.Edge.Type schema;

        Overrider(Schema.Edge.Type schema, P type, Stream<P> overridable, Stream<P> notOverridable) {
            this.schema = schema;
            this.type = type;
            this.overridable = overridable;
            this.notOverridable = notOverridable;
        }

        public void as(P property) {
            if (notOverridable.anyMatch(prop -> prop.equals(property)) || overridable.noneMatch(prop -> prop.equals(property))) {
                throw new HypergraphException("Invalid Type Overriding: " + property.label() + " is not overridable");
            } else if (this.type.sups().noneMatch(prop -> prop.equals(property))) {
                throw new HypergraphException("Invalid Type Overriding: " + property.label() + " is not a supertype");
            }

            vertex.outs().edge(schema, this.type.vertex).overridden(property.vertex);
        }
    }
}
