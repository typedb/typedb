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

package hypergraph.concept.thing.impl;

import hypergraph.common.exception.Error;
import hypergraph.common.exception.HypergraphException;
import hypergraph.concept.thing.Attribute;
import hypergraph.concept.thing.Relation;
import hypergraph.concept.thing.Thing;
import hypergraph.concept.type.AttributeType;
import hypergraph.concept.type.RoleType;
import hypergraph.concept.type.impl.TypeImpl;
import hypergraph.graph.util.Schema;
import hypergraph.graph.vertex.ThingVertex;
import hypergraph.graph.vertex.TypeVertex;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

import static hypergraph.common.iterator.Iterators.apply;
import static hypergraph.common.iterator.Iterators.filter;
import static hypergraph.common.iterator.Iterators.stream;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public abstract class ThingImpl implements Thing {

    final ThingVertex vertex;

    ThingImpl(ThingVertex vertex) {
        this.vertex = Objects.requireNonNull(vertex);
    }

    @Override
    public String iid() {
        return vertex.iid().toHexString();
    }

    @Override
    public boolean isInferred() {
        return vertex.isInferred();
    }

    @Override
    public Thing has(Attribute attribute) {
        if (type().attributes().noneMatch(t -> t.equals(attribute.type()))) {
            throw new HypergraphException(Error.ThingWrite.THING_ATTRIBUTE_UNDEFINED.format(vertex.schema().name()));
        } else if (type().keys().anyMatch(t -> t.equals(attribute.type()))) {
            if (keys(attribute.type()).findAny().isPresent()) {
                throw new HypergraphException(Error.ThingWrite.THING_KEY_OVER.format(vertex.schema().name()));
            } else if (attribute.owners().findAny().isPresent()) {
                throw new HypergraphException(Error.ThingWrite.THING_KEY_TAKEN.format(vertex.schema().name()));
            }
        }

        vertex.outs().put(Schema.Edge.Thing.HAS, ((AttributeImpl) attribute).vertex);
        return this;
    }

    @Override
    public void unhas(Attribute attribute) {
        vertex.outs().delete(Schema.Edge.Thing.HAS, ((AttributeImpl) attribute).vertex);
    }

    @Override
    public Stream<? extends Attribute> keys(List<AttributeType> attributeTypes) {
        if (attributeTypes.isEmpty()) return attributes(type().keys().collect(toList()));

        attributeTypes.retainAll(type().keys().collect(toList()));
        if (attributeTypes.isEmpty()) return Stream.empty();
        else return attributes(attributeTypes);
    }

    @Override
    public Stream<? extends Attribute> attributes(List<AttributeType> attributeTypes) {
        Iterator<ThingVertex> vertices;
        if (!attributeTypes.isEmpty()) {
            Set<TypeVertex> filter = attributeTypes.stream().map(t -> ((TypeImpl) t).vertex).collect(toSet());
            vertices = filter(vertex.outs().edge(Schema.Edge.Thing.HAS).to(), v -> filter.contains(v.type()));
        } else {
            vertices = vertex.outs().edge(Schema.Edge.Thing.HAS).to();
        }

        return stream(apply(vertices, v -> AttributeImpl.of(v.asAttribute())));
    }

    @Override
    public Stream<? extends RoleType> roles() {
        return null; // TODO
    }

    @Override
    public Stream<? extends Relation> relations(List<RoleType> roleTypes) {
        return null; // TODO
    }

    @Override
    public void delete() {
        vertex.delete();
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        ThingImpl that = (ThingImpl) object;
        return this.vertex.equals(that.vertex);
    }

    @Override
    public final int hashCode() {
        return vertex.hashCode(); // does not need caching
    }
}
