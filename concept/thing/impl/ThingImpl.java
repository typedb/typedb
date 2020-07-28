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

package grakn.core.concept.thing.impl;

import grakn.core.common.exception.GraknException;
import grakn.core.concept.thing.Attribute;
import grakn.core.concept.thing.Thing;
import grakn.core.concept.type.AttributeType;
import grakn.core.concept.type.RoleType;
import grakn.core.concept.type.Type;
import grakn.core.concept.type.impl.RoleTypeImpl;
import grakn.core.concept.type.impl.TypeImpl;
import grakn.core.graph.iid.PrefixIID;
import grakn.core.graph.util.Schema;
import grakn.core.graph.vertex.AttributeVertex;
import grakn.core.graph.vertex.ThingVertex;
import grakn.core.graph.vertex.TypeVertex;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

import static grakn.common.util.Collections.list;
import static grakn.core.common.exception.Error.Internal.UNRECOGNISED_VALUE;
import static grakn.core.common.exception.Error.ThingWrite.THING_ATTRIBUTE_UNDEFINED;
import static grakn.core.common.exception.Error.ThingWrite.THING_KEY_MISSING;
import static grakn.core.common.exception.Error.ThingWrite.THING_KEY_OVER;
import static grakn.core.common.exception.Error.ThingWrite.THING_KEY_TAKEN;
import static grakn.core.common.iterator.Iterators.apply;
import static grakn.core.common.iterator.Iterators.link;
import static grakn.core.common.iterator.Iterators.stream;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public abstract class ThingImpl implements Thing {

    final ThingVertex vertex;

    ThingImpl(ThingVertex vertex) {
        this.vertex = Objects.requireNonNull(vertex);
    }

    public static ThingImpl of(ThingVertex vertex) {
        switch (vertex.schema()) {
            case ENTITY:
                return EntityImpl.of(vertex);
            case ATTRIBUTE:
                return AttributeImpl.of(vertex.asAttribute());
            case RELATION:
                return RelationImpl.of(vertex);
            default:
                throw new GraknException(UNRECOGNISED_VALUE);
        }
    }

    @Override
    public String iid() {
        return vertex.iid().toHexString();
    }

    @Override
    public boolean isDeleted() {
        return vertex.isDeleted();
    }

    @Override
    public boolean isInferred() {
        return vertex.isInferred();
    }

    @Override
    public Thing has(Attribute attribute) {
        if (type().attributes().noneMatch(t -> t.equals(attribute.type()))) {
            throw new GraknException(THING_ATTRIBUTE_UNDEFINED.message(vertex.type().label()));
        } else if (type().attributes(true).anyMatch(t -> t.equals(attribute.type()))) {
            if (attributes(attribute.type()).findAny().isPresent()) {
                throw new GraknException(THING_KEY_OVER.message(attribute.type().label(), type().label()));
            } else if (attribute.owners(type()).findAny().isPresent()) {
                throw new GraknException(THING_KEY_TAKEN.message(attribute.type().label(), type().label()));
            }
        }

        vertex.outs().put(Schema.Edge.Thing.HAS, ((AttributeImpl) attribute).vertex);
        return this;
    }

    @Override
    public void unhas(Attribute attribute) {
        vertex.outs().edge(Schema.Edge.Thing.HAS, ((AttributeImpl) attribute).vertex).delete();
    }

    @Override
    public Stream<AttributeImpl> attributes() {
        return attributes(false);
    }

    @Override
    public Stream<AttributeImpl> attributes(boolean onlyKey) {
        return attributes(type().attributes(onlyKey).collect(toList()));
    }

    @Override
    public Stream<AttributeImpl> attributes(AttributeType attributeType) {
        return attributeVertices(list(attributeType)).map(AttributeImpl::of);
    }

    @Override
    public Stream<AttributeImpl.Boolean> attributes(AttributeType.Boolean attributeType) {
        return attributeVertices(list(attributeType)).map(v -> AttributeImpl.of(v).asBoolean());
    }

    @Override
    public Stream<AttributeImpl.Long> attributes(AttributeType.Long attributeType) {
        return attributeVertices(list(attributeType)).map(v -> AttributeImpl.of(v).asLong());
    }

    @Override
    public Stream<AttributeImpl.Double> attributes(AttributeType.Double attributeType) {
        return attributeVertices(list(attributeType)).map(v -> AttributeImpl.of(v).asDouble());
    }

    @Override
    public Stream<AttributeImpl.String> attributes(AttributeType.String attributeType) {
        return attributeVertices(list(attributeType)).map(v -> AttributeImpl.of(v).asString());
    }

    @Override
    public Stream<AttributeImpl.DateTime> attributes(AttributeType.DateTime attributeType) {
        return attributeVertices(list(attributeType)).map(v -> AttributeImpl.of(v).asDateTime());
    }

    @Override
    public Stream<AttributeImpl> attributes(List<AttributeType> attributeType) {
        return attributeVertices(attributeType).map(AttributeImpl::of);
    }

    private Stream<AttributeVertex> attributeVertices(List<? extends AttributeType> attributeTypes) {
        if (!attributeTypes.isEmpty()) {
            return attributeTypes.stream()
                    .flatMap(AttributeType::subs).distinct()
                    .map(t -> ((TypeImpl) t).vertex)
                    .flatMap(this::attributeVertices);
        } else {
            return stream(apply(vertex.outs().edge(Schema.Edge.Thing.HAS).to(), ThingVertex::asAttribute));
        }
    }

    private Stream<AttributeVertex> attributeVertices(TypeVertex attributeType) {
        return stream(apply(vertex.outs().edge(
                Schema.Edge.Thing.HAS,
                PrefixIID.of(Schema.Vertex.Thing.of(attributeType.schema()).prefix()),
                attributeType.iid()
        ).to(), ThingVertex::asAttribute));
    }

    @Override
    public Stream<RoleType> roles() {
        return stream(apply(apply(vertex.outs().edge(Schema.Edge.Thing.PLAYS).to(), ThingVertex::type), RoleTypeImpl::of));
    }

    @Override
    public Stream<RelationImpl> relations(List<RoleType> roleTypes) {
        if (roleTypes.isEmpty()) {
            return stream(apply(vertex.ins().edge(Schema.Edge.Thing.ROLEPLAYER).from(), RelationImpl::of));
        } else {
            List<Iterator<ThingVertex>> iterators = new LinkedList<>();
            for (RoleType roleType : roleTypes) {
                iterators.add(vertex.ins().edge(Schema.Edge.Thing.ROLEPLAYER, ((RoleTypeImpl) roleType).vertex.iid()).from());
            }
            return stream(apply(link(iterators), RelationImpl::of));
        }
    }

    @Override
    public void delete() {
        vertex.delete();
    }

    @Override
    public void validate() {
        if (attributes(true).map(Attribute::type).count() < type().attributes(true).count()) {
            Set<AttributeType> missing = type().attributes(true).collect(toSet());
            missing.removeAll(attributes(true).map(Attribute::type).collect(toSet()));
            throw new GraknException(THING_KEY_MISSING.message(type().label(), printTypeSet(missing)));
        }
    }

    private String printTypeSet(Set<? extends Type> types) {
        Type[] array = types.toArray(new Type[0]);
        StringBuilder string = new StringBuilder();
        for (int i = 0; i < array.length; i++) {
            string.append('\'').append(array[i].label()).append('\'');
            if (i < array.length - 1) string.append(", ");
        }
        return string.toString();
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
