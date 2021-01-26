/*
 * Copyright (C) 2021 Grakn Labs
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
import grakn.core.concept.ConceptImpl;
import grakn.core.concept.thing.Attribute;
import grakn.core.concept.thing.Thing;
import grakn.core.concept.type.AttributeType;
import grakn.core.concept.type.RoleType;
import grakn.core.concept.type.Type;
import grakn.core.concept.type.impl.RoleTypeImpl;
import grakn.core.concept.type.impl.TypeImpl;
import grakn.core.graph.iid.PrefixIID;
import grakn.core.graph.vertex.AttributeVertex;
import grakn.core.graph.vertex.ThingVertex;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

import static grakn.common.collection.Collections.list;
import static grakn.common.util.Objects.className;
import static grakn.core.common.exception.ErrorMessage.Internal.UNRECOGNISED_VALUE;
import static grakn.core.common.exception.ErrorMessage.ThingRead.INVALID_ROLE_TYPE_LABEL;
import static grakn.core.common.exception.ErrorMessage.ThingWrite.THING_CANNOT_OWN_ATTRIBUTE;
import static grakn.core.common.exception.ErrorMessage.ThingWrite.THING_KEY_MISSING;
import static grakn.core.common.exception.ErrorMessage.ThingWrite.THING_KEY_OVER;
import static grakn.core.common.exception.ErrorMessage.ThingWrite.THING_KEY_TAKEN;
import static grakn.core.graph.common.Encoding.Edge.Thing.HAS;
import static grakn.core.graph.common.Encoding.Edge.Thing.PLAYING;
import static grakn.core.graph.common.Encoding.Edge.Thing.RELATING;
import static grakn.core.graph.common.Encoding.Edge.Thing.ROLEPLAYER;
import static java.util.Arrays.stream;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.Stream.concat;

public abstract class ThingImpl extends ConceptImpl implements Thing {

    final ThingVertex vertex;

    ThingImpl(ThingVertex vertex) {
        this.vertex = Objects.requireNonNull(vertex);
    }

    public static ThingImpl of(ThingVertex vertex) {
        switch (vertex.encoding()) {
            case ENTITY:
                return EntityImpl.of(vertex);
            case ATTRIBUTE:
                return AttributeImpl.of(vertex.asAttribute());
            case RELATION:
                return RelationImpl.of(vertex);
            default:
                throw vertex.graphs().exception(GraknException.of(UNRECOGNISED_VALUE));
        }
    }

    @Override
    public byte[] getIID() {
        return vertex.iid().bytes();
    }

    @Override
    public String getIIDForPrinting() {
        return vertex.iid().toString();
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
    public void setHas(Attribute attribute) {
        setHas(attribute, false);
    }

    @Override
    public void setHas(Attribute attribute, boolean isInferred) {
        if (getType().getOwns().noneMatch(t -> t.equals(attribute.getType()))) {
            throw exception(GraknException.of(THING_CANNOT_OWN_ATTRIBUTE, attribute.getType().getLabel(), vertex.type().label()));
        } else if (getType().getOwns(true).anyMatch(t -> t.equals(attribute.getType()))) {
            if (getHas(attribute.getType()).findAny().isPresent()) {
                throw exception(GraknException.of(THING_KEY_OVER, attribute.getType().getLabel(), getType().getLabel()));
            } else if (attribute.getOwners(getType()).findAny().isPresent()) {
                throw exception(GraknException.of(THING_KEY_TAKEN, attribute.getType().getLabel(), getType().getLabel()));
            }
        }
        vertex.outs().put(HAS, ((AttributeImpl<?>) attribute).vertex, isInferred);
    }

    @Override
    public void unsetHas(Attribute attribute) {
        vertex.outs().edge(HAS, ((AttributeImpl<?>) attribute).vertex).delete();
    }

    @Override
    public Stream<AttributeImpl<?>> getHas(boolean onlyKey) {
        return getHas(getType().getOwns(onlyKey).toArray(AttributeType[]::new));
    }

    @Override
    public Stream<AttributeImpl.Boolean> getHas(AttributeType.Boolean attributeType) {
        return getAttributeVertices(list(attributeType)).map(v -> AttributeImpl.of(v).asBoolean());
    }

    @Override
    public Stream<AttributeImpl.Long> getHas(AttributeType.Long attributeType) {
        return getAttributeVertices(list(attributeType)).map(v -> AttributeImpl.of(v).asLong());
    }

    @Override
    public Stream<AttributeImpl.Double> getHas(AttributeType.Double attributeType) {
        return getAttributeVertices(list(attributeType)).map(v -> AttributeImpl.of(v).asDouble());
    }

    @Override
    public Stream<AttributeImpl.String> getHas(AttributeType.String attributeType) {
        return getAttributeVertices(list(attributeType)).map(v -> AttributeImpl.of(v).asString());
    }

    @Override
    public Stream<AttributeImpl.DateTime> getHas(AttributeType.DateTime attributeType) {
        return getAttributeVertices(list(attributeType)).map(v -> AttributeImpl.of(v).asDateTime());
    }

    @Override
    public Stream<AttributeImpl<?>> getHas(AttributeType... attributeTypes) {
        if (attributeTypes.length == 0) {
            return getAttributeVertices(getType().getOwns().collect(toList())).map(AttributeImpl::of);
        }
        return getAttributeVertices(Arrays.asList(attributeTypes)).map(AttributeImpl::of);
    }

    private Stream<? extends AttributeVertex<?>> getAttributeVertices(List<? extends AttributeType> attributeTypes) {
        if (!attributeTypes.isEmpty()) {
            return attributeTypes.stream()
                    .flatMap(AttributeType::getSubtypes).distinct()
                    .map(t -> ((TypeImpl) t).vertex)
                    .flatMap(type -> vertex.outs().edge(
                            HAS, PrefixIID.of(type.encoding().instance()), type.iid()
                    ).to().stream()).map(ThingVertex::asAttribute);
        } else {
            return vertex.outs().edge(HAS).to().map(ThingVertex::asAttribute).stream();
        }
    }

    @Override
    public Stream<? extends RoleType> getPlays() {
        return vertex.outs().edge(PLAYING).to().map(ThingVertex::type).map(v -> RoleTypeImpl.of(vertex.graphs(), v)).stream();
    }

    @Override
    public Stream<RelationImpl> getRelations(String roleType, String... roleTypes) {
        return getRelations(concat(Stream.of(roleType), stream(roleTypes)).map(scopedLabel -> {
            if (!scopedLabel.contains(":")) {
                throw exception(GraknException.of(INVALID_ROLE_TYPE_LABEL, scopedLabel));
            }
            String[] label = scopedLabel.split(":");
            return RoleTypeImpl.of(vertex.graphs(), vertex.graph().schema().getType(label[1], label[0]));
        }).toArray(RoleType[]::new));
    }

    @Override
    public Stream<RelationImpl> getRelations(RoleType... roleTypes) {
        if (roleTypes.length == 0) {
            return vertex.ins().edge(ROLEPLAYER).from().map(RelationImpl::of).stream();
        } else {
            return stream(roleTypes).flatMap(RoleType::getSubtypes).distinct().flatMap(
                    rt -> vertex.ins().edge(ROLEPLAYER, ((RoleTypeImpl) rt).vertex.iid()).from().stream()
            ).map(RelationImpl::of);
        }
    }

    @Override
    public void delete() {
        Set<RelationImpl> relations = vertex.ins().edge(ROLEPLAYER).from().map(RelationImpl::of).toSet();
        vertex.outs().edge(PLAYING).to().map(RoleImpl::of).forEachRemaining(RoleImpl::delete);
        vertex.delete();
        relations.forEach(RelationImpl::deleteIfNoPlayer);
    }

    @Override
    public void validate() {
        if (getHas(true).map(Attribute::getType).count() < getType().getOwns(true).count()) {
            Set<AttributeType> missing = getType().getOwns(true).collect(toSet());
            missing.removeAll(getHas(true).map(Attribute::getType).collect(toSet()));
            throw exception(GraknException.of(THING_KEY_MISSING, getType().getLabel(), printTypeSet(missing)));
        }
    }

    @Override
    public ThingImpl asThing() { return this; }

    @Override
    public boolean isThing() { return true; }

    private String printTypeSet(Set<? extends Type> types) {
        Type[] array = types.toArray(new Type[0]);
        StringBuilder string = new StringBuilder();
        for (int i = 0; i < array.length; i++) {
            string.append('\'').append(array[i].getLabel()).append('\'');
            if (i < array.length - 1) string.append(", ");
        }
        return string.toString();
    }

    @Override
    public GraknException exception(GraknException exception) {
        return vertex.graphs().exception(exception);
    }

    @Override
    public String toString() {
        return className(this.getClass()) + " {" + vertex.toString() + "}";
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
