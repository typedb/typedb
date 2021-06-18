/*
 * Copyright (C) 2021 Vaticle
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

package com.vaticle.typedb.core.concept.thing.impl;

import com.vaticle.typedb.core.common.collection.ByteArray;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.concept.Concept;
import com.vaticle.typedb.core.concept.ConceptImpl;
import com.vaticle.typedb.core.concept.thing.Attribute;
import com.vaticle.typedb.core.concept.thing.Thing;
import com.vaticle.typedb.core.concept.type.AttributeType;
import com.vaticle.typedb.core.concept.type.RoleType;
import com.vaticle.typedb.core.concept.type.Type;
import com.vaticle.typedb.core.concept.type.impl.RoleTypeImpl;
import com.vaticle.typedb.core.concept.type.impl.ThingTypeImpl;
import com.vaticle.typedb.core.concept.type.impl.TypeImpl;
import com.vaticle.typedb.core.graph.edge.ThingEdge;
import com.vaticle.typedb.core.graph.iid.PrefixIID;
import com.vaticle.typedb.core.graph.vertex.AttributeVertex;
import com.vaticle.typedb.core.graph.vertex.ThingVertex;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static com.vaticle.typedb.common.collection.Collections.list;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.UNRECOGNISED_VALUE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.ThingRead.INVALID_ROLE_TYPE_LABEL;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.ThingWrite.INVALID_DELETE_HAS;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.ThingWrite.THING_CANNOT_OWN_ATTRIBUTE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.ThingWrite.THING_HAS_BEEN_DELETED;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.ThingWrite.THING_KEY_MISSING;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.ThingWrite.THING_KEY_OVER;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.ThingWrite.THING_KEY_TAKEN;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.common.iterator.Iterators.link;
import static com.vaticle.typedb.core.common.iterator.Iterators.single;
import static com.vaticle.typedb.core.graph.common.Encoding.Edge.Thing.HAS;
import static com.vaticle.typedb.core.graph.common.Encoding.Edge.Thing.PLAYING;
import static com.vaticle.typedb.core.graph.common.Encoding.Edge.Thing.ROLEPLAYER;

public abstract class ThingImpl extends ConceptImpl implements Thing {

    private ThingVertex vertex;

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
                throw vertex.graphs().exception(TypeDBException.of(UNRECOGNISED_VALUE));
        }
    }

    protected ThingVertex vertex() {
        return vertex;
    }

    protected ThingVertex.Write vertexWritable() {
        if (!vertex.isWrite()) vertex = vertex.writable();
        return vertex.asWrite();
    }

    @Override
    public ByteArray getIID() {
        return vertex().iid().bytes();
    }

    @Override
    public String getIIDForPrinting() {
        return vertex().iid().bytes().toHexString();
    }

    @Override
    public boolean isDeleted() {
        return vertexWritable().isDeleted();
    }

    @Override
    public boolean isInferred() {
        return vertexWritable().isInferred();
    }

    @Override
    public void setHas(Attribute attribute) {
        setHas(attribute, false);
    }

    @Override
    public void setHas(Attribute attribute, boolean isInferred) {
        validateIsNotDeleted();
        AttributeVertex.Write<?> attrVertex = ((AttributeImpl<?>) attribute).vertexWritable();
        if (getType().getOwns().noneMatch(t -> t.equals(attribute.getType()))) {
            throw exception(TypeDBException.of(THING_CANNOT_OWN_ATTRIBUTE, attribute.getType().getLabel(), vertex().type().label()));
        } else if (getType().getOwns(true).anyMatch(t -> t.equals(attribute.getType()))) {
            if (getHas(attribute.getType()).first().isPresent()) {
                throw exception(TypeDBException.of(THING_KEY_OVER, attribute.getType().getLabel(), getType().getLabel()));
            } else if (attribute.getOwners(getType()).first().isPresent()) {
                throw exception(TypeDBException.of(THING_KEY_TAKEN, attribute.getType().getLabel(), getType().getLabel()));
            }
            vertex.graph().exclusiveOwnership(((ThingTypeImpl) this.getType()).vertex(), attrVertex);
        }
        vertexWritable().outs().put(HAS, attrVertex, isInferred);
    }

    @Override
    public void unsetHas(Attribute attribute) {
        validateIsNotDeleted();
        ThingEdge hasEdge = vertex.outs().edge(HAS, ((AttributeImpl<?>) attribute).vertexWritable());
        if (hasEdge == null) throw exception(TypeDBException.of(INVALID_DELETE_HAS, this, attribute));
        hasEdge.delete();
    }

    @Override
    public FunctionalIterator<AttributeImpl<?>> getHas(boolean onlyKey) {
        return getHas(getType().getOwns(onlyKey).stream().toArray(AttributeType[]::new));
    }

    @Override
    public FunctionalIterator<AttributeImpl.Boolean> getHas(AttributeType.Boolean attributeType) {
        return getAttributeVertices(list(attributeType)).map(v -> AttributeImpl.of(v).asBoolean());
    }

    @Override
    public FunctionalIterator<AttributeImpl.Long> getHas(AttributeType.Long attributeType) {
        return getAttributeVertices(list(attributeType)).map(v -> AttributeImpl.of(v).asLong());
    }

    @Override
    public FunctionalIterator<AttributeImpl.Double> getHas(AttributeType.Double attributeType) {
        return getAttributeVertices(list(attributeType)).map(v -> AttributeImpl.of(v).asDouble());
    }

    @Override
    public FunctionalIterator<AttributeImpl.String> getHas(AttributeType.String attributeType) {
        return getAttributeVertices(list(attributeType)).map(v -> AttributeImpl.of(v).asString());
    }

    @Override
    public FunctionalIterator<AttributeImpl.DateTime> getHas(AttributeType.DateTime attributeType) {
        return getAttributeVertices(list(attributeType)).map(v -> AttributeImpl.of(v).asDateTime());
    }

    @Override
    public FunctionalIterator<AttributeImpl<?>> getHas(AttributeType... attributeTypes) {
        if (attributeTypes.length == 0) {
            return getAttributeVertices(getType().getOwns().toList()).map(AttributeImpl::of);
        }
        return getAttributeVertices(Arrays.asList(attributeTypes)).map(AttributeImpl::of);
    }

    private FunctionalIterator<? extends AttributeVertex<?>> getAttributeVertices(List<? extends AttributeType> attributeTypes) {
        if (!attributeTypes.isEmpty()) {
            return iterate(attributeTypes)
                    .flatMap(AttributeType::getSubtypes).distinct()
                    .map(t -> ((TypeImpl) t).vertex())
                    .flatMap(type -> vertex().outs().edge(
                            HAS, PrefixIID.of(type.encoding().instance()), type.iid()
                    ).to()).map(ThingVertex::asAttribute);
        } else {
            return vertex().outs().edge(HAS).to().map(ThingVertex::asAttribute);
        }
    }

    @Override
    public boolean hasInferred(Attribute attribute) {
        ThingEdge hasEdge = vertex().outs().edge(HAS, ((ThingImpl) attribute).vertex());
        return hasEdge != null && hasEdge.isInferred();
    }

    @Override
    public FunctionalIterator<? extends RoleType> getPlaying() {
        return vertex().outs().edge(PLAYING).to().map(ThingVertex::type)
                .map(v -> RoleTypeImpl.of(vertex().graphs(), v));
    }

    @Override
    public FunctionalIterator<RelationImpl> getRelations(String roleType, String... roleTypes) {
        return getRelations(link(single(roleType), iterate(roleTypes)).map(scopedLabel -> {
            if (!scopedLabel.contains(":")) {
                throw exception(TypeDBException.of(INVALID_ROLE_TYPE_LABEL, scopedLabel));
            }
            String[] label = scopedLabel.split(":");
            return RoleTypeImpl.of(vertex().graphs(), vertex().graph().type().getType(label[1], label[0]));
        }).stream().toArray(RoleType[]::new));
    }

    @Override
    public FunctionalIterator<RelationImpl> getRelations(RoleType... roleTypes) {
        if (roleTypes.length == 0) {
            return vertex().ins().edge(ROLEPLAYER).from().map(RelationImpl::of);
        } else {
            return iterate(roleTypes).flatMap(RoleType::getSubtypes).distinct().flatMap(
                    rt -> vertex().ins().edge(ROLEPLAYER, ((RoleTypeImpl) rt).vertex().iid()).from()
            ).map(RelationImpl::of);
        }
    }

    @Override
    public void delete() {
        Set<RelationImpl> relations = vertexWritable().ins().edge(ROLEPLAYER).from().map(RelationImpl::of).toSet();
        vertexWritable().outs().edge(PLAYING).to().map(v -> RoleImpl.of(v.writable())).forEachRemaining(RoleImpl::delete);
        vertexWritable().delete();
        relations.forEach(RelationImpl::deleteIfNoPlayer);
    }

    @Override
    public void validate() {
        long requiredKeys = getType().getOwns(true).count();
        if (requiredKeys > 0 && getHas(true).map(Attribute::getType).count() < requiredKeys) {
            Set<AttributeType> missing = getType().getOwns(true).map(Concept::asAttributeType).toSet();
            missing.removeAll(getHas(true).map(Attribute::getType).toSet());
            throw exception(TypeDBException.of(THING_KEY_MISSING, getType().getLabel(), printTypeSet(missing)));
        }
    }

    void validateIsNotDeleted() {
        if (vertexWritable().isDeleted()) throw exception(TypeDBException.of(THING_HAS_BEEN_DELETED, getIIDForPrinting()));
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
    public TypeDBException exception(TypeDBException exception) {
        return vertex().graphs().exception(exception);
    }

    @Override
    public String toString() {
        return vertex().encoding().name() + ":" + vertex().type().properLabel() + ":" + getIIDForPrinting();
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        ThingImpl that = (ThingImpl) object;
        return this.vertex().equals(that.vertex());
    }

    @Override
    public final int hashCode() {
        return vertex().hashCode(); // does not need caching
    }
}
