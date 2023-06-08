/*
 * Copyright (C) 2022 Vaticle
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
import com.vaticle.typedb.core.common.parameters.Concept.Existence;
import com.vaticle.typedb.core.concept.ConceptImpl;
import com.vaticle.typedb.core.concept.ConceptManager;
import com.vaticle.typedb.core.concept.thing.Attribute;
import com.vaticle.typedb.core.concept.thing.Thing;
import com.vaticle.typedb.core.concept.type.AttributeType;
import com.vaticle.typedb.core.concept.type.RoleType;
import com.vaticle.typedb.core.concept.type.ThingType;
import com.vaticle.typedb.core.concept.type.Type;
import com.vaticle.typedb.core.concept.type.impl.RoleTypeImpl;
import com.vaticle.typedb.core.concept.type.impl.TypeImpl;
import com.vaticle.typedb.core.encoding.iid.PrefixIID;
import com.vaticle.typedb.core.graph.edge.ThingEdge;
import com.vaticle.typedb.core.graph.vertex.AttributeVertex;
import com.vaticle.typedb.core.graph.vertex.ThingVertex;
import com.vaticle.typeql.lang.common.TypeQLToken.Annotation;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Set;

import static com.vaticle.typedb.common.collection.Collections.list;
import static com.vaticle.typedb.common.collection.Collections.set;
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
import static com.vaticle.typedb.core.common.parameters.Concept.Existence.INFERRED;
import static com.vaticle.typedb.core.common.parameters.Concept.Existence.STORED;
import static com.vaticle.typedb.core.encoding.Encoding.Edge.Thing.Base.HAS;
import static com.vaticle.typedb.core.encoding.Encoding.Edge.Thing.Base.PLAYING;
import static com.vaticle.typedb.core.encoding.Encoding.Edge.Thing.Optimised.ROLEPLAYER;
import static com.vaticle.typeql.lang.common.TypeQLToken.Annotation.KEY;
import static com.vaticle.typeql.lang.common.TypeQLToken.Annotation.UNIQUE;

public abstract class ThingImpl extends ConceptImpl implements Thing {

    private ThingVertex vertex;

    ThingImpl(ConceptManager conceptMgr, ThingVertex vertex) {
        super(conceptMgr);
        this.vertex = Objects.requireNonNull(vertex);
    }

    public static ThingImpl of(ConceptManager conceptMgr, ThingVertex vertex) {
        switch (vertex.encoding()) {
            case ENTITY:
                return EntityImpl.of(conceptMgr, vertex);
            case ATTRIBUTE:
                return AttributeImpl.of(conceptMgr, vertex.asAttribute());
            case RELATION:
                return RelationImpl.of(conceptMgr, vertex);
            default:
                throw vertex.graphs().exception(TypeDBException.of(UNRECOGNISED_VALUE));
        }
    }

    public ThingVertex readableVertex() {
        return vertex;
    }

    protected ThingVertex.Write writableVertex() {
        if (!vertex.isWrite()) vertex = vertex.toWrite();
        return vertex.asWrite();
    }

    @Override
    public ByteArray getIID() {
        return readableVertex().iid().bytes();
    }

    @Override
    public String getIIDForPrinting() {
        return readableVertex().iid().bytes().toHexString();
    }

    @Override
    public boolean isDeleted() {
        return writableVertex().isDeleted();
    }

    @Override
    public Existence existence() {
        return readableVertex().existence();
    }

    @Override
    public void setHas(Attribute attribute) {
        setHas(attribute, STORED);
    }

    @Override
    public void setHas(Attribute attribute, Existence existence) {
        validateIsNotDeleted();
        AttributeVertex.Write<?> attrVertex = ((AttributeImpl<?>) attribute).writableVertex();
        NavigableSet<ThingType.Owns> owns = getType().getOwns();
        if (iterate(owns).noneMatch(o -> o.attributeType().equals(attribute.getType()))) {
            throw exception(TypeDBException.of(THING_CANNOT_OWN_ATTRIBUTE, attribute.getType().getLabel(), readableVertex().type().label()));
        } else if (iterate(owns).anyMatch(o -> o.effectiveAnnotations().contains(KEY) && o.attributeType().equals(attribute.getType()))) {
            if (getHas(attribute.getType()).first().isPresent()) {
                throw exception(TypeDBException.of(THING_KEY_OVER, attribute.getType().getLabel(), getType().getLabel()));
            } else {
                if (attribute.getOwners(getType()).anyMatch(owner -> owner.getType().equals(getType()))) {
                    throw exception(TypeDBException.of(THING_KEY_TAKEN, ((AttributeImpl<?>) attribute).getValue(),
                            attribute.getType().getLabel(), getType().getLabel()));
                }
            }
            vertex.graph().exclusiveOwnership(((TypeImpl) this.getType()).vertex, attrVertex);
        } else if (iterate(owns).anyMatch(o -> o.effectiveAnnotations().contains(UNIQUE) && o.attributeType().equals(attribute.getType()))) {
            if (attribute.getOwners(getType()).anyMatch(owner -> owner.getType().equals(getType()))) {
                throw exception(TypeDBException.of(THING_KEY_TAKEN, ((AttributeImpl<?>) attribute).getValue(),
                        attribute.getType().getLabel(), getType().getLabel()));
            }
            vertex.graph().exclusiveOwnership(((TypeImpl) this.getType()).vertex, attrVertex);
        }
        writableVertex().outs().put(HAS, attrVertex, existence);
    }

    @Override
    public void unsetHas(Attribute attribute) {
        validateIsNotDeleted();
        ThingEdge hasEdge = vertex.outs().edge(HAS, ((AttributeImpl<?>) attribute).writableVertex());
        if (hasEdge == null) throw exception(TypeDBException.of(INVALID_DELETE_HAS, this, attribute));
        hasEdge.delete();
    }

    @Override
    public FunctionalIterator<AttributeImpl<?>> getHas(List<AttributeType> attributeTypes, Set<Annotation> ownsAnnotations) {
        return getHas(getType().getOwns(ownsAnnotations).stream().map(ThingType.Owns::attributeType)
                .filter(attributeTypes::contains).toArray(AttributeType[]::new));
    }

    @Override
    public FunctionalIterator<AttributeImpl<?>> getHas(Set<Annotation> ownsAnnotations) {
        return getHas(getType().getOwns(ownsAnnotations).stream().map(ThingType.Owns::attributeType).toArray(AttributeType[]::new));
    }

    @Override
    public FunctionalIterator<AttributeImpl.Boolean> getHas(AttributeType.Boolean attributeType) {
        return getAttributeVertices(list(attributeType)).map(v -> AttributeImpl.of(conceptMgr, v).asBoolean());
    }

    @Override
    public FunctionalIterator<AttributeImpl.Long> getHas(AttributeType.Long attributeType) {
        return getAttributeVertices(list(attributeType)).map(v -> AttributeImpl.of(conceptMgr, v).asLong());
    }

    @Override
    public FunctionalIterator<AttributeImpl.Double> getHas(AttributeType.Double attributeType) {
        return getAttributeVertices(list(attributeType)).map(v -> AttributeImpl.of(conceptMgr, v).asDouble());
    }

    @Override
    public FunctionalIterator<AttributeImpl.String> getHas(AttributeType.String attributeType) {
        return getAttributeVertices(list(attributeType)).map(v -> AttributeImpl.of(conceptMgr, v).asString());
    }

    @Override
    public FunctionalIterator<AttributeImpl.DateTime> getHas(AttributeType.DateTime attributeType) {
        return getAttributeVertices(list(attributeType)).map(v -> AttributeImpl.of(conceptMgr, v).asDateTime());
    }

    @Override
    public FunctionalIterator<AttributeImpl<?>> getHas(AttributeType... attributeTypes) {
        if (attributeTypes.length == 0) {
            return getAttributeVertices(Collections.emptyList()).map(v -> AttributeImpl.of(conceptMgr, v));
        } else {
            return getAttributeVertices(Arrays.asList(attributeTypes)).map(v -> AttributeImpl.of(conceptMgr, v));
        }
    }

    private FunctionalIterator<? extends AttributeVertex<?>> getAttributeVertices(List<? extends
            AttributeType> attributeTypes) {
        if (!attributeTypes.isEmpty()) {
            return iterate(attributeTypes)
                    .flatMap(AttributeType::getSubtypes).distinct()
                    .map(t -> ((TypeImpl) t).vertex)
                    .flatMap(type -> readableVertex().outs().edge(HAS, PrefixIID.of(type.encoding().instance()), type.iid()).to())
                    .map(ThingVertex::asAttribute);
        } else {
            return readableVertex().outs().edge(HAS).to().map(ThingVertex::asAttribute);
        }
    }

    @Override
    public boolean hasInferred(Attribute attribute) {
        ThingEdge hasEdge = readableVertex().outs().edge(HAS, ((ThingImpl) attribute).readableVertex());
        return hasEdge != null && hasEdge.existence() == INFERRED;
    }

    @Override
    public boolean hasNonInferred(Attribute attribute) {
        ThingEdge hasEdge = readableVertex().outs().edge(HAS, ((ThingImpl) attribute).readableVertex());
        return hasEdge != null && hasEdge.existence() == STORED;
    }

    @Override
    public FunctionalIterator<RoleType> getPlaying() {
        return readableVertex().outs().edge(PLAYING).to().map(ThingVertex::type)
                .map(conceptMgr::convertRoleType);
    }

    @Override
    public FunctionalIterator<RelationImpl> getRelations(String roleType, String... roleTypes) {
        return getRelations(link(single(roleType), iterate(roleTypes)).map(scopedLabel -> {
            if (!scopedLabel.contains(":")) {
                throw exception(TypeDBException.of(INVALID_ROLE_TYPE_LABEL, scopedLabel));
            }
            String[] label = scopedLabel.split(":");
            return conceptMgr.convertRoleType(readableVertex().graph().type().getType(label[1], label[0]));
        }).stream().toArray(RoleType[]::new));
    }

    @Override
    public FunctionalIterator<RelationImpl> getRelations(RoleType... roleTypes) {
        if (roleTypes.length == 0) {
            return readableVertex().ins().edge(ROLEPLAYER).from().map(v -> RelationImpl.of(conceptMgr, v));
        } else {
            return iterate(roleTypes).flatMap(RoleType::getSubtypes).distinct().flatMap(
                    rt -> readableVertex().ins().edge(ROLEPLAYER, ((RoleTypeImpl) rt).vertex).from()
            ).map(v -> RelationImpl.of(conceptMgr, v));
        }
    }

    @Override
    public void delete() {
        Set<RelationImpl> relations = writableVertex().ins().edge(ROLEPLAYER).from().map(v -> RelationImpl.of(conceptMgr, v)).toSet();
        writableVertex().outs().edge(PLAYING).to().map(RoleImpl::of).forEachRemaining(RoleImpl::delete);
        writableVertex().delete();
        relations.forEach(RelationImpl::deleteIfNoPlayer);
    }

    @Override
    public void validate() {
        Set<Annotation> keyAnnotation = set(KEY);
        long requiredKeys = getType().getOwns(keyAnnotation).count();
        if (requiredKeys > 0 && getHas(keyAnnotation).map(Attribute::getType).count() < requiredKeys) {
            Set<AttributeType> missing = getType().getOwns(keyAnnotation).map(ThingType.Owns::attributeType).toSet();
            missing.removeAll(getHas(keyAnnotation).map(Attribute::getType).toSet());
            throw exception(TypeDBException.of(THING_KEY_MISSING, getType().getLabel(), printTypeSet(missing)));
        }
    }

    void validateIsNotDeleted() {
        if (writableVertex().isDeleted())
            throw exception(TypeDBException.of(THING_HAS_BEEN_DELETED, getIIDForPrinting(), getType().getLabel()));
    }

    @Override
    public ThingImpl asThing() {
        return this;
    }

    @Override
    public boolean isThing() {
        return true;
    }

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
        return readableVertex().graphs().exception(exception);
    }

    @Override
    public String toString() {
        return readableVertex().encoding().name() + ":" + readableVertex().type().properLabel() + ":" + getIIDForPrinting();
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        ThingImpl that = (ThingImpl) object;
        return this.readableVertex().equals(that.readableVertex());
    }

    @Override
    public final int hashCode() {
        return readableVertex().hashCode(); // does not need caching
    }

    @Override
    public int compareTo(Thing other) {
        return vertex.compareTo(((ThingImpl) other).vertex);
    }
}
