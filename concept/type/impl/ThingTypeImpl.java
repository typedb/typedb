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

package com.vaticle.typedb.core.concept.type.impl;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.iterator.Iterators;
import com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.Order;
import com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.Seekable;
import com.vaticle.typedb.core.concept.thing.Attribute;
import com.vaticle.typedb.core.concept.thing.impl.AttributeImpl;
import com.vaticle.typedb.core.concept.thing.impl.EntityImpl;
import com.vaticle.typedb.core.concept.thing.impl.RelationImpl;
import com.vaticle.typedb.core.concept.thing.impl.ThingImpl;
import com.vaticle.typedb.core.concept.type.AttributeType;
import com.vaticle.typedb.core.concept.type.RoleType;
import com.vaticle.typedb.core.concept.type.ThingType;
import com.vaticle.typedb.core.concept.type.Type;
import com.vaticle.typedb.core.graph.GraphManager;
import com.vaticle.typedb.core.graph.common.Encoding;
import com.vaticle.typedb.core.graph.edge.TypeEdge;
import com.vaticle.typedb.core.graph.vertex.ThingVertex;
import com.vaticle.typedb.core.graph.vertex.TypeVertex;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.UNRECOGNISED_VALUE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.INVALID_UNDEFINE_INHERITED_OWNS;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.INVALID_UNDEFINE_INHERITED_PLAYS;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.INVALID_UNDEFINE_NONEXISTENT_OWNS;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.INVALID_UNDEFINE_NONEXISTENT_PLAYS;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.INVALID_UNDEFINE_OWNS_HAS_INSTANCES;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.INVALID_UNDEFINE_PLAYS_HAS_INSTANCES;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.OVERRIDDEN_NOT_SUPERTYPE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.OVERRIDE_NOT_AVAILABLE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.OWNS_ABSTRACT_ATT_TYPE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.OWNS_ATT_NOT_AVAILABLE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.OWNS_KEY_NOT_AVAILABLE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.OWNS_KEY_PRECONDITION_NO_INSTANCES;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.OWNS_KEY_PRECONDITION_UNIQUENESS;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.OWNS_KEY_VALUE_TYPE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.OWS_KEY_PRECONDITION_OWNERSHIP_KEY_MISSING;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.OWS_KEY_PRECONDITION_OWNERSHIP_KEY_TOO_MANY;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.PLAYS_ABSTRACT_ROLE_TYPE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.PLAYS_ROLE_NOT_AVAILABLE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.ROOT_TYPE_MUTATION;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.TYPE_HAS_INSTANCES_DELETE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.TYPE_HAS_INSTANCES_SET_ABSTRACT;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.TYPE_HAS_SUBTYPES;
import static com.vaticle.typedb.core.common.iterator.Iterators.Sorted.Seekable.emptySorted;
import static com.vaticle.typedb.core.common.iterator.Iterators.Sorted.Seekable.iterateSorted;
import static com.vaticle.typedb.core.common.iterator.Iterators.compareSize;
import static com.vaticle.typedb.core.common.iterator.Iterators.link;
import static com.vaticle.typedb.core.common.iterator.Iterators.loop;
import static com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.ASC;
import static com.vaticle.typedb.core.graph.common.Encoding.Edge.Type.OWNS;
import static com.vaticle.typedb.core.graph.common.Encoding.Edge.Type.OWNS_KEY;
import static com.vaticle.typedb.core.graph.common.Encoding.Edge.Type.PLAYS;
import static com.vaticle.typedb.core.graph.common.Encoding.Edge.Type.SUB;

public abstract class ThingTypeImpl extends TypeImpl implements ThingType {

    ThingTypeImpl(GraphManager graphMgr, TypeVertex vertex) {
        super(graphMgr, vertex);
    }

    ThingTypeImpl(GraphManager graphMgr, String label, Encoding.Vertex.Type encoding) {
        super(graphMgr, label, encoding);
    }

    public static ThingTypeImpl of(GraphManager graphMgr, TypeVertex vertex) {
        switch (vertex.encoding()) {
            case ENTITY_TYPE:
                return EntityTypeImpl.of(graphMgr, vertex);
            case ATTRIBUTE_TYPE:
                return AttributeTypeImpl.of(graphMgr, vertex);
            case RELATION_TYPE:
                return RelationTypeImpl.of(graphMgr, vertex);
            case THING_TYPE:
                return new ThingTypeImpl.Root(graphMgr, vertex);
            default:
                throw graphMgr.exception(TypeDBException.of(UNRECOGNISED_VALUE));
        }
    }

    @Override
    public void setAbstract() {
        if (isAbstract()) return;
        validateIsNotDeleted();
        if (getInstancesExplicit().first().isPresent()) {
            throw exception(TypeDBException.of(TYPE_HAS_INSTANCES_SET_ABSTRACT, getLabel()));
        }
        vertex.isAbstract(true);
    }

    @Override
    public void unsetAbstract() {
        if (isAbstract()) {
            validateIsNotDeleted();
            vertex.isAbstract(false);
        }
    }

    @Nullable
    @Override
    public ThingTypeImpl getSupertype() {
        return vertex.outs().edge(SUB).to().map(t -> ThingTypeImpl.of(graphMgr, t)).firstOrNull();
    }

    @Override
    public FunctionalIterator<ThingTypeImpl> getSupertypes() {
        return loop(vertex, Objects::nonNull, v -> v.outs().edge(SUB).to().firstOrNull())
                .map(v -> ThingTypeImpl.of(graphMgr, v));
    }

    @Override
    public abstract Seekable<? extends ThingTypeImpl, Order.Asc> getSubtypes();

    @Override
    public abstract Seekable<? extends ThingTypeImpl, Order.Asc> getSubtypesExplicit();

    <THING extends ThingImpl> Seekable<THING, Order.Asc> instances(Function<ThingVertex, THING> thingConstructor) {
        return getSubtypes().filter(t -> !t.isAbstract())
                .mergeMap(t -> graphMgr.data().getReadable(t.vertex), ASC)
                .mapSorted(thingConstructor, ThingImpl::readableVertex, ASC);
    }

    <THING extends ThingImpl> Seekable<THING, Order.Asc> instancesExplicit(Function<ThingVertex, THING> thingConstructor) {
        return graphMgr.data().getReadable(vertex).mapSorted(thingConstructor, ThingImpl::readableVertex, ASC);
    }

    @Override
    public void setOwns(AttributeType attributeType) {
        validateIsNotDeleted();
        setOwns(attributeType, false);
    }

    @Override
    public void setOwns(AttributeType attributeType, boolean isKey) {
        validateIsNotDeleted();
        if (isKey) ownsKey((AttributeTypeImpl) attributeType);
        else ownsAttribute((AttributeTypeImpl) attributeType);
    }

    @Override
    public void setOwns(AttributeType attributeType, AttributeType overriddenType) {
        setOwns(attributeType, overriddenType, false);
    }

    @Override
    public void setOwns(AttributeType attributeType, AttributeType overriddenType, boolean isKey) {
        validateIsNotDeleted();
        if (isKey) ownsKey((AttributeTypeImpl) attributeType, (AttributeTypeImpl) overriddenType);
        else ownsAttribute((AttributeTypeImpl) attributeType, (AttributeTypeImpl) overriddenType);
    }

    @Override
    public void unsetOwns(AttributeType attributeType) {
        validateIsNotDeleted();
        TypeEdge edge;
        TypeVertex attVertex = ((AttributeTypeImpl) attributeType).vertex;
        if (getInstances().anyMatch(thing -> thing.getHas(attributeType).first().isPresent())) {
            throw exception(TypeDBException.of(INVALID_UNDEFINE_OWNS_HAS_INSTANCES, vertex.label(), attVertex.label()));
        }
        if ((edge = vertex.outs().edge(OWNS_KEY, attVertex)) != null) edge.delete();
        else if ((edge = vertex.outs().edge(OWNS, attVertex)) != null) edge.delete();
        else if (this.getOwns().findFirst((AttributeTypeImpl) attributeType).isPresent()) {
            throw exception(TypeDBException.of(INVALID_UNDEFINE_INHERITED_OWNS,
                    this.getLabel().toString(), attributeType.getLabel().toString()));
        } else {
            throw exception(TypeDBException.of(INVALID_UNDEFINE_NONEXISTENT_OWNS,
                    this.getLabel().toString(), attributeType.getLabel().toString()));
        }
    }

    private <T extends com.vaticle.typedb.core.concept.type.Type> void override(Encoding.Edge.Type encoding, T type, T overriddenType,
                                                                                Seekable<? extends Type, Order.Asc> overridable,
                                                                                Seekable<? extends Type, Order.Asc> notOverridable) {
        if (type.getSupertypes().noneMatch(t -> t.equals(overriddenType))) {
            throw exception(TypeDBException.of(OVERRIDDEN_NOT_SUPERTYPE, type.getLabel(), overriddenType.getLabel()));
        } else if (notOverridable.anyMatch(t -> t.equals(overriddenType)) || overridable.noneMatch(t -> t.equals(overriddenType))) {
            throw exception(TypeDBException.of(OVERRIDE_NOT_AVAILABLE, type.getLabel(), overriddenType.getLabel()));
        }

        vertex.outs().edge(encoding, ((TypeImpl) type).vertex).overridden(((TypeImpl) overriddenType).vertex);
    }

    private void ownsKey(AttributeTypeImpl attributeType) {
        validateIsNotDeleted();

        TypeVertex attVertex = attributeType.vertex;
        TypeEdge ownsEdge, ownsKeyEdge;

        if (vertex.outs().edge(OWNS_KEY, attVertex) != null) return;

        if (!attributeType.isKeyable()) {
            throw exception(TypeDBException.of(OWNS_KEY_VALUE_TYPE, attributeType.getLabel(), attributeType.getValueType().name()));
        } else if (link(getSupertype().getOwns(attributeType.getValueType(), true),
                getSupertype().overriddenOwns(false, true)).anyMatch(a -> a.equals(attributeType))) {
            throw exception(TypeDBException.of(OWNS_KEY_NOT_AVAILABLE, attributeType.getLabel()));
        }

        if ((ownsEdge = vertex.outs().edge(OWNS, attVertex)) != null) {
            // TODO: These ownership and uniqueness checks should be parallelised to scale better
            getInstances().forEachRemaining(thing -> {
                FunctionalIterator<? extends Attribute> attrs = thing.getHas(attributeType);
                if (!attrs.hasNext())
                    throw exception(TypeDBException.of(OWS_KEY_PRECONDITION_OWNERSHIP_KEY_TOO_MANY, vertex.label(), attVertex.label()));
                Attribute attr = attrs.next();
                if (attrs.hasNext())
                    throw exception(TypeDBException.of(OWS_KEY_PRECONDITION_OWNERSHIP_KEY_MISSING, vertex.label(), attVertex.label()));
                else if (compareSize(attr.getOwners(this), 1) != 0) {
                    throw exception(TypeDBException.of(OWNS_KEY_PRECONDITION_UNIQUENESS, attVertex.label(), vertex.label()));
                }
            });
            ownsEdge.delete();
        } else if (getInstances().first().isPresent()) {
            throw exception(TypeDBException.of(OWNS_KEY_PRECONDITION_NO_INSTANCES, vertex.label(), attVertex.label()));
        }
        ownsKeyEdge = vertex.outs().put(OWNS_KEY, attVertex);
        if (getSupertype().declaredOwns(false).findFirst(attributeType).isPresent()) ownsKeyEdge.overridden(attVertex);
    }

    private void ownsKey(AttributeTypeImpl attributeType, AttributeTypeImpl overriddenType) {
        ownsKey(attributeType);
        override(OWNS_KEY, attributeType, overriddenType,
                getSupertype().getOwns(attributeType.getValueType()),
                declaredOwns(false));
    }

    private void ownsAttribute(AttributeTypeImpl attributeType) {
        validateIsNotDeleted();
        Seekable<AttributeType, Order.Asc> owns = getSupertypes().filter(t -> !t.equals(this)).mergeMap(ThingType::getOwns, ASC);
        if (owns.findFirst(attributeType).isPresent()) {
            throw exception(TypeDBException.of(OWNS_ATT_NOT_AVAILABLE, attributeType.getLabel()));
        }

        TypeVertex attVertex = attributeType.vertex;
        TypeEdge keyEdge;
        if ((keyEdge = vertex.outs().edge(OWNS_KEY, attVertex)) != null) keyEdge.delete();
        vertex.outs().put(OWNS, attVertex);
    }

    private void ownsAttribute(AttributeTypeImpl attributeType, AttributeTypeImpl overriddenType) {
        this.ownsAttribute(attributeType);
        override(OWNS, attributeType, overriddenType,
                getSupertype().getOwns(attributeType.getValueType()),
                getSupertype().getOwns(true).merge(declaredOwns(false)));
    }

    private Seekable<AttributeType, Order.Asc> declaredOwns(boolean onlyKey) {
        if (isRoot()) return emptySorted();
        Seekable<TypeVertex, Order.Asc> iterator;
        if (onlyKey) iterator = vertex.outs().edge(OWNS_KEY).to();
        else iterator = vertex.outs().edge(OWNS_KEY).to().merge(vertex.outs().edge(OWNS).to());
        return iterator.mapSorted(v -> AttributeTypeImpl.of(graphMgr, v), attr -> ((AttributeTypeImpl) attr).vertex, ASC);
    }

    FunctionalIterator<AttributeTypeImpl> overriddenOwns(boolean onlyKey, boolean transitive) {
        if (isRoot()) return Iterators.empty();
        FunctionalIterator<AttributeTypeImpl> overriddenOwns;
        if (onlyKey) {
            overriddenOwns = vertex.outs().edge(OWNS_KEY).overridden().filter(Objects::nonNull)
                    .map(v -> AttributeTypeImpl.of(graphMgr, v));
        } else {
            overriddenOwns = link(
                    vertex.outs().edge(OWNS_KEY).overridden(),
                    vertex.outs().edge(OWNS).overridden()
            ).filter(Objects::nonNull).distinct().map(v -> AttributeTypeImpl.of(graphMgr, v));
        }

        if (transitive) return link(overriddenOwns, getSupertype().overriddenOwns(onlyKey, true));
        else return overriddenOwns;
    }

    @Override
    public Seekable<AttributeType, Order.Asc> getOwns() {
        return getOwns(false);
    }

    @Override
    public Seekable<AttributeType, Order.Asc> getOwnsExplicit() {
        return getOwnsExplicit(false);
    }

    @Override
    public Seekable<AttributeType, Order.Asc> getOwns(AttributeType.ValueType valueType) {
        return getOwns(valueType, false);
    }

    @Override
    public Seekable<AttributeType, Order.Asc> getOwnsExplicit(AttributeType.ValueType valueType) {
        return getOwnsExplicit(valueType, false);
    }

    @Override
    public Seekable<AttributeType, Order.Asc> getOwns(boolean onlyKey) {
        if (onlyKey) {
            return iterateSorted(graphMgr.schema().ownedKeyAttributeTypes(vertex), ASC)
                    .mapSorted(v -> AttributeTypeImpl.of(graphMgr, v), attr -> ((AttributeTypeImpl) attr).vertex, ASC);
        } else {
            return iterateSorted(graphMgr.schema().ownedAttributeTypes(vertex), ASC)
                    .mapSorted(v -> AttributeTypeImpl.of(graphMgr, v), attr -> ((AttributeTypeImpl) attr).vertex, ASC);
        }
    }

    @Override
    public Seekable<AttributeType, Order.Asc> getOwnsExplicit(boolean onlyKey) {
        if (isRoot()) return emptySorted();
        return declaredOwns(onlyKey);
    }

    @Override
    public Seekable<AttributeType, Order.Asc> getOwns(AttributeType.ValueType valueType, boolean onlyKey) {
        return getOwns(onlyKey).filter(att -> att.getValueType().equals(valueType));
    }

    @Override
    public Seekable<AttributeType, Order.Asc> getOwnsExplicit(AttributeType.ValueType valueType, boolean onlyKey) {
        return getOwnsExplicit(onlyKey).filter(att -> att.getValueType().equals(valueType));
    }

    @Override
    public AttributeType getOwnsOverridden(AttributeType attributeType) {
        TypeVertex attrVertex = graphMgr.schema().getType(attributeType.getLabel());
        if (attrVertex != null) {
            TypeEdge ownsEdge = vertex.outs().edge(OWNS_KEY, attrVertex);
            if (ownsEdge != null && ownsEdge.overridden() != null)
                return AttributeTypeImpl.of(graphMgr, ownsEdge.overridden());
            ownsEdge = vertex.outs().edge(OWNS, attrVertex);
            if (ownsEdge != null && ownsEdge.overridden() != null)
                return AttributeTypeImpl.of(graphMgr, ownsEdge.overridden());
        }
        return null;
    }

    @Override
    public void setPlays(RoleType roleType) {
        validateIsNotDeleted();
        if (getSupertypes().filter(t -> !t.equals(this)).mergeMap(ThingType::getPlays, ASC).findFirst(roleType).isPresent()) {
            throw exception(TypeDBException.of(PLAYS_ROLE_NOT_AVAILABLE, roleType.getLabel()));
        }
        vertex.outs().put(Encoding.Edge.Type.PLAYS, ((RoleTypeImpl) roleType).vertex);
    }

    @Override
    public void setPlays(RoleType roleType, RoleType overriddenType) {
        validateIsNotDeleted();
        setPlays(roleType);
        override(Encoding.Edge.Type.PLAYS, roleType, overriddenType, getSupertype().getPlays(),
                vertex.outs().edge(Encoding.Edge.Type.PLAYS).to().mapSorted(v -> RoleTypeImpl.of(graphMgr, v), rt -> rt.vertex, ASC));
    }

    @Override
    public void unsetPlays(RoleType roleType) {
        validateIsNotDeleted();
        TypeEdge edge = vertex.outs().edge(Encoding.Edge.Type.PLAYS, ((RoleTypeImpl) roleType).vertex);
        if (edge == null) {
            if (this.getPlays().findFirst(roleType).isPresent()) {
                throw exception(TypeDBException.of(INVALID_UNDEFINE_INHERITED_PLAYS,
                        this.getLabel().toString(), roleType.getLabel().toString()));
            } else {
                throw exception(TypeDBException.of(INVALID_UNDEFINE_NONEXISTENT_PLAYS,
                        this.getLabel().toString(), roleType.getLabel().toString()));
            }
        }
        if (getInstances().anyMatch(thing -> thing.getRelations(roleType).first().isPresent())) {
            throw exception(TypeDBException.of(INVALID_UNDEFINE_PLAYS_HAS_INSTANCES, vertex.label(), roleType.getLabel().toString()));
        }
        edge.delete();
    }

    @Override
    public Seekable<RoleType, Order.Asc> getPlays() {
        if (isRoot()) return emptySorted();
        assert getSupertype() != null;
        return iterateSorted(graphMgr.schema().playedRoleTypes(vertex), ASC)
                .mapSorted(v -> RoleTypeImpl.of(graphMgr, v), roleType -> ((RoleTypeImpl) roleType).vertex, ASC);
    }

    @Override
    public Seekable<RoleType, Order.Asc> getPlaysExplicit() {
        if (isRoot()) return emptySorted();
        return vertex.outs().edge(Encoding.Edge.Type.PLAYS).to()
                .mapSorted(v -> RoleTypeImpl.of(graphMgr, v), rt -> ((RoleTypeImpl) rt).vertex, ASC);
    }

    @Override
    public RoleType getPlaysOverridden(RoleType roleType) {
        TypeVertex roleVertex = graphMgr.schema().getType(roleType.getLabel());
        if (roleVertex != null) {
            TypeEdge playsEdge = vertex.outs().edge(PLAYS, roleVertex);
            if (playsEdge != null && playsEdge.overridden() != null)
                return RoleTypeImpl.of(graphMgr, playsEdge.overridden());
        }
        return null;
    }

    @Override
    public void delete() {
        validateDelete();
        vertex.delete();
    }

    @Override
    void validateDelete() {
        super.validateDelete();
        if (getSubtypes().anyMatch(s -> !s.equals(this))) {
            throw exception(TypeDBException.of(TYPE_HAS_SUBTYPES, getLabel()));
        } else if (getSubtypes().flatMap(ThingType::getInstances).first().isPresent()) {
            throw exception(TypeDBException.of(TYPE_HAS_INSTANCES_DELETE, getLabel()));
        }
    }

    @Override
    public List<TypeDBException> validate() {
        List<TypeDBException> exceptions = super.validate();
        if (!isAbstract()) {
            exceptions.addAll(exceptions_ownsAbstractAttType());
            exceptions.addAll(exceptions_playsAbstractRoleType());
        }
        return exceptions;
    }

    @Override
    public boolean isThingType() {
        return true;
    }

    @Override
    public ThingTypeImpl asThingType() {
        return this;
    }

    private List<TypeDBException> exceptions_ownsAbstractAttType() {
        return getOwns().filter(Type::isAbstract)
                .map(attType -> TypeDBException.of(OWNS_ABSTRACT_ATT_TYPE, getLabel(), attType.getLabel()))
                .toList();
    }

    private List<TypeDBException> exceptions_playsAbstractRoleType() {
        return getPlays().filter(Type::isAbstract)
                .map(roleType -> TypeDBException.of(PLAYS_ABSTRACT_ROLE_TYPE, getLabel(), roleType.getLabel()))
                .toList();
    }

    public static class Root extends ThingTypeImpl {

        public Root(GraphManager graphMgr, TypeVertex vertex) {
            super(graphMgr, vertex);
            assert vertex.label().equals(Encoding.Vertex.Type.Root.THING.label());
        }

        @Override
        public boolean isRoot() {
            return true;
        }

        @Override
        public void setLabel(String label) {
            throw exception(TypeDBException.of(ROOT_TYPE_MUTATION));
        }

        @Override
        public void unsetAbstract() {
            throw exception(TypeDBException.of(ROOT_TYPE_MUTATION));
        }

        @Override
        public ThingTypeImpl getSupertype() {
            return null;
        }

        @Override
        public FunctionalIterator<ThingTypeImpl> getSupertypes() {
            return Iterators.single(this);
        }

        @Override
        public Seekable<ThingTypeImpl, Order.Asc> getSubtypes() {
            return iterateSorted(graphMgr.schema().getSubtypes(vertex), ASC).mapSorted(v -> {
                switch (v.encoding()) {
                    case THING_TYPE:
                        assert vertex == v;
                        return this;
                    case ENTITY_TYPE:
                        return EntityTypeImpl.of(graphMgr, v);
                    case ATTRIBUTE_TYPE:
                        return AttributeTypeImpl.of(graphMgr, v);
                    case RELATION_TYPE:
                        return RelationTypeImpl.of(graphMgr, v);
                    default:
                        throw exception(TypeDBException.of(UNRECOGNISED_VALUE));
                }
            }, thingType -> thingType.vertex, ASC);
        }

        @Override
        public Seekable<ThingTypeImpl, Order.Asc> getSubtypesExplicit() {
            return getSubtypesExplicit(v -> {
                switch (v.encoding()) {
                    case ENTITY_TYPE:
                        return EntityTypeImpl.of(graphMgr, v);
                    case ATTRIBUTE_TYPE:
                        return AttributeTypeImpl.of(graphMgr, v);
                    case RELATION_TYPE:
                        return RelationTypeImpl.of(graphMgr, v);
                    default:
                        throw exception(TypeDBException.of(UNRECOGNISED_VALUE));
                }
            });
        }

        @Override
        public Seekable<ThingImpl, Order.Asc> getInstances() {
            return instances(v -> {
                switch (v.encoding()) {
                    case ENTITY:
                        return EntityImpl.of(v);
                    case ATTRIBUTE:
                        return AttributeImpl.of(v);
                    case RELATION:
                        return RelationImpl.of(v);
                    default:
                        assert false;
                        throw exception(TypeDBException.of(UNRECOGNISED_VALUE));
                }
            });
        }

        @Override
        public Seekable<ThingImpl, Order.Asc> getInstancesExplicit() {
            return emptySorted();
        }

        @Override
        public void setOwns(AttributeType attributeType, boolean isKey) {
            throw exception(TypeDBException.of(ROOT_TYPE_MUTATION));
        }

        @Override
        public void setOwns(AttributeType attributeType, AttributeType overriddenType, boolean isKey) {
            throw exception(TypeDBException.of(ROOT_TYPE_MUTATION));
        }

        @Override
        public void setPlays(RoleType roleType) {
            throw exception(TypeDBException.of(ROOT_TYPE_MUTATION));
        }

        @Override
        public void setPlays(RoleType roleType, RoleType overriddenType) {
            throw exception(TypeDBException.of(ROOT_TYPE_MUTATION));
        }

        @Override
        public void unsetPlays(RoleType roleType) {
            throw exception(TypeDBException.of(ROOT_TYPE_MUTATION));
        }

        /**
         * No-op validation method of the root type 'thing'.
         *
         * There's nothing to validate for the root type 'thing'.
         */
        @Override
        public List<TypeDBException> validate() {
            return Collections.emptyList();
        }
    }
}
