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

package com.vaticle.typedb.core.concept.type.impl;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.Forwardable;
import com.vaticle.typedb.core.common.parameters.Order;
import com.vaticle.typedb.core.concept.ConceptManager;
import com.vaticle.typedb.core.concept.thing.Entity;
import com.vaticle.typedb.core.concept.thing.impl.RelationImpl;
import com.vaticle.typedb.core.concept.thing.impl.RoleImpl;
import com.vaticle.typedb.core.concept.thing.impl.ThingImpl;
import com.vaticle.typedb.core.concept.type.RelationType;
import com.vaticle.typedb.core.concept.type.RoleType;
import com.vaticle.typedb.core.encoding.Encoding;
import com.vaticle.typedb.core.graph.vertex.ThingVertex;
import com.vaticle.typedb.core.graph.vertex.TypeVertex;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

import static com.vaticle.typedb.common.collection.Collections.list;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeRead.TYPE_ROOT_MISMATCH;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.INVALID_UNDEFINE_RELATES_HAS_INSTANCES;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.OVERRIDDEN_RELATED_ROLE_TYPE_NOT_INHERITED;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.ROOT_TYPE_MUTATION;
import static com.vaticle.typedb.core.common.iterator.sorted.SortedIterators.Forwardable.iterateSorted;
import static com.vaticle.typedb.core.common.parameters.Order.Asc.ASC;
import static com.vaticle.typedb.core.encoding.Encoding.Edge.Type.SUB;
import static com.vaticle.typedb.core.encoding.Encoding.Vertex.Type.ROLE_TYPE;
import static com.vaticle.typedb.core.encoding.Encoding.Vertex.Type.Root.ROLE;

public class RoleTypeImpl extends TypeImpl implements RoleType {

    public RoleTypeImpl(ConceptManager conceptMgr, TypeVertex vertex) {
        super(conceptMgr, vertex);
        assert vertex.encoding() == ROLE_TYPE;
        if (vertex.encoding() != ROLE_TYPE) {
            throw exception(TypeDBException.of(TYPE_ROOT_MISMATCH, vertex.label(),
                    ROLE_TYPE.root().label(), vertex.encoding().root().label()));
        }
    }

    private RoleTypeImpl(ConceptManager conceptMgr, String label, String relation) {
        super(conceptMgr, label, ROLE_TYPE, relation);
    }

    public static RoleTypeImpl of(ConceptManager conceptMgr, String label, String relation) {
        return new RoleTypeImpl(conceptMgr, label, relation);
    }

    void setSupertype(RoleType superType) {
        setSuperTypeVertex(((RoleTypeImpl) superType).vertex);
    }

    @Nullable
    @Override
    public RoleType getSupertype() {
        return vertex.outs().edge(SUB).to().map(conceptMgr::convertRoleType).firstOrNull();
    }

    @Override
    public Forwardable<RoleTypeImpl, Order.Asc> getSupertypes() {
        return iterateSorted(graphMgr().schema().getSupertypes(vertex), ASC)
                .mapSorted(v -> (RoleTypeImpl) conceptMgr.convertRoleType(v), rt -> rt.vertex, ASC);
    }

    @Override
    public Forwardable<RoleTypeImpl, Order.Asc> getSubtypes() {
        return iterateSorted(graphMgr().schema().getSubtypes(vertex), ASC)
                .mapSorted(v ->  (RoleTypeImpl) conceptMgr.convertRoleType(v), rt -> rt.vertex, ASC);
    }

    @Override
    public Forwardable<RoleTypeImpl, Order.Asc> getSubtypesExplicit() {
        return super.getSubtypesExplicit(v ->  (RoleTypeImpl) conceptMgr.convertRoleType(v));
    }

    @Override
    public RelationType getRelationType() {
        return conceptMgr.convertRelationType(vertex.ins().edge(Encoding.Edge.Type.RELATES).from().first().get());
    }

    @Override
    public Forwardable<RelationTypeImpl, Order.Asc> getRelationTypes() {
        return iterateSorted(graphMgr().schema().relationsOfRoleType(vertex), ASC)
                .mapSorted(v -> (RelationTypeImpl) conceptMgr.convertRelationType(v), relType -> relType.vertex, ASC);
    }

    @Override
    public Forwardable<ThingTypeImpl, Order.Asc> getPlayerTypes() {
        return iterateSorted(graphMgr().schema().playersOfRoleType(vertex), ASC)
                .mapSorted(v -> (ThingTypeImpl) conceptMgr.convertThingType(v), roleType -> roleType.vertex, ASC);
    }

    @Override
    public Forwardable<ThingTypeImpl, Order.Asc> getPlayerTypesExplicit() {
        return vertex.ins().edge(Encoding.Edge.Type.PLAYS).from()
                .mapSorted(v -> (ThingTypeImpl) conceptMgr.convertThingType(v), thingType -> thingType.vertex, ASC);
    }

    @Override
    public FunctionalIterator<RelationImpl> getRelationInstances() {
        return getSubtypes().filter(t -> !t.isAbstract())
                .flatMap(t -> graphMgr().data().getReadable(t.vertex))
                .flatMap(v -> v.ins().edge(Encoding.Edge.Thing.Base.RELATING).from())
                .map(v -> RelationImpl.of(conceptMgr, v));
    }

    @Override
    public FunctionalIterator<RelationImpl> getRelationInstancesExplicit() {
        return graphMgr().data().getReadable(vertex)
                .flatMap(v -> v.ins().edge(Encoding.Edge.Thing.Base.RELATING).from())
                .map(v -> RelationImpl.of(conceptMgr, v));
    }

    @Override
    public FunctionalIterator<ThingImpl> getPlayerInstances() {
        return getSubtypes().filter(t -> !t.isAbstract())
                .flatMap(t -> graphMgr().data().getReadable(t.vertex))
                .flatMap(v -> v.ins().edge(Encoding.Edge.Thing.Base.PLAYING).from())
                .map(v -> ThingImpl.of(conceptMgr, v));
    }

    @Override
    public FunctionalIterator<ThingImpl> getPlayerInstancesExplicit() {
        return graphMgr().data().getReadable(vertex)
                .flatMap(v -> v.ins().edge(Encoding.Edge.Thing.Base.PLAYING).from())
                .map(v -> ThingImpl.of(conceptMgr, v));
    }

    @Override
    public void delete() {
        validateDelete();
        vertex.delete();
    }

    @Override
    void validateDelete() {
        super.validateDelete();
        if (getInstances().first().isPresent()) {
            throw exception(TypeDBException.of(INVALID_UNDEFINE_RELATES_HAS_INSTANCES, getLabel()));
        }
    }

    private FunctionalIterator<RoleImpl> getInstances() {
        return getSubtypes().filter(t -> !t.isAbstract())
                .flatMap(t -> graphMgr().data().getReadable(t.vertex))
                .map(RoleImpl::of);
    }

    @Override
    public List<TypeDBException> exceptions() {
        return new ArrayList<>(validateOverriddenTypesAreInheritedFromRelationType());
    }

    private List<TypeDBException> validateOverriddenTypesAreInheritedFromRelationType() {
        RoleType superType = getSupertype();
        assert !isRoot() || superType != null;
        if (superType.isRoot()) return list();
        else if (getRelationType().getSupertype().asRelationType().getRelates().noneMatch(rt -> rt.equals(superType))) {
            return list(TypeDBException.of(OVERRIDDEN_RELATED_ROLE_TYPE_NOT_INHERITED,
                    getRelationType().getLabel().name(), getLabel().name(), superType.getLabel().name()));
        } else return list();
    }

    @Override
    public boolean isRoleType() {
        return true;
    }

    @Override
    public RoleTypeImpl asRoleType() {
        return this;
    }

    public RoleImpl create() {
        return create(false);
    }

    public RoleImpl create(boolean isInferred) {
        validateCanHaveInstances(Entity.class);
        ThingVertex.Write instance = graphMgr().data().create(vertex, isInferred);
        return RoleImpl.of(instance);
    }

    public static class Root extends RoleTypeImpl {

        public Root(ConceptManager conceptMgr, TypeVertex vertex) {
            super(conceptMgr, vertex);
            assert vertex.label().equals(ROLE.label());
        }

        @Override
        public boolean isRoot() {
            return true;
        }

        @Override
        public void delete() {
            throw exception(TypeDBException.of(ROOT_TYPE_MUTATION));
        }

        @Override
        public void setLabel(String label) {
            throw exception(TypeDBException.of(ROOT_TYPE_MUTATION));
        }

        @Override
        void setSupertype(RoleType superType) {
            throw exception(TypeDBException.of(ROOT_TYPE_MUTATION));
        }
    }
}
