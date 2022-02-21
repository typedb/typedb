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
import com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.Order;
import com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.Seekable;
import com.vaticle.typedb.core.concept.thing.Entity;
import com.vaticle.typedb.core.concept.thing.impl.RoleImpl;
import com.vaticle.typedb.core.concept.type.RoleType;
import com.vaticle.typedb.core.graph.GraphManager;
import com.vaticle.typedb.core.graph.common.Encoding;
import com.vaticle.typedb.core.graph.vertex.ThingVertex;
import com.vaticle.typedb.core.graph.vertex.TypeVertex;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeRead.TYPE_ROOT_MISMATCH;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.INVALID_UNDEFINE_RELATES_HAS_INSTANCES;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.ROOT_TYPE_MUTATION;
import static com.vaticle.typedb.core.common.iterator.Iterators.Sorted.Seekable.iterateSorted;
import static com.vaticle.typedb.core.common.iterator.Iterators.loop;
import static com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.ASC;
import static com.vaticle.typedb.core.graph.common.Encoding.Edge.Type.SUB;
import static com.vaticle.typedb.core.graph.common.Encoding.Vertex.Type.ROLE_TYPE;
import static com.vaticle.typedb.core.graph.common.Encoding.Vertex.Type.Root.ROLE;

public class RoleTypeImpl extends TypeImpl implements RoleType {

    private RoleTypeImpl(GraphManager graphMgr, TypeVertex vertex) {
        super(graphMgr, vertex);
        assert vertex.encoding() == ROLE_TYPE;
        if (vertex.encoding() != ROLE_TYPE) {
            throw exception(TypeDBException.of(TYPE_ROOT_MISMATCH, vertex.label(),
                    ROLE_TYPE.root().label(), vertex.encoding().root().label()));
        }
    }

    private RoleTypeImpl(GraphManager graphMgr, String label, String relation) {
        super(graphMgr, label, ROLE_TYPE, relation);
    }

    public static RoleTypeImpl of(GraphManager graphMgr, TypeVertex vertex) {
        if (vertex.label().equals(ROLE.label())) return new RoleTypeImpl.Root(graphMgr, vertex);
        else return new RoleTypeImpl(graphMgr, vertex);
    }

    public static RoleTypeImpl of(GraphManager graphMgr, String label, String relation) {
        return new RoleTypeImpl(graphMgr, label, relation);
    }

    void setAbstract() {
        vertex.isAbstract(true);
    }

    void unsetAbstract() {
        vertex.isAbstract(false);
    }

    void sup(RoleType superType) {
        super.setSuperTypeVertex(((RoleTypeImpl) superType).vertex);
    }

    @Nullable
    @Override
    public RoleTypeImpl getSupertype() {
        return vertex.outs().edge(SUB).to().map(t -> RoleTypeImpl.of(graphMgr, t)).firstOrNull();
    }

    @Override
    public FunctionalIterator<RoleTypeImpl> getSupertypes() {
        return loop(vertex, Objects::nonNull, v -> v.outs().edge(SUB).to().firstOrNull())
                .map(v -> RoleTypeImpl.of(graphMgr, v));
    }

    @Override
    public Seekable<RoleTypeImpl, Order.Asc> getSubtypes() {
        return iterateSorted(graphMgr.schema().getSubtypes(vertex), ASC)
                .mapSorted(v -> of(graphMgr, v), rt -> rt.vertex, ASC);
    }

    @Override
    public Seekable<RoleTypeImpl, Order.Asc> getSubtypesExplicit() {
        return super.getSubtypesExplicit(v -> of(graphMgr, v));
    }

    @Override
    public RelationTypeImpl getRelationType() {
        return RelationTypeImpl.of(graphMgr, vertex.ins().edge(Encoding.Edge.Type.RELATES).from().first().get());
    }

    @Override
    public Seekable<RelationTypeImpl, Order.Asc> getRelationTypes() {
        return iterateSorted(graphMgr.schema().relationsOfRoleType(vertex), ASC)
                .mapSorted(v -> RelationTypeImpl.of(graphMgr, v), relType -> relType.vertex, ASC);
    }

    @Override
    public Seekable<ThingTypeImpl, Order.Asc> getPlayers() {
        return vertex.ins().edge(Encoding.Edge.Type.PLAYS).from()
                .mapSorted(v -> ThingTypeImpl.of(graphMgr, v), thingType -> thingType.vertex, ASC);
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
                .flatMap(t -> graphMgr.data().getReadable(t.vertex))
                .map(RoleImpl::of);
    }

    @Override
    public List<TypeDBException> validate() {
        return super.validate();
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
        ThingVertex.Write instance = graphMgr.data().create(vertex, isInferred);
        return RoleImpl.of(instance);
    }

    public static class Root extends RoleTypeImpl {

        Root(GraphManager graphMgr, TypeVertex vertex) {
            super(graphMgr, vertex);
            assert vertex.label().equals(ROLE.label());
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
        void unsetAbstract() {
            throw exception(TypeDBException.of(ROOT_TYPE_MUTATION));
        }

        @Override
        void sup(RoleType superType) {
            throw exception(TypeDBException.of(ROOT_TYPE_MUTATION));
        }
    }
}
