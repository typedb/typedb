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

package grakn.core.concept.type.impl;

import grakn.core.common.exception.GraknException;
import grakn.core.concept.thing.Entity;
import grakn.core.concept.thing.impl.RoleImpl;
import grakn.core.concept.type.RoleType;
import grakn.core.graph.GraphManager;
import grakn.core.graph.util.Encoding;
import grakn.core.graph.vertex.ThingVertex;
import grakn.core.graph.vertex.TypeVertex;

import javax.annotation.Nullable;
import java.util.List;
import java.util.stream.Stream;

import static grakn.core.common.exception.ErrorMessage.TypeRead.TYPE_ROOT_MISMATCH;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.INVALID_UNDEFINE_RELATES_HAS_INSTANCES;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.ROOT_TYPE_MUTATION;
import static grakn.core.graph.util.Encoding.Vertex.Type.ROLE_TYPE;
import static grakn.core.graph.util.Encoding.Vertex.Type.Root.ROLE;

public class RoleTypeImpl extends TypeImpl implements RoleType {

    private RoleTypeImpl(GraphManager graphMgr, TypeVertex vertex) {
        super(graphMgr, vertex);
        assert vertex.encoding() == ROLE_TYPE;
        if (vertex.encoding() != ROLE_TYPE) {
            throw exception(GraknException.of(TYPE_ROOT_MISMATCH, vertex.label(),
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
        return super.getSupertype(v -> of(graphMgr, v));
    }

    @Override
    public Stream<RoleTypeImpl> getSupertypes() {
        return super.getSupertypes(v -> of(graphMgr, v));
    }

    @Override
    public Stream<RoleTypeImpl> getSubtypes() {
        return super.getSubtypes(v -> of(graphMgr, v));
    }

    @Override
    public RelationTypeImpl getRelationType() {
        return RelationTypeImpl.of(graphMgr, vertex.ins().edge(Encoding.Edge.Type.RELATES).from().next());
    }

    @Override
    public Stream<RelationTypeImpl> getRelationTypes() {
        return getRelationType().getSubtypes().filter(r -> r.overriddenRoles().noneMatch(o -> o.equals(this)));
    }

    @Override
    public Stream<ThingTypeImpl> getPlayers() {
        return vertex.ins().edge(Encoding.Edge.Type.PLAYS).from().map(v -> ThingTypeImpl.of(graphMgr, v)).stream();
    }

    @Override
    public void delete() {
        if (getInstances().findAny().isPresent()) {
            throw GraknException.of(INVALID_UNDEFINE_RELATES_HAS_INSTANCES, getLabel());
        }
        vertex.delete();
    }

    private Stream<RoleImpl> getInstances() {
        return instances(RoleImpl::of);
    }

    @Override
    public List<GraknException> validate() {
        return super.validate();
    }

    @Override
    public RoleTypeImpl asRoleType() { return this; }

    public RoleImpl create() {
        return create(false);
    }

    public RoleImpl create(boolean isInferred) {
        validateIsCommittedAndNotAbstract(Entity.class);
        final ThingVertex instance = graphMgr.data().create(vertex, isInferred);
        return RoleImpl.of(instance);
    }

    public static class Root extends RoleTypeImpl {

        Root(GraphManager graphMgr, TypeVertex vertex) {
            super(graphMgr, vertex);
            assert vertex.label().equals(ROLE.label());
        }

        @Override
        public boolean isRoot() { return true; }

        @Override
        public void setLabel(String label) { throw exception(GraknException.of(ROOT_TYPE_MUTATION)); }

        @Override
        void unsetAbstract() { throw exception(GraknException.of(ROOT_TYPE_MUTATION)); }

        @Override
        void sup(RoleType superType) { throw exception(GraknException.of(ROOT_TYPE_MUTATION)); }
    }
}
