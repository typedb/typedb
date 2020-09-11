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

package grakn.core.concept.schema.impl;

import grakn.core.common.exception.GraknException;
import grakn.core.concept.data.Entity;
import grakn.core.concept.data.impl.RoleImpl;
import grakn.core.concept.schema.RoleType;
import grakn.core.graph.SchemaGraph;
import grakn.core.graph.util.Encoding;
import grakn.core.graph.vertex.ThingVertex;
import grakn.core.graph.vertex.TypeVertex;

import javax.annotation.Nullable;
import java.util.List;
import java.util.stream.Stream;

import static grakn.core.common.exception.ErrorMessage.TypeRead.TYPE_ROOT_MISMATCH;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.ROOT_TYPE_MUTATION;
import static grakn.core.common.iterator.Iterators.apply;
import static grakn.core.common.iterator.Iterators.stream;

public class RoleTypeImpl extends TypeImpl implements RoleType {

    private RoleTypeImpl(TypeVertex vertex) {
        super(vertex);
        assert vertex.encoding() == Encoding.Vertex.Type.ROLE_TYPE;
        if (vertex.encoding() != Encoding.Vertex.Type.ROLE_TYPE) {
            throw exception(TYPE_ROOT_MISMATCH.message(
                    vertex.label(),
                    Encoding.Vertex.Type.ROLE_TYPE.root().label(),
                    vertex.encoding().root().label()
            ));
        }
    }

    private RoleTypeImpl(SchemaGraph graph, String label, String relation) {
        super(graph, label, Encoding.Vertex.Type.ROLE_TYPE, relation);
    }

    public static RoleTypeImpl of(TypeVertex vertex) {
        if (vertex.label().equals(Encoding.Vertex.Type.Root.ROLE.label())) return new RoleTypeImpl.Root(vertex);
        else return new RoleTypeImpl(vertex);
    }

    public static RoleTypeImpl of(SchemaGraph graph, String label, String relation) {
        return new RoleTypeImpl(graph, label, relation);
    }

    void setAbstract() {
        vertex.isAbstract(true);
    }

    void unsetAbstract() {
        vertex.isAbstract(false);
    }

    void sup(RoleType superType) {
        super.superTypeVertex(((RoleTypeImpl) superType).vertex);
    }

    @Override
    public String getScope() {
        return vertex.scope();
    }

    @Nullable
    @Override
    public RoleTypeImpl getSupertype() {
        return super.getSupertype(RoleTypeImpl::of);
    }

    @Override
    public Stream<RoleTypeImpl> getSupertypes() {
        return super.getSupertypes(RoleTypeImpl::of);
    }

    @Override
    public Stream<RoleTypeImpl> getSubtypes() {
        return super.getSubtypes(RoleTypeImpl::of);
    }

    @Override
    public RelationTypeImpl getRelation() {
        return RelationTypeImpl.of(vertex.ins().edge(Encoding.Edge.Type.RELATES).from().next());
    }

    @Override
    public Stream<RelationTypeImpl> getRelations() {
        return getRelation().getSubtypes().filter(r -> r.overriddenRoles().noneMatch(o -> o.equals(this)));
    }

    @Override
    public Stream<ThingTypeImpl> getPlayers() {
        return stream(apply(vertex.ins().edge(Encoding.Edge.Type.PLAYS).from(), ThingTypeImpl::of));
    }

    @Override
    public void delete() {
        vertex.delete();
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
        ThingVertex instance = vertex.graph().data().create(vertex.iid(), isInferred);
        return RoleImpl.of(instance);
    }

    public static class Root extends RoleTypeImpl {

        Root(TypeVertex vertex) {
            super(vertex);
            assert vertex.label().equals(Encoding.Vertex.Type.Root.ROLE.label());
        }

        @Override
        public boolean isRoot() { return true; }

        @Override
        public void setLabel(String label) { throw exception(ROOT_TYPE_MUTATION.message()); }

        @Override
        void unsetAbstract() { throw exception(ROOT_TYPE_MUTATION.message()); }

        @Override
        void sup(RoleType superType) { throw exception(ROOT_TYPE_MUTATION.message()); }
    }
}
