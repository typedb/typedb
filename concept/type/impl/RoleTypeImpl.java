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
import grakn.core.graph.TypeGraph;
import grakn.core.graph.util.Schema;
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
        assert vertex.schema() == Schema.Vertex.Type.ROLE_TYPE;
        if (vertex.schema() != Schema.Vertex.Type.ROLE_TYPE) {
            throw new GraknException(TYPE_ROOT_MISMATCH.message(
                    vertex.label(),
                    Schema.Vertex.Type.ROLE_TYPE.root().label(),
                    vertex.schema().root().label()
            ));
        }
    }

    private RoleTypeImpl(TypeGraph graph, String label, String relation) {
        super(graph, label, Schema.Vertex.Type.ROLE_TYPE, relation);
    }

    public static RoleTypeImpl of(TypeVertex vertex) {
        if (vertex.label().equals(Schema.Vertex.Type.Root.ROLE.label())) return new RoleTypeImpl.Root(vertex);
        else return new RoleTypeImpl(vertex);
    }

    public static RoleTypeImpl of(TypeGraph graph, String label, String relation) {
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

    @Nullable
    @Override
    public RoleTypeImpl getSupertype() {
        return super.sup(RoleTypeImpl::of);
    }

    @Override
    public Stream<RoleTypeImpl> getSupertypes() {
        return super.sups(RoleTypeImpl::of);
    }

    @Override
    public Stream<RoleTypeImpl> getSubtypes() {
        return super.subs(RoleTypeImpl::of);
    }

    @Override
    public String getScopedLabel() {
        return vertex.scopedLabel();
    }

    @Override
    public RelationTypeImpl getRelation() {
        return RelationTypeImpl.of(vertex.ins().edge(Schema.Edge.Type.RELATES).from().next());
    }

    @Override
    public Stream<RelationTypeImpl> getRelations() {
        return getRelation().getSubtypes().filter(rel -> rel.getRelates().anyMatch(rol -> rol.equals(this)));
    }

    @Override
    public Stream<ThingTypeImpl> getPlayers() {
        return stream(apply(vertex.ins().edge(Schema.Edge.Type.PLAYS).from(), ThingTypeImpl::of));
    }

    @Override
    public void delete() {
        vertex.delete();
    }

    @Override
    public List<GraknException> validate() {
        return super.validate();
    }

    public RoleImpl create() {
        return create(false);
    }

    public RoleImpl create(boolean isInferred) {
        validateIsCommitedAndNotAbstract(Entity.class);
        ThingVertex instance = vertex.graph().thing().create(vertex.iid(), isInferred);
        return RoleImpl.of(instance);
    }

    public static class Root extends RoleTypeImpl {

        Root(TypeVertex vertex) {
            super(vertex);
            assert vertex.label().equals(Schema.Vertex.Type.Root.ROLE.label());
        }

        @Override
        public boolean isRoot() { return true; }

        @Override
        public void setLabel(String label) { throw new GraknException(ROOT_TYPE_MUTATION); }

        @Override
        void unsetAbstract() { throw new GraknException(ROOT_TYPE_MUTATION); }

        @Override
        void sup(RoleType superType) { throw new GraknException(ROOT_TYPE_MUTATION); }
    }
}
