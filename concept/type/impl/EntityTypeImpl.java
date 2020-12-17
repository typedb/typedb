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
import grakn.core.concept.thing.impl.EntityImpl;
import grakn.core.concept.type.AttributeType;
import grakn.core.concept.type.EntityType;
import grakn.core.concept.type.RoleType;
import grakn.core.graph.GraphManager;
import grakn.core.graph.vertex.ThingVertex;
import grakn.core.graph.vertex.TypeVertex;

import javax.annotation.Nullable;
import java.util.List;
import java.util.stream.Stream;

import static grakn.core.common.exception.ErrorMessage.TypeRead.TYPE_ROOT_MISMATCH;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.ROOT_TYPE_MUTATION;
import static grakn.core.graph.util.Encoding.Vertex.Type.ENTITY_TYPE;
import static grakn.core.graph.util.Encoding.Vertex.Type.Root.ENTITY;

public class EntityTypeImpl extends ThingTypeImpl implements EntityType {

    private EntityTypeImpl(GraphManager graphMgr, TypeVertex vertex) {
        super(graphMgr, vertex);
        if (vertex.encoding() != ENTITY_TYPE) {
            throw exception(GraknException.of(TYPE_ROOT_MISMATCH, vertex.label(),
                                              ENTITY_TYPE.root().label(), vertex.encoding().root().label()));
        }
    }

    private EntityTypeImpl(GraphManager graphMgr, String label) {
        super(graphMgr, label, ENTITY_TYPE);
        assert !label.equals(ENTITY.label());
    }

    public static EntityTypeImpl of(GraphManager graphMgr, TypeVertex vertex) {
        if (vertex.label().equals(ENTITY.label())) {
            return new EntityTypeImpl.Root(graphMgr, vertex);
        } else return new EntityTypeImpl(graphMgr, vertex);
    }

    public static EntityTypeImpl of(GraphManager graphMgr, String label) {
        return new EntityTypeImpl(graphMgr, label);
    }

    @Override
    public void setSupertype(EntityType superType) {
        super.setSuperTypeVertex(((EntityTypeImpl) superType).vertex);
    }

    @Override
    public Stream<EntityTypeImpl> getSubtypes() {
        return super.getSubtypes(v -> of(graphMgr, v));
    }

    @Override
    public Stream<EntityTypeImpl> getSubtypesDirect() {
        return super.getSubtypesDirect(v -> of(graphMgr, v));
    }

    @Override
    public Stream<EntityImpl> getInstances() {
        return instances(EntityImpl::of);
    }

    @Override
    public List<GraknException> validate() {
        return super.validate();
    }

    @Override
    public EntityImpl create() {
        return create(false);
    }

    @Override
    public EntityImpl create(boolean isInferred) {
        validateIsCommittedAndNotAbstract(Entity.class);
        final ThingVertex instance = graphMgr.data().create(vertex, isInferred);
        return EntityImpl.of(instance);
    }

    @Override
    public boolean isEntityType() { return true; }

    @Override
    public EntityTypeImpl asEntityType() { return this; }

    private static class Root extends EntityTypeImpl {

        private Root(GraphManager graphMgr, TypeVertex vertex) {
            super(graphMgr, vertex);
            assert vertex.label().equals(ENTITY.label());
        }

        @Override
        public boolean isRoot() { return true; }

        @Override
        public void setLabel(String label) {
            throw exception(GraknException.of(ROOT_TYPE_MUTATION));
        }

        @Override
        public void unsetAbstract() {
            throw exception(GraknException.of(ROOT_TYPE_MUTATION));
        }

        @Override
        public void setSupertype(EntityType superType) {
            throw exception(GraknException.of(ROOT_TYPE_MUTATION));
        }

        @Override
        public void setOwns(AttributeType attributeType, boolean isKey) {
            throw exception(GraknException.of(ROOT_TYPE_MUTATION));
        }

        @Override
        public void setOwns(AttributeType attributeType, AttributeType overriddenType, boolean isKey) {
            throw exception(GraknException.of(ROOT_TYPE_MUTATION));
        }

        @Override
        public void setPlays(RoleType roleType) {
            throw exception(GraknException.of(ROOT_TYPE_MUTATION));
        }

        @Override
        public void setPlays(RoleType roleType, RoleType overriddenType) {
            throw exception(GraknException.of(ROOT_TYPE_MUTATION));
        }

        @Override
        public void unsetPlays(RoleType roleType) {
            throw exception(GraknException.of(ROOT_TYPE_MUTATION));
        }
    }
}
