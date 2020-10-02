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
import grakn.core.graph.Graphs;
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

    private EntityTypeImpl(final Graphs graphs, final TypeVertex vertex) {
        super(graphs, vertex);
        if (vertex.encoding() != ENTITY_TYPE) {
            throw exception(TYPE_ROOT_MISMATCH.message(
                    vertex.label(), ENTITY_TYPE.root().label(), vertex.encoding().root().label()
            ));
        }
    }

    private EntityTypeImpl(final Graphs graphs, final String label) {
        super(graphs, label, ENTITY_TYPE);
        assert !label.equals(ENTITY.label());
    }

    public static EntityTypeImpl of(final Graphs graphs, final TypeVertex vertex) {
        if (vertex.label().equals(ENTITY.label())) {
            return new EntityTypeImpl.Root(graphs, vertex);
        } else return new EntityTypeImpl(graphs, vertex);
    }

    public static EntityTypeImpl of(final Graphs graphs, final String label) {
        return new EntityTypeImpl(graphs, label);
    }

    @Override
    public void setSupertype(final EntityType superType) {
        super.superTypeVertex(((EntityTypeImpl) superType).vertex);
    }

    @Nullable
    @Override
    public EntityTypeImpl getSupertype() {
        return super.getSupertype(v -> of(graphs, v));
    }

    @Override
    public Stream<EntityTypeImpl> getSupertypes() {
        return super.getSupertypes(v -> of(graphs, v));
    }

    @Override
    public Stream<EntityTypeImpl> getSubtypes() {
        return super.getSubtypes(v -> of(graphs, v));
    }

    @Override
    public Stream<EntityImpl> getInstances() {
        return super.instances(EntityImpl::of);
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
    public EntityImpl create(final boolean isInferred) {
        validateIsCommittedAndNotAbstract(Entity.class);
        final ThingVertex instance = graphs.data().create(vertex.iid(), isInferred);
        return EntityImpl.of(instance);
    }

    @Override
    public EntityTypeImpl asEntityType() { return this; }

    private static class Root extends EntityTypeImpl {

        private Root(final Graphs graphs, final TypeVertex vertex) {
            super(graphs, vertex);
            assert vertex.label().equals(ENTITY.label());
        }

        @Override
        public boolean isRoot() { return true; }

        @Override
        public void setLabel(final String label) {
            throw exception(ROOT_TYPE_MUTATION.message());
        }

        @Override
        public void unsetAbstract() {
            throw exception(ROOT_TYPE_MUTATION.message());
        }

        @Override
        public void setSupertype(final EntityType superType) {
            throw exception(ROOT_TYPE_MUTATION.message());
        }

        @Override
        public void setOwns(final AttributeType attributeType, final boolean isKey) {
            throw exception(ROOT_TYPE_MUTATION.message());
        }

        @Override
        public void setOwns(final AttributeType attributeType, final AttributeType overriddenType, final boolean isKey) {
            throw exception(ROOT_TYPE_MUTATION.message());
        }

        @Override
        public void setPlays(final RoleType roleType) {
            throw exception(ROOT_TYPE_MUTATION.message());
        }

        @Override
        public void setPlays(final RoleType roleType, final RoleType overriddenType) {
            throw exception(ROOT_TYPE_MUTATION.message());
        }

        @Override
        public void unsetPlays(final RoleType roleType) {
            throw exception(ROOT_TYPE_MUTATION.message());
        }
    }
}
