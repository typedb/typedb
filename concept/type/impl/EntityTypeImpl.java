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

package grakn.concept.type.impl;

import grakn.common.exception.Error;
import grakn.common.exception.GraknException;
import grakn.concept.thing.Entity;
import grakn.concept.thing.impl.EntityImpl;
import grakn.concept.type.AttributeType;
import grakn.concept.type.EntityType;
import grakn.concept.type.RoleType;
import grakn.graph.TypeGraph;
import grakn.graph.util.Schema;
import grakn.graph.vertex.ThingVertex;
import grakn.graph.vertex.TypeVertex;

import javax.annotation.Nullable;
import java.util.List;
import java.util.stream.Stream;

import static grakn.common.exception.Error.TypeWrite.ROOT_TYPE_MUTATION;

public class EntityTypeImpl extends ThingTypeImpl implements EntityType {

    private EntityTypeImpl(TypeVertex vertex) {
        super(vertex);
        if (vertex.schema() != Schema.Vertex.Type.ENTITY_TYPE) {
            throw new GraknException(Error.TypeRead.TYPE_ROOT_MISMATCH.format(
                    vertex.label(),
                    Schema.Vertex.Type.ENTITY_TYPE.root().label(),
                    vertex.schema().root().label()
            ));
        }
    }

    private EntityTypeImpl(TypeGraph graph, String label) {
        super(graph, label, Schema.Vertex.Type.ENTITY_TYPE);
        assert !label.equals(Schema.Vertex.Type.Root.ENTITY.label());
    }

    public static EntityTypeImpl of(TypeVertex vertex) {
        if (vertex.label().equals(Schema.Vertex.Type.Root.ENTITY.label())) return new EntityTypeImpl.Root(vertex);
        else return new EntityTypeImpl(vertex);
    }

    public static EntityTypeImpl of(TypeGraph graph, String label) {
        return new EntityTypeImpl(graph, label);
    }

    @Override
    public void sup(EntityType superType) {
        super.superTypeVertex(((EntityTypeImpl) superType).vertex);
    }

    @Nullable
    @Override
    public EntityTypeImpl sup() {
        return super.sup(EntityTypeImpl::of);
    }

    @Override
    public Stream<EntityTypeImpl> sups() {
        return super.sups(EntityTypeImpl::of);
    }

    @Override
    public Stream<EntityTypeImpl> subs() {
        return super.subs(EntityTypeImpl::of);
    }

    @Override
    public Stream<EntityImpl> instances() {
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
    public EntityImpl create(boolean isInferred) {
        validateIsCommitedAndNotAbstract(Entity.class);
        ThingVertex instance = vertex.graph().thing().create(vertex.iid(), isInferred);
        return EntityImpl.of(instance);
    }

    private static class Root extends EntityTypeImpl {

        private Root(TypeVertex vertex) {
            super(vertex);
            assert vertex.label().equals(Schema.Vertex.Type.Root.ENTITY.label());
        }

        @Override
        public boolean isRoot() { return true; }

        @Override
        public void label(String label) {
            throw new GraknException(ROOT_TYPE_MUTATION);
        }

        @Override
        public void isAbstract(boolean isAbstract) {
            throw new GraknException(ROOT_TYPE_MUTATION);
        }

        @Override
        public void sup(EntityType superType) {
            throw new GraknException(ROOT_TYPE_MUTATION);
        }

        @Override
        public void has(AttributeType attributeType, boolean isKey) {
            throw new GraknException(ROOT_TYPE_MUTATION);
        }

        @Override
        public void has(AttributeType attributeType, AttributeType overriddenType, boolean isKey) {
            throw new GraknException(ROOT_TYPE_MUTATION);
        }

        @Override
        public void plays(RoleType roleType) {
            throw new GraknException(ROOT_TYPE_MUTATION);
        }

        @Override
        public void plays(RoleType roleType, RoleType overriddenType) {
            throw new GraknException(ROOT_TYPE_MUTATION);
        }

        @Override
        public void unplay(RoleType roleType) {
            throw new GraknException(ROOT_TYPE_MUTATION);
        }
    }
}
