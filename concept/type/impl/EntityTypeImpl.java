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

package hypergraph.concept.type.impl;

import hypergraph.common.exception.HypergraphException;
import hypergraph.common.iterator.Iterators;
import hypergraph.concept.thing.impl.EntityImpl;
import hypergraph.concept.type.EntityType;
import hypergraph.graph.TypeGraph;
import hypergraph.graph.util.Schema;
import hypergraph.graph.vertex.ThingVertex;
import hypergraph.graph.vertex.TypeVertex;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.stream.Stream;

import static hypergraph.common.exception.Error.TypeWrite.INVALID_ROOT_TYPE_MUTATION;
import static java.util.Spliterator.IMMUTABLE;
import static java.util.Spliterator.ORDERED;
import static java.util.Spliterators.spliteratorUnknownSize;
import static java.util.stream.StreamSupport.stream;

public class EntityTypeImpl extends ThingTypeImpl implements EntityType {

    private EntityTypeImpl(TypeVertex vertex) {
        super(vertex);
        if (vertex.schema() != Schema.Vertex.Type.ENTITY_TYPE) {
            throw new HypergraphException("Invalid Entity Type: " + vertex.label() +
                                                  " subtypes " + vertex.schema().root().label());
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
    public EntityImpl create() {
        return create(false);
    }

    @Override
    public EntityImpl create(boolean isInferred) {
        ThingVertex instance = vertex.graph().thingGraph().create(Schema.Vertex.Thing.ENTITY, vertex, isInferred);
        return new EntityImpl(instance);
    }

    @Override
    public void sup(EntityType superType) {
        super.superTypeVertex(((EntityTypeImpl) superType).vertex);
    }

    @Nullable
    @Override
    public EntityTypeImpl sup() {
        TypeVertex vertex = super.superTypeVertex();
        return vertex != null ? of(vertex) : null;
    }

    @Override
    public Stream<EntityTypeImpl> sups() {
        Iterator<EntityTypeImpl> sups = Iterators.apply(super.superTypeVertices(), EntityTypeImpl::of);
        return stream(spliteratorUnknownSize(sups, ORDERED | IMMUTABLE), false);
    }

    @Override
    public Stream<EntityTypeImpl> subs() {
        Iterator<EntityTypeImpl> subs = Iterators.apply(super.subTypeVertices(), EntityTypeImpl::of);
        return stream(spliteratorUnknownSize(subs, ORDERED | IMMUTABLE), false);
    }

    private static class Root extends EntityTypeImpl {

        private Root(TypeVertex vertex) {
            super(vertex);
            assert vertex.label().equals(Schema.Vertex.Type.Root.ENTITY.label());
        }

        @Override
        public boolean isRoot() { return true; }

        @Override
        public void label(String label) { throw new HypergraphException(INVALID_ROOT_TYPE_MUTATION); }

        @Override
        public void isAbstract(boolean isAbstract) { throw new HypergraphException(INVALID_ROOT_TYPE_MUTATION); }

        @Override
        public void sup(EntityType superType) { throw new HypergraphException(INVALID_ROOT_TYPE_MUTATION); }
    }
}
