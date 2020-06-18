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

package hypergraph.graph.adjacency.impl;

import hypergraph.graph.adjacency.Adjacency;
import hypergraph.graph.adjacency.ThingAdjacency;
import hypergraph.graph.edge.Edge;
import hypergraph.graph.edge.ThingEdge;
import hypergraph.graph.edge.impl.ThingEdgeImpl;
import hypergraph.graph.iid.EdgeIID;
import hypergraph.graph.iid.InfixIID;
import hypergraph.graph.iid.VertexIID;
import hypergraph.graph.util.Schema;
import hypergraph.graph.vertex.ThingVertex;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static hypergraph.common.collection.Bytes.join;
import static hypergraph.common.iterator.Iterators.apply;
import static hypergraph.common.iterator.Iterators.distinct;
import static hypergraph.common.iterator.Iterators.link;
import static java.util.Collections.emptyList;

public abstract class ThingAdjacencyImpl implements Adjacency<Schema.Edge.Thing, ThingEdge, ThingVertex> {

    final ThingVertex owner;
    final Direction direction;
    final ConcurrentMap<InfixIID, Set<ThingEdge>> edges;

    ThingAdjacencyImpl(ThingVertex owner, Direction direction) {
        this.owner = owner;
        this.direction = direction;
        this.edges = new ConcurrentHashMap<>();
    }

    InfixIID infixIID(Schema.Edge.Thing prefix) {
        return infixIID(prefix, Collections.emptyList());
    }

    InfixIID infixIID(Schema.Edge.Thing prefix, List<VertexIID> metadata) {
        Schema.Infix infix = direction.isOut() ? prefix.out() : prefix.in();
        if (infix != null) return InfixIID.of(infix, metadata);
        return null;
    }

    public abstract ThingIteratorBuilderImpl edge(Schema.Edge.Thing schema, List<VertexIID> metadata);

    public abstract ThingEdge edge(Schema.Edge.Thing schema, List<VertexIID> metadata, ThingVertex adjacent);

    public abstract void delete(Schema.Edge.Thing schema, List<VertexIID> metadata, ThingVertex adjacent);

    public abstract void delete(Schema.Edge.Thing schema, List<VertexIID> metadata);

    @Override
    public ThingIteratorBuilderImpl edge(Schema.Edge.Thing schema) {
        return edge(schema, emptyList());
    }

    @Override
    public ThingEdge edge(Schema.Edge.Thing schema, ThingVertex adjacent) {
        return edge(schema, emptyList(), adjacent);
    }

    @Override
    public void delete(Schema.Edge.Thing schema, ThingVertex adjacent) {
        delete(schema, emptyList(), adjacent);
    }

    @Override
    public void delete(Schema.Edge.Thing schema) {
        delete(schema, emptyList());
    }

    @Override
    public void deleteNonRecursive(ThingEdge edge) {
        InfixIID infix = infixIID(edge.schema());
        if (edges.containsKey(infix)) {
            edges.get(infix).remove(edge);
            owner.setModified();
        }
    }

    @Override
    public void put(Schema.Edge.Thing schema, ThingVertex adjacent) {
        put(schema, emptyList(), adjacent);
    }

    public void put(Schema.Edge.Thing schema, List<VertexIID> metadata, ThingVertex adjacent) {
        ThingVertex from = direction.isOut() ? owner : adjacent;
        ThingVertex to = direction.isOut() ? adjacent : owner;
        ThingEdge edge = new ThingEdgeImpl.Buffered(from, schema, to);
        edges.computeIfAbsent(infixIID(schema, metadata), e -> ConcurrentHashMap.newKeySet()).add(edge);
        to.ins().putNonRecursive(edge);
        owner.setModified();
    }

    @Override
    public void putNonRecursive(ThingEdge edge) {
        load(edge);
        owner.setModified();
    }

    @Override
    public void load(ThingEdge edge) {
        edges.computeIfAbsent(infixIID(edge.schema()), e -> ConcurrentHashMap.newKeySet()).add(edge);
    }

    @Override
    public void deleteAll() {
        for (Schema.Edge.Thing schema : Schema.Edge.Thing.values()) delete(schema);
    }

    static class ThingIteratorBuilderImpl
            implements Adjacency.IteratorBuilder<ThingVertex> {

        private final Iterator<ThingEdge> edgeIterator;

        ThingIteratorBuilderImpl(Iterator<ThingEdge> edgeIterator) {
            this.edgeIterator = edgeIterator;
        }

        @Override
        public Iterator<ThingVertex> to() {
            return apply(edgeIterator, Edge::to);
        }

        @Override
        public Iterator<ThingVertex> from() {
            return apply(edgeIterator, Edge::from);
        }
    }

    public static class Buffered extends ThingAdjacencyImpl implements ThingAdjacency {

        public Buffered(ThingVertex owner, Direction direction) {
            super(owner, direction);
        }

        @Override
        public ThingIteratorBuilderImpl edge(Schema.Edge.Thing schema, List<VertexIID> metadata) {
            Set<ThingEdge> edges;

            if (schema.isOptimisation() && (edges = this.edges.get(infixIID(schema, metadata))) != null) {
                return new ThingIteratorBuilderImpl(edges.iterator());
            } else if (!schema.isOptimisation() && (edges = this.edges.get(infixIID(schema))) != null) {
//                return new ThingIteratorBuilderImpl(filter(edges.iterator(), e -> e.infix().hasMetaData(metadata)));
                return new ThingIteratorBuilderImpl(edges.iterator());
            } else {
                return new ThingIteratorBuilderImpl(Collections.emptyIterator());
            }
        }

        @Override
        public ThingEdge edge(Schema.Edge.Thing schema, List<VertexIID> metadata, ThingVertex adjacent) {
            InfixIID infix = infixIID(schema);
            if (edges.containsKey(infix)) {
                Predicate<ThingEdge> predicate = direction.isOut()
                        ? e -> e.to().equals(adjacent)
                        : e -> e.from().equals(adjacent);
                return edges.get(infix).stream().filter(predicate).findAny().orElse(null);
            }
            return null;
        }

        @Override
        public void delete(Schema.Edge.Thing schema, List<VertexIID> metadata, ThingVertex adjacent) {
            InfixIID infix = infixIID(schema);
            if (edges.containsKey(infix)) {
                Predicate<ThingEdge> predicate = direction.isOut()
                        ? e -> e.to().equals(adjacent)
                        : e -> e.from().equals(adjacent);
                edges.get(infix).stream().filter(predicate).forEach(Edge::delete);
            }
        }

        @Override
        public void delete(Schema.Edge.Thing schema, List<VertexIID> metadata) {
            InfixIID infix = infixIID(schema);
            if (infix != null && edges.containsKey(infix)) edges.get(infix).forEach(Edge::delete);
        }

        @Override
        public void forEach(Consumer<ThingEdge> function) {
            edges.forEach((key, set) -> set.forEach(function));
        }
    }

    public static class Persisted extends ThingAdjacencyImpl implements ThingAdjacency {


        public Persisted(ThingVertex owner, Direction direction) {
            super(owner, direction);
        }

        private Iterator<ThingEdge> edgeIterator(InfixIID infix) {
            Iterator<ThingEdge> storageIterator = owner.graph().storage().iterate(
                    join(owner.iid().bytes(), infix.bytes()),
                    (key, value) -> new ThingEdgeImpl.Persisted(owner.graph(), EdgeIID.Thing.of(key))
            );

            if (edges.get(infix) == null) {
                return storageIterator;
            } else {
                return distinct(link(edges.get(infix).iterator(), storageIterator));
            }
        }

        @Override
        public ThingIteratorBuilderImpl edge(Schema.Edge.Thing schema, List<VertexIID> metadata) {
            return new ThingIteratorBuilderImpl(edgeIterator(infixIID(schema)));
        }

        @Override
        public ThingEdge edge(Schema.Edge.Thing schema, List<VertexIID> metadata, ThingVertex adjacent) {
            InfixIID infix = infixIID(schema);
            Optional<ThingEdge> container;
            Predicate<ThingEdge> predicate = direction.isOut()
                    ? e -> e.to().equals(adjacent)
                    : e -> e.from().equals(adjacent);

            if (edges.containsKey(infix) && (container = edges.get(infix).stream().filter(predicate).findAny()).isPresent()) {
                return container.get();
            } else {
                byte[] edgeIID = join(owner.iid().bytes(), infix.bytes(), adjacent.iid().bytes());
                if (owner.graph().storage().get(edgeIID) != null) {
                    return new ThingEdgeImpl.Persisted(owner.graph(), EdgeIID.Thing.of(edgeIID));
                }
            }

            return null;
        }

        @Override
        public void delete(Schema.Edge.Thing schema, List<VertexIID> metadata, ThingVertex adjacent) {
            InfixIID infix = infixIID(schema);
            Optional<ThingEdge> edge;
            Predicate<ThingEdge> predicate = direction.isOut()
                    ? e -> e.to().equals(adjacent)
                    : e -> e.from().equals(adjacent);

            if (edges.containsKey(infix) && (edge = edges.get(infix).stream().filter(predicate).findAny()).isPresent()) {
                edge.get().delete();
            }

            byte[] edgeIID = join(owner.iid().bytes(), infix.bytes(), adjacent.iid().bytes());
            if (owner.graph().storage().get(edgeIID) != null) {
                ((ThingEdge) new ThingEdgeImpl.Persisted(owner.graph(), EdgeIID.Thing.of(edgeIID))).delete();
            }
        }

        @Override
        public void delete(Schema.Edge.Thing schema, List<VertexIID> metadata) {
            InfixIID infix = infixIID(schema);
            if (edges.containsKey(infix)) edges.get(infix).parallelStream().forEach(Edge::delete);
            Iterator<ThingEdge> storageIterator = owner.graph().storage().iterate(
                    join(owner.iid().bytes(), direction.isOut() ? schema.out().bytes() : schema.in().bytes()),
                    (key, value) -> new ThingEdgeImpl.Persisted(owner.graph(), EdgeIID.Thing.of(key))
            );
            storageIterator.forEachRemaining(Edge::delete);
        }

        @Override
        public void forEach(Consumer<ThingEdge> function) {
            for (Schema.Edge.Thing schema : Schema.Edge.Thing.values()) {
                InfixIID infix = infixIID(schema);
                if (infix != null) edgeIterator(infix).forEachRemaining(function);
            }
        }
    }
}
