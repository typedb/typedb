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
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static hypergraph.common.collection.Bytes.join;
import static hypergraph.common.iterator.Iterators.apply;
import static hypergraph.common.iterator.Iterators.distinct;
import static hypergraph.common.iterator.Iterators.filter;
import static hypergraph.common.iterator.Iterators.link;

public abstract class ThingAdjacencyImpl implements ThingAdjacency {

    final ThingVertex owner;
    final Direction direction;
    final ConcurrentMap<InfixIID.Thing, Set<ThingEdge>> edges;

    ThingAdjacencyImpl(ThingVertex owner, Direction direction) {
        this.owner = owner;
        this.direction = direction;
        this.edges = new ConcurrentHashMap<>();
    }

    InfixIID.Thing infixIID(Schema.Edge.Thing schema) {
        return infixIID(schema, null);
    }

    InfixIID.Thing infixIID(Schema.Edge.Thing schema, VertexIID.Type metadata) {
        Schema.Infix infix = direction.isOut() ? schema.out() : schema.in();
        if (infix != null) return InfixIID.Thing.of(infix, metadata);
        return null;
    }

    abstract ThingIteratorBuilderImpl edge(InfixIID.Thing infix);

    abstract ThingEdge edge(InfixIID.Thing infix, ThingVertex adjacent);

    abstract void delete(InfixIID.Thing infix, ThingVertex adjacent);

    abstract void delete(InfixIID.Thing infix);

    @Override
    public ThingIteratorBuilderImpl edge(Schema.Edge.Thing schema, VertexIID.Type metadata) {
        return edge(infixIID(schema, metadata));
    }

    @Override
    public ThingIteratorBuilderImpl edge(Schema.Edge.Thing schema) {
        return edge(infixIID(schema));
    }

    @Override
    public ThingEdge edge(Schema.Edge.Thing schema, VertexIID.Type metadata, ThingVertex adjacent) {
        return edge(infixIID(schema, metadata), adjacent);
    }

    @Override
    public ThingEdge edge(Schema.Edge.Thing schema, ThingVertex adjacent) {
        return edge(infixIID(schema), adjacent);
    }

    @Override
    public void delete(Schema.Edge.Thing schema, VertexIID.Type metadata, ThingVertex adjacent) {
        delete(infixIID(schema, metadata), adjacent);
    }

    @Override
    public void delete(Schema.Edge.Thing schema, VertexIID.Type metadata) {
        delete(infixIID(schema, metadata));
    }

    @Override
    public void delete(Schema.Edge.Thing schema, ThingVertex adjacent) {
        delete(infixIID(schema), adjacent);
    }

    @Override
    public void delete(Schema.Edge.Thing schema) {
        delete(infixIID(schema));
    }

    @Override
    public void deleteNonRecursive(ThingEdge edge) {
        InfixIID.Thing infix = infixIID(edge.schema());
        if (edges.containsKey(infix)) {
            edges.get(infix).remove(edge);
            owner.setModified();
        }
    }

    @Override
    public void put(Schema.Edge.Thing schema, ThingVertex adjacent) {
        put(schema, null, adjacent);
    }

    public void put(Schema.Edge.Thing schema, VertexIID.Type metadata, ThingVertex adjacent) {
        ThingVertex from = direction.isOut() ? owner : adjacent;
        ThingVertex to = direction.isOut() ? adjacent : owner;
        ThingEdge edge = new ThingEdgeImpl.Buffered(from, schema, metadata, to);
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
        ThingIteratorBuilderImpl edge(InfixIID.Thing infix) {
            Set<ThingEdge> edges;

            if (infix.isOptimisation() && (edges = this.edges.get(infix)) != null) {
                return new ThingIteratorBuilderImpl(edges.iterator());
            } else if (!infix.isOptimisation() && (edges = this.edges.get(infix.withoutMetaData())) != null) { // TODO: Look up by infix.withoutMetaData()
                if (!infix.hasMetaData()) return new ThingIteratorBuilderImpl(edges.iterator());
                else return new ThingIteratorBuilderImpl(filter(edges.iterator(), e -> e.to().type().iid().equals(infix.metadata())));
            } else {
                return new ThingIteratorBuilderImpl(Collections.emptyIterator());
            }
        }

        @Override
        ThingEdge edge(InfixIID.Thing infix, ThingVertex adjacent) {
            if (edges.containsKey(infix)) {
                Predicate<ThingEdge> predicate = direction.isOut()
                        ? e -> e.to().equals(adjacent)
                        : e -> e.from().equals(adjacent);
                return edges.get(infix).stream().filter(predicate).findAny().orElse(null);
            }
            return null;
        }

        @Override
        void delete(InfixIID.Thing infix, ThingVertex adjacent) {
            if (edges.containsKey(infix)) {
                Predicate<ThingEdge> predicate = direction.isOut()
                        ? e -> e.to().equals(adjacent)
                        : e -> e.from().equals(adjacent);
                edges.get(infix).stream().filter(predicate).forEach(Edge::delete);
            }
        }

        @Override
        public void delete(InfixIID.Thing infix) {
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

        private Iterator<ThingEdge> edgeIterator(InfixIID.Thing infix) {
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
        ThingIteratorBuilderImpl edge(InfixIID.Thing infix) {
            return new ThingIteratorBuilderImpl(edgeIterator(infix));
        }

        @Override
        ThingEdge edge(InfixIID.Thing infix, ThingVertex adjacent) {
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
        void delete(InfixIID.Thing infix, ThingVertex adjacent) {
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
        void delete(InfixIID.Thing infix) {
            if (edges.containsKey(infix)) edges.get(infix).parallelStream().forEach(Edge::delete);
            Iterator<ThingEdge> storageIterator = owner.graph().storage().iterate(
                    join(owner.iid().bytes(), infix.bytes()),
                    (key, value) -> new ThingEdgeImpl.Persisted(owner.graph(), EdgeIID.Thing.of(key))
            );
            storageIterator.forEachRemaining(Edge::delete);
        }

        @Override
        public void forEach(Consumer<ThingEdge> function) {
            for (Schema.Edge.Thing schema : Schema.Edge.Thing.values()) {
                InfixIID.Thing infix = infixIID(schema);
                if (infix != null) edgeIterator(infix).forEachRemaining(function);
            }
        }
    }
}
