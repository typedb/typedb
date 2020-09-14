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

package grakn.core.graph.adjacency.impl;

import grakn.core.graph.adjacency.SchemaAdjacency;
import grakn.core.graph.edge.Edge;
import grakn.core.graph.edge.SchemaEdge;
import grakn.core.graph.edge.impl.SchemaEdgeImpl;
import grakn.core.graph.iid.EdgeIID;
import grakn.core.graph.iid.VertexIID;
import grakn.core.graph.util.Encoding;
import grakn.core.graph.vertex.RuleVertex;
import grakn.core.graph.vertex.SchemaVertex;
import grakn.core.graph.vertex.TypeVertex;

import java.util.Collections;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static grakn.core.common.collection.Bytes.join;
import static grakn.core.common.iterator.Iterators.apply;
import static grakn.core.common.iterator.Iterators.distinct;
import static grakn.core.common.iterator.Iterators.link;

public abstract class SchemaAdjacencyImpl implements SchemaAdjacency {

    final SchemaVertex<?, ?> owner;
    final Encoding.Direction direction;
    final ConcurrentMap<Encoding.Edge.Schema, Set<SchemaEdge>> edges;

    SchemaAdjacencyImpl(SchemaVertex<?, ?> owner, Encoding.Direction direction) {
        this.owner = owner;
        this.direction = direction;
        this.edges = new ConcurrentHashMap<>();
    }

    private void putNonRecursive(SchemaEdge edge) {
        loadToBuffer(edge);
        owner.setModified();
    }

    @Override
    public SchemaEdge put(Encoding.Edge.Schema encoding, SchemaVertex<?, ?> adjacent) {
        SchemaVertex<?, ?> from = direction.isOut() ? owner : adjacent;
        SchemaVertex<?, ?> to = direction.isOut() ? adjacent : owner;
        SchemaEdgeImpl edge = new SchemaEdgeImpl.Buffered(encoding, from, to);
        edges.computeIfAbsent(encoding, e -> ConcurrentHashMap.newKeySet()).add(edge);
        if (direction.isOut()) ((SchemaAdjacencyImpl) to.ins()).putNonRecursive(edge);
        else ((SchemaAdjacencyImpl) from.outs()).putNonRecursive(edge);
        owner.setModified();
        return edge;
    }

    @Override
    public void loadToBuffer(SchemaEdge edge) {
        edges.computeIfAbsent(edge.encoding(), e -> ConcurrentHashMap.newKeySet()).add(edge);
    }

    @Override
    public void removeFromBuffer(SchemaEdge edge) {
        if (edges.containsKey(edge.encoding())) {
            edges.get(edge.encoding()).remove(edge);
            owner.setModified();
        }
    }

    @Override
    public void deleteAll() {
        for (Encoding.Edge.Type encoding : Encoding.Edge.Type.values()) delete(encoding);
        for (Encoding.Edge.Rule encoding : Encoding.Edge.Rule.values()) delete(encoding);
    }

    @Override
    public void commit() {
        edges.values().forEach(set -> set.forEach(Edge::commit));
    }

    /**
     * When used in combination with purely retrieving type edges (by infix encoding),
     * this iterator builder performs safe vertex downcasts at both ends of the edge
     */
    static class TypeIteratorBuilderImpl implements SchemaAdjacency.TypeIteratorBuilder {
        private final Iterator<SchemaEdge> edgeIterator;

        TypeIteratorBuilderImpl(Iterator<SchemaEdge> edgeIterator) {
            this.edgeIterator = edgeIterator;
        }

        @Override
        public Iterator<TypeVertex> from() {
            return apply(edgeIterator, edge -> edge.from().asType());
        }

        @Override
        public Iterator<TypeVertex> to() {
            return apply(edgeIterator, edge -> edge.to().asType());
        }

        @Override
        public Iterator<TypeVertex> overridden() {
            return apply(edgeIterator, SchemaEdge::overridden);
        }
    }

    /**
     * When used in combination with purely retrieving type edges (by infix encoding),
     * this iterator builder performs safe vertex downcasts at both ends of the edge
     *
     * We define the 'from'/start of a Rule edge to always start at the rule
     * and define the 'to'/end of a Rule edge to always end at a Type
     */
    static class RuleIteratorBuilderImpl implements SchemaAdjacency.RuleIteratorBuilder {
        private final Iterator<SchemaEdge> edgeIterator;

        RuleIteratorBuilderImpl(Iterator<SchemaEdge> edgeIterator) {
            this.edgeIterator = edgeIterator;
        }

        @Override
        public Iterator<RuleVertex> from() {
            return apply(edgeIterator, edge -> edge.from().asRule());
        }

        @Override
        public Iterator<TypeVertex> to() {
            return apply(edgeIterator, edge -> edge.to().asType());
        }
    }

    public static class Buffered extends SchemaAdjacencyImpl implements SchemaAdjacency {

        public Buffered(SchemaVertex<?, ?> owner, Encoding.Direction direction) {
            super(owner, direction);
        }

        @Override
        public TypeIteratorBuilder edge(Encoding.Edge.Type encoding) {
            Set<SchemaEdge> t = edges.get(encoding);
            if (t != null) return new TypeIteratorBuilderImpl(t.iterator());
            return new TypeIteratorBuilderImpl(Collections.emptyIterator());
        }

        @Override
        public RuleIteratorBuilderImpl edge(Encoding.Edge.Rule encoding) {
            Set<SchemaEdge> t = edges.get(encoding);
            if (t != null) return new RuleIteratorBuilderImpl(t.iterator());
            return new RuleIteratorBuilderImpl(Collections.emptyIterator());
        }

        @Override
        public SchemaEdge edge(Encoding.Edge.Type encoding, TypeVertex adjacent) {
            if (edges.containsKey(encoding)) {
                Predicate<SchemaEdge> predicate = direction.isOut()
                        ? e -> e.to().equals(adjacent)
                        : e -> e.from().equals(adjacent);
                return edges.get(encoding).stream().filter(predicate).findAny().orElse(null);
            }
            return null;
        }

        @Override
        public void delete(Encoding.Edge.Schema encoding) {
            if (edges.containsKey(encoding)) edges.get(encoding).forEach(Edge::delete);
        }
    }

    public static class Persisted extends SchemaAdjacencyImpl implements SchemaAdjacency {

        public Persisted(SchemaVertex<?, ?> owner, Encoding.Direction direction) {
            super(owner, direction);
        }

        private byte[] edgeIID(Encoding.Edge.Schema encoding, SchemaVertex<?, ?> adjacent) {
            return join(owner.iid().bytes(),
                        direction.isOut() ? encoding.out().bytes() : encoding.in().bytes(),
                        adjacent.iid().bytes());
        }

        private SchemaEdge newPersistedEdge(byte[] key, byte[] value) {
            VertexIID.Type overridden = ((value.length == 0) ? null : VertexIID.Type.of(value));
            return new SchemaEdgeImpl.Persisted(owner.graph(), EdgeIID.Schema.of(key), overridden);
        }

        private Iterator<SchemaEdge> edgeIterator(Encoding.Edge.Schema encoding) {
            Iterator<SchemaEdge> storageIterator = owner.graph().storage().iterate(
                    join(owner.iid().bytes(), direction.isOut() ? encoding.out().bytes() : encoding.in().bytes()),
                    this::newPersistedEdge
            );

            if (edges.get(encoding) == null) {
                return storageIterator;
            } else {
                return distinct(link(edges.get(encoding).iterator(), storageIterator));
            }
        }

        @Override
        public TypeIteratorBuilder edge(Encoding.Edge.Type encoding) {
            return new TypeIteratorBuilderImpl(edgeIterator(encoding));
        }

        @Override
        public RuleIteratorBuilder edge(Encoding.Edge.Rule encoding) {
            return new RuleIteratorBuilderImpl(edgeIterator(encoding));
        }

        @Override
        public SchemaEdge edge(Encoding.Edge.Type encoding, TypeVertex adjacent) {
            Optional<SchemaEdge> container;
            Predicate<SchemaEdge> predicate = direction.isOut()
                    ? e -> e.to().equals(adjacent)
                    : e -> e.from().equals(adjacent);

            if (edges.containsKey(encoding) && (container = edges.get(encoding).stream().filter(predicate).findAny()).isPresent()) {
                return container.get();
            } else {
                byte[] edgeIID = edgeIID(encoding, adjacent);
                byte[] overriddenIID;
                if ((overriddenIID = owner.graph().storage().get(edgeIID)) != null) {
                    return newPersistedEdge(edgeIID, overriddenIID);
                }
            }

            return null;
        }

        public void delete(Encoding.Edge.Schema encoding) {
            if (edges.containsKey(encoding)) edges.get(encoding).parallelStream().forEach(Edge::delete);
            Iterator<SchemaEdge> storageIterator = owner.graph().storage().iterate(
                    join(owner.iid().bytes(), direction.isOut() ? encoding.out().bytes() : encoding.in().bytes()),
                    this::newPersistedEdge
            );
            storageIterator.forEachRemaining(Edge::delete);
        }

        public void deleteAll() {
            for (Encoding.Edge.Type type : Encoding.Edge.Type.values()) delete(type);
            for (Encoding.Edge.Rule rule : Encoding.Edge.Rule.values()) delete(rule);
        }
    }
}
