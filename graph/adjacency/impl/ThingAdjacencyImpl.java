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
import hypergraph.graph.iid.IID;
import hypergraph.graph.iid.InfixIID;
import hypergraph.graph.iid.SuffixIID;
import hypergraph.graph.util.Schema;
import hypergraph.graph.vertex.ThingVertex;

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static hypergraph.common.collection.Bytes.join;
import static hypergraph.common.iterator.Iterators.apply;
import static hypergraph.common.iterator.Iterators.distinct;
import static hypergraph.common.iterator.Iterators.link;
import static java.util.Arrays.copyOfRange;
import static java.util.Collections.emptyIterator;
import static java.util.concurrent.ConcurrentHashMap.newKeySet;

public abstract class ThingAdjacencyImpl implements ThingAdjacency {

    final ThingVertex owner;
    final Direction direction;
    final ConcurrentMap<InfixIID.Thing, Set<InfixIID.Thing>> infixes;
    final ConcurrentMap<InfixIID.Thing, Set<ThingEdge>> edges;

    ThingAdjacencyImpl(ThingVertex owner, Direction direction) {
        this.owner = owner;
        this.direction = direction;
        this.infixes = new ConcurrentHashMap<>();
        this.edges = new ConcurrentHashMap<>();
    }

    InfixIID.Thing infixIID(Schema.Edge.Thing schema, IID... lookAhead) {
        Schema.Infix infix = direction.isOut() ? schema.out() : schema.in();
        return InfixIID.Thing.of(infix, lookAhead);
    }

    IID[] infixTails(ThingEdge edge) {
        if (edge.schema().isOptimisation()) {
            if (direction.isOut()) {
                return new IID[]{edge.outIID().infix().asRolePlayer().tail(), edge.to().iid().prefix(), edge.to().iid().type()};
            } else {
                return new IID[]{edge.inIID().infix().asRolePlayer().tail(), edge.from().iid().prefix(), edge.from().iid().type()};
            }
        } else {
            if (direction.isOut()) return new IID[]{edge.to().iid().prefix(), edge.to().iid().type()};
            else return new IID[]{edge.from().iid().prefix(), edge.from().iid().type()};
        }
    }

    Iterator<ThingEdge> bufferedEdgeIterator(Schema.Edge.Thing schema, IID[] lookAhead) {
        Set<ThingEdge> result;
        InfixIID.Thing infixIID = infixIID(schema, lookAhead);
        if (lookAhead.length == schema.lookAhead()) {
            return (result = edges.get(infixIID)) != null ? result.iterator() : emptyIterator();
        }

        assert lookAhead.length < schema.lookAhead();
        Set<InfixIID.Thing> iids = new HashSet<>();
        iids.add(infixIID);
        for (int i = lookAhead.length; i < schema.lookAhead() && !iids.isEmpty(); i++) {
            Set<InfixIID.Thing> newIIDs = new HashSet<>();
            for (InfixIID.Thing iid : iids) {
                Set<InfixIID.Thing> someNewIIDs = infixes.get(iid);
                if (someNewIIDs != null) newIIDs.addAll(someNewIIDs);
            }
            iids = newIIDs;
        }

        List<Iterator<ThingEdge>> iterators = new LinkedList<>();
        iids.forEach(iid -> iterators.add(edges.get(iid).iterator()));
        return link(iterators);
    }

    @Override
    public ThingEdge edge(Schema.Edge.Thing schema, ThingVertex adjacent, ThingVertex optimised) {
        assert schema.isOptimisation();
        Predicate<ThingEdge> predicate = direction.isOut()
                ? e -> e.to().equals(adjacent) && e.outIID().suffix().equals(SuffixIID.of(optimised.iid().key()))
                : e -> e.from().equals(adjacent) && e.inIID().suffix().equals(SuffixIID.of(optimised.iid().key()));
        Iterator<ThingEdge> iterator = bufferedEdgeIterator(
                schema, new IID[]{optimised.iid().type(), adjacent.iid().prefix(), adjacent.iid().type()}
        );
        ThingEdge edge = null;
        while (iterator.hasNext()) {
            if (predicate.test(edge = iterator.next())) break;
            else edge = null;
        }
        return edge;
    }

    @Override
    public ThingEdge edge(Schema.Edge.Thing schema, ThingVertex adjacent) {
        assert !schema.isOptimisation();
        Predicate<ThingEdge> predicate = direction.isOut() ? e -> e.to().equals(adjacent) : e -> e.from().equals(adjacent);
        Iterator<ThingEdge> iterator = bufferedEdgeIterator(schema, new IID[]{adjacent.iid().prefix(), adjacent.iid().type()});
        ThingEdge edge = null;
        while (iterator.hasNext()) {
            if (predicate.test(edge = iterator.next())) break;
            else edge = null;
        }
        return edge;
    }

    private void put(Schema.Edge.Thing schema, ThingEdge edge, IID[] infixes, boolean isModified, boolean recurse) {
        assert schema.lookAhead() == infixes.length;
        InfixIID.Thing infixIID = infixIID(schema);
        for (int i = 0; i < schema.lookAhead(); i++) {
            this.infixes.computeIfAbsent(infixIID, x -> newKeySet()).add(infixIID = infixIID(schema, copyOfRange(infixes, 0, i + 1)));
        }
        edges.computeIfAbsent(infixIID, iid -> newKeySet()).add(edge);
        if (isModified) owner.isModified();
        if (recurse) {
            if (direction.isOut()) ((ThingAdjacencyImpl) edge.to().ins()).putNonRecursive(edge);
            else ((ThingAdjacencyImpl) edge.from().outs()).putNonRecursive(edge);
        }
    }

    @Override
    public ThingEdgeImpl put(Schema.Edge.Thing schema, ThingVertex adjacent) {
        assert !schema.isOptimisation();
        ThingEdgeImpl edge = direction.isOut()
                ? new ThingEdgeImpl.Buffered(schema, owner, adjacent)
                : new ThingEdgeImpl.Buffered(schema, adjacent, owner);
        IID[] infixes = new IID[]{adjacent.iid().prefix(), adjacent.iid().type()};
        put(schema, edge, infixes, true, true);
        return edge;
    }

    public void put(Schema.Edge.Thing schema, ThingVertex adjacent, ThingVertex optimised) {
        assert schema.isOptimisation();
        ThingEdge edge = direction.isOut()
                ? new ThingEdgeImpl.Buffered(schema, owner, adjacent, optimised)
                : new ThingEdgeImpl.Buffered(schema, adjacent, owner, optimised);
        IID[] infixes = new IID[]{optimised.iid().type(), adjacent.iid().prefix(), adjacent.iid().type()};
        put(schema, edge, infixes, true, true);
    }

    public void putNonRecursive(ThingEdge edge) {
        put(edge.schema(), edge, infixTails(edge), true, false);
    }

    @Override
    public void loadToBuffer(ThingEdge edge) {
        put(edge.schema(), edge, infixTails(edge), false, false);
    }

    @Override
    public void removeFromBuffer(ThingEdge edge) {
        InfixIID.Thing infixIID = infixIID(edge.schema(), infixTails(edge));
        if (edges.containsKey(infixIID)) {
            edges.get(infixIID).remove(edge);
            owner.setModified();
        }
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
        public ThingIteratorBuilderImpl edge(Schema.Edge.Thing schema) {
            return new ThingIteratorBuilderImpl(bufferedEdgeIterator(schema, new IID[]{}));
        }

        @Override
        public ThingIteratorBuilderImpl edge(Schema.Edge.Thing schema, IID... lookAhead) {
            return new ThingIteratorBuilderImpl(bufferedEdgeIterator(schema, lookAhead));
        }

        @Override
        public void delete(Schema.Edge.Thing schema) {
            bufferedEdgeIterator(schema, new IID[0]).forEachRemaining(Edge::delete);
        }

        @Override
        public void delete(Schema.Edge.Thing schema, IID[] lookAhead) {
            bufferedEdgeIterator(schema, lookAhead).forEachRemaining(Edge::delete);
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

        private Iterator<ThingEdge> edgeIterator(Schema.Edge.Thing schema, IID... lookahead) {
            Iterator<ThingEdge> storageIterator = owner.graph().storage().iterate(
                    join(owner.iid().bytes(), infixIID(schema, lookahead).bytes()),
                    (key, value) -> new ThingEdgeImpl.Persisted(owner.graph(), EdgeIID.Thing.of(key))
            );

            Iterator<ThingEdge> bufferedIterator = bufferedEdgeIterator(schema, lookahead);
            if (!bufferedIterator.hasNext()) return storageIterator;
            else return distinct(link(bufferedIterator, storageIterator));
        }

        @Override
        public ThingIteratorBuilderImpl edge(Schema.Edge.Thing schema) {
            return new ThingIteratorBuilderImpl(edgeIterator(schema));
        }

        @Override
        public IteratorBuilder<ThingVertex> edge(Schema.Edge.Thing schema, IID... lookAhead) {
            return new ThingIteratorBuilderImpl(edgeIterator(schema, lookAhead));
        }

        @Override
        public ThingEdge edge(Schema.Edge.Thing schema, ThingVertex adjacent) {
            assert !schema.isOptimisation();
            ThingEdge edge = super.edge(schema, adjacent);
            if (edge != null) return edge;

            EdgeIID.Thing edgeIID = EdgeIID.Thing.of(owner.iid(), infixIID(schema), adjacent.iid());
            if (owner.graph().storage().get(edgeIID.bytes()) == null) return null;
            else return new ThingEdgeImpl.Persisted(owner.graph(), edgeIID);
        }

        @Override
        public ThingEdge edge(Schema.Edge.Thing schema, ThingVertex adjacent, ThingVertex optimised) {
            assert schema.isOptimisation();
            ThingEdge edge = super.edge(schema, adjacent, optimised);
            if (edge != null) return edge;

            EdgeIID.Thing edgeIID = EdgeIID.Thing.of(owner.iid(), infixIID(schema, optimised.iid().type()),
                                                     adjacent.iid(), SuffixIID.of(optimised.iid().key()));
            if (owner.graph().storage().get(edgeIID.bytes()) == null) return null;
            else return new ThingEdgeImpl.Persisted(owner.graph(), edgeIID);
        }

        @Override
        public void delete(Schema.Edge.Thing schema) {
            edgeIterator(schema).forEachRemaining(Edge::delete);
        }

        @Override
        public void delete(Schema.Edge.Thing schema, IID[] lookAhead) {
            edgeIterator(schema, lookAhead).forEachRemaining(Edge::delete);
        }

        @Override
        public void forEach(Consumer<ThingEdge> function) {
            for (Schema.Edge.Thing schema : Schema.Edge.Thing.values()) {
                edgeIterator(schema).forEachRemaining(function);
            }
        }
    }
}
