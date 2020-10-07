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

import grakn.core.common.iterator.ResourceIterator;
import grakn.core.graph.adjacency.ThingAdjacency;
import grakn.core.graph.edge.Edge;
import grakn.core.graph.edge.ThingEdge;
import grakn.core.graph.edge.impl.ThingEdgeImpl;
import grakn.core.graph.iid.EdgeIID;
import grakn.core.graph.iid.IID;
import grakn.core.graph.iid.InfixIID;
import grakn.core.graph.iid.SuffixIID;
import grakn.core.graph.util.Encoding;
import grakn.core.graph.vertex.ThingVertex;

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Predicate;

import static grakn.core.common.collection.Bytes.join;
import static grakn.core.common.iterator.Iterators.base;
import static grakn.core.common.iterator.Iterators.link;
import static java.util.Arrays.copyOfRange;
import static java.util.Collections.emptyIterator;
import static java.util.concurrent.ConcurrentHashMap.newKeySet;

public abstract class ThingAdjacencyImpl implements ThingAdjacency {

    final ThingVertex owner;
    final Encoding.Direction direction;
    final ConcurrentMap<InfixIID.Thing, Set<InfixIID.Thing>> infixes;
    final ConcurrentMap<InfixIID.Thing, Set<ThingEdge>> edges;

    ThingAdjacencyImpl(final ThingVertex owner, final Encoding.Direction direction) {
        this.owner = owner;
        this.direction = direction;
        this.infixes = new ConcurrentHashMap<>();
        this.edges = new ConcurrentHashMap<>();
    }

    InfixIID.Thing infixIID(final Encoding.Edge.Thing encoding, final IID... lookAhead) {
        final Encoding.Infix infix = direction.isOut() ? encoding.out() : encoding.in();
        return InfixIID.Thing.of(infix, lookAhead);
    }

    IID[] infixTails(final ThingEdge edge) {
        if (edge.encoding().isOptimisation()) {
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

    ResourceIterator<ThingEdge> bufferedEdgeIterator(final Encoding.Edge.Thing encoding, final IID[] lookAhead) {
        final Set<ThingEdge> result;
        final InfixIID.Thing infixIID = infixIID(encoding, lookAhead);
        if (lookAhead.length == encoding.lookAhead()) {
            return base((result = edges.get(infixIID)) != null ? result.iterator() : emptyIterator());
        }

        assert lookAhead.length < encoding.lookAhead();
        Set<InfixIID.Thing> iids = new HashSet<>();
        iids.add(infixIID);
        for (int i = lookAhead.length; i < encoding.lookAhead() && !iids.isEmpty(); i++) {
            final Set<InfixIID.Thing> newIIDs = new HashSet<>();
            for (InfixIID.Thing iid : iids) {
                final Set<InfixIID.Thing> someNewIIDs = infixes.get(iid);
                if (someNewIIDs != null) newIIDs.addAll(someNewIIDs);
            }
            iids = newIIDs;
        }

        final List<Iterator<ThingEdge>> iterators = new LinkedList<>();
        iids.forEach(iid -> iterators.add(edges.get(iid).iterator()));
        return link(iterators);
    }

    @Override
    public ThingEdge edge(final Encoding.Edge.Thing encoding, final ThingVertex adjacent, final ThingVertex optimised) {
        assert encoding.isOptimisation();
        final Predicate<ThingEdge> predicate = direction.isOut()
                ? e -> e.to().equals(adjacent) && e.outIID().suffix().equals(SuffixIID.of(optimised.iid().key()))
                : e -> e.from().equals(adjacent) && e.inIID().suffix().equals(SuffixIID.of(optimised.iid().key()));
        final ResourceIterator<ThingEdge> iterator = bufferedEdgeIterator(
                encoding, new IID[]{optimised.iid().type(), adjacent.iid().prefix(), adjacent.iid().type()}
        );
        ThingEdge edge = null;
        while (iterator.hasNext()) {
            if (predicate.test(edge = iterator.next())) break;
            else edge = null;
        }
        iterator.recycle();
        return edge;
    }

    @Override
    public ThingEdge edge(final Encoding.Edge.Thing encoding, final ThingVertex adjacent) {
        assert !encoding.isOptimisation();
        final Predicate<ThingEdge> predicate = direction.isOut() ? e -> e.to().equals(adjacent) : e -> e.from().equals(adjacent);
        final ResourceIterator<ThingEdge> iterator = bufferedEdgeIterator(encoding, new IID[]{adjacent.iid().prefix(), adjacent.iid().type()});
        ThingEdge edge = null;
        while (iterator.hasNext()) {
            if (predicate.test(edge = iterator.next())) break;
            else edge = null;
        }
        iterator.recycle();
        return edge;
    }

    private ThingEdgeImpl put(final Encoding.Edge.Thing encoding, final ThingEdgeImpl edge, final IID[] infixes, final boolean isModified, final boolean recurse) {
        assert encoding.lookAhead() == infixes.length;
        InfixIID.Thing infixIID = infixIID(encoding);
        for (int i = 0; i < encoding.lookAhead(); i++) {
            this.infixes.computeIfAbsent(infixIID, x -> newKeySet()).add(infixIID = infixIID(encoding, copyOfRange(infixes, 0, i + 1)));
        }
        edges.computeIfAbsent(infixIID, iid -> newKeySet()).add(edge);
        if (isModified) owner.isModified();
        if (recurse) {
            if (direction.isOut()) ((ThingAdjacencyImpl) edge.to().ins()).putNonRecursive(edge);
            else ((ThingAdjacencyImpl) edge.from().outs()).putNonRecursive(edge);
        }
        return edge;
    }

    @Override
    public ThingEdgeImpl put(final Encoding.Edge.Thing encoding, final ThingVertex adjacent) {
        assert !encoding.isOptimisation();
        final ThingEdgeImpl edge = direction.isOut()
                ? new ThingEdgeImpl.Buffered(encoding, owner, adjacent)
                : new ThingEdgeImpl.Buffered(encoding, adjacent, owner);
        final IID[] infixes = new IID[]{adjacent.iid().prefix(), adjacent.iid().type()};
        return put(encoding, edge, infixes, true, true);
    }

    @Override
    public ThingEdge put(final Encoding.Edge.Thing encoding, final ThingVertex adjacent, final ThingVertex optimised) {
        assert encoding.isOptimisation();
        final ThingEdgeImpl edge = direction.isOut()
                ? new ThingEdgeImpl.Buffered(encoding, owner, adjacent, optimised)
                : new ThingEdgeImpl.Buffered(encoding, adjacent, owner, optimised);
        final IID[] infixes = new IID[]{optimised.iid().type(), adjacent.iid().prefix(), adjacent.iid().type()};
        return put(encoding, edge, infixes, true, true);
    }

    private void putNonRecursive(final ThingEdgeImpl edge) {
        put(edge.encoding(), edge, infixTails(edge), true, false);
    }

    @Override
    public ThingEdge cache(final ThingEdge edge) {
        return put(edge.encoding(), (ThingEdgeImpl) edge, infixTails(edge), false, false);
    }

    @Override
    public void remove(final ThingEdge edge) {
        final InfixIID.Thing infixIID = infixIID(edge.encoding(), infixTails(edge));
        if (edges.containsKey(infixIID)) {
            edges.get(infixIID).remove(edge);
            owner.setModified();
        }
    }

    @Override
    public void deleteAll() {
        for (Encoding.Edge.Thing encoding : Encoding.Edge.Thing.values()) delete(encoding);
    }

    @Override
    public void commit() {
        edges.values().forEach(edges -> edges.forEach(Edge::commit));
    }

    static class ThingIteratorBuilderImpl implements ThingIteratorBuilder {

        private final ResourceIterator<ThingEdge> edgeIterator;

        ThingIteratorBuilderImpl(final ResourceIterator<ThingEdge> edgeIterator) {
            this.edgeIterator = edgeIterator;
        }

        @Override
        public ResourceIterator<ThingVertex> to() {
            return edgeIterator.map(Edge::to);
        }

        @Override
        public ResourceIterator<ThingVertex> from() {
            return edgeIterator.map(Edge::from);
        }
    }

    public static class Buffered extends ThingAdjacencyImpl implements ThingAdjacency {

        public Buffered(final ThingVertex owner, final Encoding.Direction direction) {
            super(owner, direction);
        }

        @Override
        public ThingIteratorBuilderImpl edge(final Encoding.Edge.Thing encoding) {
            return new ThingIteratorBuilderImpl(bufferedEdgeIterator(encoding, new IID[]{}));
        }

        @Override
        public ThingIteratorBuilderImpl edge(final Encoding.Edge.Thing encoding, final IID... lookAhead) {
            return new ThingIteratorBuilderImpl(bufferedEdgeIterator(encoding, lookAhead));
        }

        @Override
        public void delete(final Encoding.Edge.Thing encoding) {
            bufferedEdgeIterator(encoding, new IID[0]).forEachRemaining(Edge::delete);
        }

        @Override
        public void delete(final Encoding.Edge.Thing encoding, final IID... lookAhead) {
            bufferedEdgeIterator(encoding, lookAhead).forEachRemaining(Edge::delete);
        }
    }

    public static class Persisted extends ThingAdjacencyImpl implements ThingAdjacency {

        private final ConcurrentHashMap<byte[], ResourceIterator<ThingEdge>> storageIterators;

        public Persisted(final ThingVertex owner, final Encoding.Direction direction) {
            super(owner, direction);
            storageIterators = new ConcurrentHashMap<>();
        }

        private ResourceIterator<ThingEdge> edgeIterator(final Encoding.Edge.Thing encoding, final IID... lookahead) {
            final ResourceIterator<ThingEdge> storageIterator = storageIterators.computeIfAbsent(
                    join(owner.iid().bytes(), infixIID(encoding, lookahead).bytes()),
                    iid -> owner.graph().storage().iterate(iid, (key, value) ->
                            cache(new ThingEdgeImpl.Persisted(owner.graph(), EdgeIID.Thing.of(key))))
            );

            final ResourceIterator<ThingEdge> bufferedIterator = bufferedEdgeIterator(encoding, lookahead);
            if (!bufferedIterator.hasNext()) return storageIterator;
            else if (!storageIterator.hasNext()) return bufferedIterator;
            else return link(bufferedIterator, storageIterator).distinct();
        }

        @Override
        public ThingIteratorBuilderImpl edge(final Encoding.Edge.Thing encoding) {
            return new ThingIteratorBuilderImpl(edgeIterator(encoding));
        }

        @Override
        public ThingIteratorBuilder edge(final Encoding.Edge.Thing encoding, final IID... lookAhead) {
            return new ThingIteratorBuilderImpl(edgeIterator(encoding, lookAhead));
        }

        @Override
        public ThingEdge edge(final Encoding.Edge.Thing encoding, final ThingVertex adjacent) {
            assert !encoding.isOptimisation();
            final ThingEdge edge = super.edge(encoding, adjacent);
            if (edge != null) return edge;

            final EdgeIID.Thing edgeIID = EdgeIID.Thing.of(owner.iid(), infixIID(encoding), adjacent.iid());
            if (owner.graph().storage().get(edgeIID.bytes()) == null) return null;
            else return cache(new ThingEdgeImpl.Persisted(owner.graph(), edgeIID));
        }

        @Override
        public ThingEdge edge(final Encoding.Edge.Thing encoding, final ThingVertex adjacent, final ThingVertex optimised) {
            assert encoding.isOptimisation();
            final ThingEdge edge = super.edge(encoding, adjacent, optimised);
            if (edge != null) return edge;

            final EdgeIID.Thing edgeIID = EdgeIID.Thing.of(
                    owner.iid(), infixIID(encoding, optimised.iid().type()),
                    adjacent.iid(), SuffixIID.of(optimised.iid().key())
            );
            if (owner.graph().storage().get(edgeIID.bytes()) == null) return null;
            else return cache(new ThingEdgeImpl.Persisted(owner.graph(), edgeIID));
        }

        @Override
        public void delete(final Encoding.Edge.Thing encoding) {
            edgeIterator(encoding).forEachRemaining(Edge::delete);
        }

        @Override
        public void delete(final Encoding.Edge.Thing encoding, final IID... lookAhead) {
            edgeIterator(encoding, lookAhead).forEachRemaining(Edge::delete);
        }
    }
}
