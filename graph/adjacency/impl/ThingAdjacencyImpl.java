/*
 * Copyright (C) 2021 Vaticle
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

package com.vaticle.typedb.core.graph.adjacency.impl;

import com.vaticle.typedb.common.collection.ConcurrentSet;
import com.vaticle.typedb.core.common.collection.ByteArray;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.graph.adjacency.ThingAdjacency;
import com.vaticle.typedb.core.graph.common.Encoding;
import com.vaticle.typedb.core.graph.edge.Edge;
import com.vaticle.typedb.core.graph.edge.ThingEdge;
import com.vaticle.typedb.core.graph.edge.impl.ThingEdgeImpl;
import com.vaticle.typedb.core.graph.iid.EdgeIID;
import com.vaticle.typedb.core.graph.iid.IID;
import com.vaticle.typedb.core.graph.iid.InfixIID;
import com.vaticle.typedb.core.graph.iid.SuffixIID;
import com.vaticle.typedb.core.graph.vertex.ThingVertex;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Predicate;

import static com.vaticle.typedb.core.common.collection.ByteArray.join;
import static com.vaticle.typedb.core.common.iterator.Iterators.emptySorted;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterateSorted;
import static com.vaticle.typedb.core.common.iterator.Iterators.link;
import static java.util.Arrays.copyOfRange;

public abstract class ThingAdjacencyImpl implements ThingAdjacency {

    final Encoding.Direction.Adjacency direction;

    ThingAdjacencyImpl(Encoding.Direction.Adjacency direction) {
        this.direction = direction;
    }

    InfixIID.Thing infixIID(Encoding.Edge.Thing encoding, IID... lookAhead) {
        Encoding.Infix infix = direction.isOut() ? encoding.out() : encoding.in();
        return InfixIID.Thing.of(infix, lookAhead);
    }

    @Override
    public EdgeDirected asSortable(ThingEdge edge) {
        return direction.isIn() ? EdgeDirected.in(edge) : EdgeDirected.out(edge);
    }

    static class ThingIteratorBuilderImpl implements ThingIteratorBuilder {

        private final FunctionalIterator<ThingEdge> edgeIterator;

        ThingIteratorBuilderImpl(FunctionalIterator<ThingEdge> edgeIterator) {
            this.edgeIterator = edgeIterator;
        }

        @Override
        public FunctionalIterator<ThingVertex> from() {
            return edgeIterator.map(Edge::from);
        }

        @Override
        public FunctionalIterator<ThingVertex> to() {
            return edgeIterator.map(Edge::to);
        }

        @Override
        public FunctionalIterator<ThingEdge> get() {
            return edgeIterator;
        }
    }

    static class ThingIteratorSortedBuilderImpl implements ThingIteratorSortedBuilder {

        private final FunctionalIterator.Sorted<EdgeDirected> sortableEdges;

        ThingIteratorSortedBuilderImpl(FunctionalIterator.Sorted<EdgeDirected> sortableEdges) {
            this.sortableEdges = sortableEdges;
        }

        @Override
        public FunctionalIterator<ThingVertex> from() {
            return sortableEdges.map(sortable -> sortable.getEdge().from());
        }

        @Override
        public FunctionalIterator<ThingVertex> to() {
            return sortableEdges.map(sortable -> sortable.getEdge().to());
        }

        @Override
        public FunctionalIterator.Sorted<EdgeDirected> get() {
            return sortableEdges;
        }
    }

    public static class Read extends ThingAdjacencyImpl {

        final ThingVertex owner;

        public Read(ThingVertex owner, Encoding.Direction.Adjacency direction) {
            super(direction);
            this.owner = owner;
        }

        private FunctionalIterator<ThingEdge> edgeIterator(Encoding.Edge.Thing encoding, IID... lookahead) {
            ByteArray iid = join(owner.iid().bytes(), infixIID(encoding, lookahead).bytes());
            return owner.graph().storage().iterate(iid, (key, value) -> key)
                    .map(key -> newPersistedEdge(EdgeIID.Thing.of(key)));
        }

        private FunctionalIterator.Sorted<EdgeDirected> edgeIteratorSorted(Encoding.Edge.Thing encoding, IID... lookahead) {
            ByteArray iid = join(owner.iid().bytes(), infixIID(encoding, lookahead).bytes());
            return owner.graph().storage().iterate(iid, (key, value) -> key).mapSorted(
                    key -> asSortable(newPersistedEdge(EdgeIID.Thing.of(key))),
                    sortable -> sortable.key.bytes()
            );
        }

        private ThingEdgeImpl.Persisted newPersistedEdge(EdgeIID.Thing of) {
            return new ThingEdgeImpl.Persisted(owner.graph(), of);
        }

        @Override
        public ThingIteratorBuilderImpl edge(Encoding.Edge.Thing encoding) {
            return new ThingIteratorBuilderImpl(edgeIterator(encoding));
        }

        @Override
        public ThingIteratorSortedBuilder edgeHas(IID... lookAhead) {
            return new ThingIteratorSortedBuilderImpl(edgeIteratorSorted(Encoding.Edge.Thing.HAS, lookAhead));
        }

        @Override
        public ThingIteratorSortedBuilder edgePlaying(IID... lookAhead) {
            return new ThingIteratorSortedBuilderImpl(edgeIteratorSorted(Encoding.Edge.Thing.PLAYING, lookAhead));
        }

        @Override
        public ThingIteratorSortedBuilder edgeRelating(IID... lookAhead) {
            return new ThingIteratorSortedBuilderImpl(edgeIteratorSorted(Encoding.Edge.Thing.RELATING, lookAhead));
        }

        @Override
        public ThingIteratorSortedBuilder edgeRolePlayer(IID roleType, IID... lookAhead) {
            IID[] merged = new IID[1 + lookAhead.length];
            merged[0] = roleType;
            System.arraycopy(lookAhead, 0, merged, 1, lookAhead.length);
            return new ThingIteratorSortedBuilderImpl(edgeIteratorSorted(Encoding.Edge.Thing.ROLEPLAYER, merged));
        }

        @Override
        public ThingEdge edge(Encoding.Edge.Thing encoding, ThingVertex adjacent) {
            EdgeIID.Thing edgeIID = EdgeIID.Thing.of(owner.iid(), infixIID(encoding), adjacent.iid());
            if (owner.graph().storage().get(edgeIID.bytes()) == null) return null;
            else return newPersistedEdge(edgeIID);
        }

        @Override
        public ThingEdge edge(Encoding.Edge.Thing encoding, ThingVertex adjacent, ThingVertex optimised) {
            EdgeIID.Thing edgeIID = EdgeIID.Thing.of(
                    owner.iid(), infixIID(encoding, optimised.iid().type()),
                    adjacent.iid(), SuffixIID.of(optimised.iid().key())
            );
            if (owner.graph().storage().get(edgeIID.bytes()) == null) return null;
            else return newPersistedEdge(edgeIID);
        }
    }

    public static abstract class Write extends ThingAdjacencyImpl implements ThingAdjacency.Write {

        final ThingVertex.Write owner;
        final ConcurrentMap<InfixIID.Thing, ConcurrentSet<InfixIID.Thing>> infixes;
        public final ConcurrentMap<InfixIID.Thing, ConcurrentNavigableMap<EdgeDirected, ThingEdge>> edges; // edges must be updateable

        Write(ThingVertex.Write owner, Encoding.Direction.Adjacency direction) {
            super(direction);
            this.owner = owner;
            this.infixes = new ConcurrentHashMap<>();
            this.edges = new ConcurrentHashMap<>();
        }

        InfixIID.Thing infixIID(Encoding.Edge.Thing encoding, IID... lookAhead) {
            Encoding.Infix infix = direction.isOut() ? encoding.out() : encoding.in();
            return InfixIID.Thing.of(infix, lookAhead);
        }

        IID[] infixTails(ThingEdge edge) {
            if (edge.encoding().isOptimisation()) {
                if (direction.isOut()) {
                    return new IID[]{edge.outIID().infix().asRolePlayer().tail(), edge.toIID().prefix(), edge.toIID().type()};
                } else {
                    return new IID[]{edge.inIID().infix().asRolePlayer().tail(), edge.fromIID().prefix(), edge.fromIID().type()};
                }
            } else {
                if (direction.isOut()) return new IID[]{edge.toIID().prefix(), edge.toIID().type()};
                else return new IID[]{edge.fromIID().prefix(), edge.fromIID().type()};
            }
        }

        FunctionalIterator.Sorted<EdgeDirected> bufferedEdgeIterator(Encoding.Edge.Thing encoding, IID[] lookAhead) {
            ConcurrentNavigableMap<EdgeDirected, ThingEdge> result;
            InfixIID.Thing infixIID = infixIID(encoding, lookAhead);
            if (lookAhead.length == encoding.lookAhead()) {
                return (result = edges.get(infixIID)) != null ? iterateSorted(result.keySet()) : emptySorted();
            }

            assert lookAhead.length < encoding.lookAhead();
            Set<InfixIID.Thing> iids = new HashSet<>();
            iids.add(infixIID);
            for (int i = lookAhead.length; i < encoding.lookAhead() && !iids.isEmpty(); i++) {
                Set<InfixIID.Thing> newIIDs = new HashSet<>();
                for (InfixIID.Thing iid : iids) {
                    Set<InfixIID.Thing> someNewIIDs = infixes.get(iid);
                    if (someNewIIDs != null) newIIDs.addAll(someNewIIDs);
                }
                iids = newIIDs;
            }

            return iterate(iids).flatMerge(iid -> {
                ConcurrentNavigableMap<EdgeDirected, ThingEdge> res;
                return (res = edges.get(iid)) != null ? iterateSorted(res.keySet()) : emptySorted();
            });
        }

        @Override
        public ThingEdge edge(Encoding.Edge.Thing encoding, ThingVertex adjacent, ThingVertex optimised) {
            assert encoding.isOptimisation();
            Predicate<ThingEdge> predicate = direction.isOut()
                    ? e -> e.to().equals(adjacent) && e.outIID().suffix().equals(SuffixIID.of(optimised.iid().key()))
                    : e -> e.from().equals(adjacent) && e.inIID().suffix().equals(SuffixIID.of(optimised.iid().key()));
            FunctionalIterator<EdgeDirected> iterator = bufferedEdgeIterator(
                    encoding, new IID[]{optimised.iid().type(), adjacent.iid().prefix(), adjacent.iid().type()}
            );
            ThingEdge edge = null;
            while (iterator.hasNext()) {
                if (predicate.test(edge = iterator.next().getEdge())) break;
                else edge = null;
            }
            iterator.recycle();
            return edge;
        }

        @Override
        public ThingEdge edge(Encoding.Edge.Thing encoding, ThingVertex adjacent) {
            assert !encoding.isOptimisation();
            Predicate<ThingEdge> predicate = direction.isOut() ? e -> e.to().equals(adjacent) : e -> e.from().equals(adjacent);
            FunctionalIterator<EdgeDirected> iterator = bufferedEdgeIterator(encoding, new IID[]{adjacent.iid().prefix(), adjacent.iid().type()});
            ThingEdge edge = null;
            while (iterator.hasNext()) {
                if (predicate.test(edge = iterator.next().getEdge())) break;
                else edge = null;
            }
            iterator.recycle();
            return edge;
        }

        private ThingEdgeImpl put(Encoding.Edge.Thing encoding, ThingEdgeImpl edge, IID[] infixes,
                                  boolean isModified, boolean isReflexive) {
            assert encoding.lookAhead() == infixes.length;
            InfixIID.Thing infixIID = infixIID(encoding);
            for (int i = 0; i < encoding.lookAhead(); i++) {
                this.infixes.computeIfAbsent(infixIID, x -> new ConcurrentSet<>()).add(
                        infixIID = infixIID(encoding, copyOfRange(infixes, 0, i + 1))
                );
            }

            edges.compute(infixIID, (iid, cachedEdges) -> {
                if (cachedEdges == null) cachedEdges = new ConcurrentSkipListMap<>();
                EdgeDirected sortableEdge = asSortable(edge);
                ThingEdge thingEdge = cachedEdges.get(sortableEdge);
                if (thingEdge != null) {
                    if (thingEdge.isInferred() && !edge.isInferred()) thingEdge.isInferred(false);
                } else {
                    cachedEdges.put(sortableEdge, sortableEdge.getEdge());
                }
                return cachedEdges;
            });

            if (isModified) {
                assert !owner.isDeleted();
                owner.setModified();
            }
            if (isReflexive) {
                if (direction.isOut()) ((ThingAdjacencyImpl.Write) edge.to().ins()).putNonReflexive(edge);
                else ((ThingAdjacencyImpl.Write) edge.from().outs()).putNonReflexive(edge);
            }
            return edge;
        }

        @Override
        public ThingEdgeImpl put(Encoding.Edge.Thing encoding, ThingVertex.Write adjacent, boolean isInferred) {
            assert !encoding.isOptimisation();
            if (encoding == Encoding.Edge.Thing.HAS && direction.isOut() && !isInferred) {
                owner.graph().stats().hasEdgeCreated(owner.iid(), adjacent.iid().asAttribute());
            }
            ThingEdgeImpl edge = direction.isOut()
                    ? new ThingEdgeImpl.Buffered(encoding, owner, adjacent, isInferred)
                    : new ThingEdgeImpl.Buffered(encoding, adjacent, owner, isInferred);
            IID[] infixes = new IID[]{adjacent.iid().prefix(), adjacent.iid().type()};
            return put(encoding, edge, infixes, true, true);
        }

        @Override
        public ThingEdge put(Encoding.Edge.Thing encoding, ThingVertex.Write adjacent, ThingVertex.Write optimised, boolean isInferred) {
            assert encoding.isOptimisation();
            ThingEdgeImpl edge = direction.isOut()
                    ? new ThingEdgeImpl.Buffered(encoding, owner, adjacent, optimised, isInferred)
                    : new ThingEdgeImpl.Buffered(encoding, adjacent, owner, optimised, isInferred);
            IID[] infixes = new IID[]{optimised.iid().type(), adjacent.iid().prefix(), adjacent.iid().type()};
            return put(encoding, edge, infixes, true, true);
        }

        private void putNonReflexive(ThingEdgeImpl edge) {
            put(edge.encoding(), edge, infixTails(edge), true, false);
        }

        @Override
        public ThingEdge cache(ThingEdge edge) {
            return put(edge.encoding(), (ThingEdgeImpl) edge, infixTails(edge), false, false);
        }

        @Override
        public void remove(ThingEdge edge) {
            InfixIID.Thing infixIID = infixIID(edge.encoding(), infixTails(edge));
            if (edges.containsKey(infixIID)) {
                edges.get(infixIID).remove(asSortable(edge));
                owner.setModified();
            }
        }

        @Override
        public void deleteAll() {
            for (Encoding.Edge.Thing encoding : Encoding.Edge.Thing.values()) delete(encoding);
        }

        @Override
        public void commit() {
            iterate(edges.values()).flatMap(edgeMap -> iterate(edgeMap.values()))
                    .filter(e -> !e.isInferred()).forEachRemaining(Edge::commit);
        }

        public static class Buffered extends ThingAdjacencyImpl.Write {

            public Buffered(ThingVertex.Write owner, Encoding.Direction.Adjacency direction) {
                super(owner, direction);
            }

            @Override
            public ThingIteratorBuilderImpl edge(Encoding.Edge.Thing encoding) {
                return new ThingIteratorBuilderImpl(bufferedEdgeIterator(encoding, new IID[]{}).map(EdgeDirected::getEdge));
            }

            @Override
            public ThingIteratorSortedBuilder edgeHas(IID... lookAhead) {
                return new ThingIteratorSortedBuilderImpl(bufferedEdgeIterator(Encoding.Edge.Thing.HAS, lookAhead));
            }

            @Override
            public ThingIteratorSortedBuilder edgePlaying(IID... lookAhead) {
                return new ThingIteratorSortedBuilderImpl(bufferedEdgeIterator(Encoding.Edge.Thing.PLAYING, lookAhead));
            }

            @Override
            public ThingIteratorSortedBuilder edgeRelating(IID... lookAhead) {
                return new ThingIteratorSortedBuilderImpl(bufferedEdgeIterator(Encoding.Edge.Thing.RELATING, lookAhead));
            }

            @Override
            public ThingIteratorSortedBuilder edgeRolePlayer(IID roleType, IID... lookAhead) {
                IID[] merged = new IID[1 + lookAhead.length];
                merged[0] = roleType;
                System.arraycopy(lookAhead, 0, merged, 1, lookAhead.length);
                return new ThingIteratorSortedBuilderImpl(bufferedEdgeIterator(Encoding.Edge.Thing.ROLEPLAYER, merged));
            }

            @Override
            public void delete(Encoding.Edge.Thing encoding) {
                bufferedEdgeIterator(encoding, new IID[0]).forEachRemaining(sortable -> sortable.getEdge().delete());
            }

            @Override
            public void delete(Encoding.Edge.Thing encoding, IID... lookAhead) {
                bufferedEdgeIterator(encoding, lookAhead).forEachRemaining(sortable -> sortable.getEdge().delete());
            }
        }

        public static class Persisted extends ThingAdjacencyImpl.Write {

            public Persisted(ThingVertex.Write owner, Encoding.Direction.Adjacency direction) {
                super(owner, direction);
            }

            private FunctionalIterator<ThingEdge> edgeIterator(Encoding.Edge.Thing encoding, IID... lookahead) {
                ByteArray iid = join(owner.iid().bytes(), infixIID(encoding, lookahead).bytes());
                FunctionalIterator<ThingEdge> storageIterator = owner.graph().storage().iterate(
                        iid, (key, value) -> key).map(key -> cache(newPersistedEdge(EdgeIID.Thing.of(key)))
                );
                FunctionalIterator<ThingEdge> bufferedIterator = bufferedEdgeIterator(encoding, lookahead).map(EdgeDirected::getEdge);
                return link(bufferedIterator, storageIterator).distinct();
            }

            private FunctionalIterator.Sorted<EdgeDirected> edgeIteratorSorted(Encoding.Edge.Thing encoding, IID... lookahead) {
                assert encoding != Encoding.Edge.Thing.ROLEPLAYER || lookahead.length >= 1;
                ByteArray prefix = join(owner.iid().bytes(), infixIID(encoding, lookahead).bytes());
                FunctionalIterator.Sorted<EdgeDirected> storageIterator = owner.graph().storage()
                        .iterate(prefix, (key, value) -> key).mapSorted(
                                key -> asSortable(cache(newPersistedEdge(EdgeIID.Thing.of(key)))),
                                sortable -> sortable.key.bytes()
                        );
                FunctionalIterator.Sorted<EdgeDirected> bufferedIterator = bufferedEdgeIterator(encoding, lookahead);
                return bufferedIterator.merge(storageIterator).distinct();
            }

            private ThingEdgeImpl.Persisted newPersistedEdge(EdgeIID.Thing of) {
                return new ThingEdgeImpl.Persisted(owner.graph(), of);
            }

            @Override
            public ThingIteratorBuilderImpl edge(Encoding.Edge.Thing encoding) {
                return new ThingIteratorBuilderImpl(edgeIterator(encoding));
            }

            @Override
            public ThingIteratorSortedBuilder edgeHas(IID... lookAhead) {
                return new ThingIteratorSortedBuilderImpl(edgeIteratorSorted(Encoding.Edge.Thing.HAS, lookAhead));
            }

            @Override
            public ThingIteratorSortedBuilder edgePlaying(IID... lookAhead) {
                return new ThingIteratorSortedBuilderImpl(edgeIteratorSorted(Encoding.Edge.Thing.PLAYING, lookAhead));
            }

            @Override
            public ThingIteratorSortedBuilder edgeRelating(IID... lookAhead) {
                return new ThingIteratorSortedBuilderImpl(edgeIteratorSorted(Encoding.Edge.Thing.RELATING, lookAhead));
            }

            @Override
            public ThingIteratorSortedBuilder edgeRolePlayer(IID roleType, IID... lookAhead) {
                IID[] merged = new IID[1 + lookAhead.length];
                merged[0] = roleType;
                System.arraycopy(lookAhead, 0, merged, 1, lookAhead.length);
                return new ThingIteratorSortedBuilderImpl(edgeIteratorSorted(Encoding.Edge.Thing.ROLEPLAYER, merged));
            }

            @Override
            public ThingEdge edge(Encoding.Edge.Thing encoding, ThingVertex adjacent) {
                assert !encoding.isOptimisation();
                ThingEdge edge = super.edge(encoding, adjacent);
                if (edge != null) return edge;

                EdgeIID.Thing edgeIID = EdgeIID.Thing.of(owner.iid(), infixIID(encoding), adjacent.iid());
                if (owner.graph().storage().get(edgeIID.bytes()) == null) return null;
                else return cache(newPersistedEdge(edgeIID));
            }

            @Override
            public ThingEdge edge(Encoding.Edge.Thing encoding, ThingVertex adjacent, ThingVertex optimised) {
                assert encoding.isOptimisation();
                ThingEdge edge = super.edge(encoding, adjacent, optimised);
                if (edge != null) return edge;

                EdgeIID.Thing edgeIID = EdgeIID.Thing.of(
                        owner.iid(), infixIID(encoding, optimised.iid().type()),
                        adjacent.iid(), SuffixIID.of(optimised.iid().key())
                );
                if (owner.graph().storage().get(edgeIID.bytes()) == null) return null;
                else return cache(newPersistedEdge(edgeIID));
            }

            @Override
            public void delete(Encoding.Edge.Thing encoding) {
                edgeIterator(encoding).forEachRemaining(Edge::delete);
            }

            @Override
            public void delete(Encoding.Edge.Thing encoding, IID... lookAhead) {
                edgeIterator(encoding, lookAhead).forEachRemaining(Edge::delete);
            }
        }
    }

}
