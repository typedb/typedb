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
import java.util.function.Predicate;

import static com.vaticle.typedb.core.common.collection.ByteArray.join;
import static com.vaticle.typedb.core.common.iterator.Iterators.empty;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
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

    public static class ThingIteratorBuilderImpl implements ThingIteratorBuilder {

        private final FunctionalIterator<ThingEdge> edgeIterator;

        ThingIteratorBuilderImpl(FunctionalIterator<ThingEdge> edgeIterator) {
            this.edgeIterator = edgeIterator;
        }

        @Override
        public FunctionalIterator<ThingVertex> from() {
            return edgeIterator.map(ThingEdge::from);
        }

        @Override
        public FunctionalIterator<ThingVertex> to() {
            return edgeIterator.map(ThingEdge::to);
        }

        @Override
        public FunctionalIterator<ThingEdge> get() {
            return edgeIterator;
        }
    }

    public static class Read extends ThingAdjacencyImpl implements ThingAdjacency.Read {

        final ThingVertex.Read owner;

        public Read(ThingVertex.Read owner, Encoding.Direction.Adjacency direction) {
            super(direction);
            this.owner = owner;
        }

        private FunctionalIterator<ThingEdge> edgeIterator(Encoding.Edge.Thing encoding, IID... lookahead) {
            ByteArray iid = join(owner.iid().bytes(), infixIID(encoding, lookahead).bytes());
            return owner.graph().storage().iterate(iid, (key, value) -> newPersistedEdge(EdgeIID.Thing.of(key)));
        }

        private ThingEdgeImpl.Persisted newPersistedEdge(EdgeIID.Thing of) {
            return new ThingEdgeImpl.Persisted(owner.graph(), of);
        }

        @Override
        public ThingIteratorBuilder edge(Encoding.Edge.Thing encoding) {
            return new ThingIteratorBuilderImpl(edgeIterator(encoding));
        }

        @Override
        public ThingIteratorBuilder edge(Encoding.Edge.Thing encoding, IID... lookAhead) {
            return new ThingIteratorBuilderImpl(edgeIterator(encoding, lookAhead));
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
        final ConcurrentMap<InfixIID.Thing, ConcurrentMap<EdgeIID.Thing, ThingEdge>> edges;

        Write(ThingVertex.Write owner, Encoding.Direction.Adjacency direction) {
            super(direction);
            this.owner = owner;
            this.infixes = new ConcurrentHashMap<>();
            this.edges = new ConcurrentHashMap<>();
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

        FunctionalIterator<ThingEdge> bufferedEdgeIterator(Encoding.Edge.Thing encoding, IID[] lookAhead) {
            ConcurrentMap<EdgeIID.Thing, ThingEdge> result;
            InfixIID.Thing infixIID = infixIID(encoding, lookAhead);
            if (lookAhead.length == encoding.lookAhead()) {
                return (result = edges.get(infixIID)) != null ? iterate(result.values()) : empty();
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

            return iterate(iids).flatMap(iid -> {
                ConcurrentMap<EdgeIID.Thing, ThingEdge> res;
                return (res = edges.get(iid)) != null ? iterate(res.values()) : empty();
            });
        }

        @Override
        public ThingEdge edge(Encoding.Edge.Thing encoding, ThingVertex adjacent, ThingVertex optimised) {
            assert encoding.isOptimisation();
            Predicate<ThingEdge> predicate = direction.isOut()
                    ? e -> e.to().equals(adjacent) && e.outIID().suffix().equals(SuffixIID.of(optimised.iid().key()))
                    : e -> e.from().equals(adjacent) && e.inIID().suffix().equals(SuffixIID.of(optimised.iid().key()));
            FunctionalIterator<ThingEdge> iterator = bufferedEdgeIterator(
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
        public ThingEdge edge(Encoding.Edge.Thing encoding, ThingVertex adjacent) {
            assert !encoding.isOptimisation();
            Predicate<ThingEdge> predicate =
                    direction.isOut() ? e -> e.to().equals(adjacent) : e -> e.from().equals(adjacent);
            FunctionalIterator<ThingEdge> iterator =
                    bufferedEdgeIterator(encoding, new IID[]{adjacent.iid().prefix(), adjacent.iid().type()});
            ThingEdge edge = null;
            while (iterator.hasNext()) {
                if (predicate.test(edge = iterator.next())) break;
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

            edges.compute(infixIID, (iid, edgesByOutIID) -> {
                if (edgesByOutIID == null) edgesByOutIID = new ConcurrentHashMap<>();
                ThingEdge thingEdge = edgesByOutIID.get(edge.outIID());
                if (thingEdge != null) {
                    if (thingEdge.isInferred() && !edge.isInferred()) thingEdge.isInferred(false);
                } else {
                    edgesByOutIID.put(edge.outIID(), edge);
                }
                return edgesByOutIID;
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
                edges.get(infixIID).remove(edge.outIID());
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
                return new ThingIteratorBuilderImpl(bufferedEdgeIterator(encoding, new IID[]{}));
            }

            @Override
            public ThingIteratorBuilderImpl edge(Encoding.Edge.Thing encoding, IID... lookAhead) {
                return new ThingIteratorBuilderImpl(bufferedEdgeIterator(encoding, lookAhead));
            }

            @Override
            public void delete(Encoding.Edge.Thing encoding) {
                bufferedEdgeIterator(encoding, new IID[0]).forEachRemaining(Edge::delete);
            }

            @Override
            public void delete(Encoding.Edge.Thing encoding, IID... lookAhead) {
                bufferedEdgeIterator(encoding, lookAhead).forEachRemaining(Edge::delete);
            }
        }

        public static class Persisted extends ThingAdjacencyImpl.Write {

            public Persisted(ThingVertex.Write owner, Encoding.Direction.Adjacency direction) {
                super(owner, direction);
            }

            private FunctionalIterator<ThingEdge> edgeIterator(Encoding.Edge.Thing encoding, IID... lookahead) {
                ByteArray iid = join(owner.iid().bytes(), infixIID(encoding, lookahead).bytes());
                FunctionalIterator<ThingEdge> storageIterator = owner.graph().storage()
                        .iterate(iid, (key, value) -> cache(newPersistedEdge(EdgeIID.Thing.of(key))));
                FunctionalIterator<ThingEdge> bufferedIterator = bufferedEdgeIterator(encoding, lookahead);
                return link(bufferedIterator, storageIterator).distinct();
            }

            private ThingEdgeImpl.Persisted newPersistedEdge(EdgeIID.Thing of) {
                return new ThingEdgeImpl.Persisted(owner.graph(), of);
            }

            @Override
            public ThingIteratorBuilderImpl edge(Encoding.Edge.Thing encoding) {
                return new ThingIteratorBuilderImpl(edgeIterator(encoding));
            }

            @Override
            public ThingIteratorBuilder edge(Encoding.Edge.Thing encoding, IID... lookAhead) {
                return new ThingIteratorBuilderImpl(edgeIterator(encoding, lookAhead));
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
