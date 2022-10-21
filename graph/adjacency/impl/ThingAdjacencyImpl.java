/*
 * Copyright (C) 2022 Vaticle
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
import com.vaticle.typedb.core.common.collection.KeyValue;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.Forwardable;
import com.vaticle.typedb.core.common.parameters.Order;
import com.vaticle.typedb.core.encoding.iid.VertexIID;
import com.vaticle.typedb.core.graph.adjacency.ThingAdjacency;
import com.vaticle.typedb.core.graph.adjacency.impl.ThingEdgeIterator.InEdgeIteratorImpl;
import com.vaticle.typedb.core.graph.adjacency.impl.ThingEdgeIterator.OutEdgeIteratorImpl;
import com.vaticle.typedb.core.encoding.Encoding;
import com.vaticle.typedb.core.encoding.key.Key;
import com.vaticle.typedb.core.graph.edge.Edge;
import com.vaticle.typedb.core.graph.edge.ThingEdge;
import com.vaticle.typedb.core.graph.edge.impl.ThingEdgeImpl;
import com.vaticle.typedb.core.encoding.iid.EdgeViewIID;
import com.vaticle.typedb.core.encoding.iid.IID;
import com.vaticle.typedb.core.encoding.iid.InfixIID;
import com.vaticle.typedb.core.graph.vertex.ThingVertex;
import com.vaticle.typedb.core.graph.vertex.TypeVertex;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Predicate;

import static com.vaticle.typedb.core.common.collection.ByteArray.empty;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_ARGUMENT;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.common.iterator.Iterators.link;
import static com.vaticle.typedb.core.common.parameters.Order.Asc.ASC;
import static com.vaticle.typedb.core.common.iterator.sorted.SortedIterators.Forwardable.emptySorted;
import static com.vaticle.typedb.core.common.iterator.sorted.SortedIterators.Forwardable.iterateSorted;
import static com.vaticle.typedb.core.encoding.Encoding.Edge.Thing.Optimised.ROLEPLAYER;
import static java.util.Arrays.copyOf;
import static java.util.Arrays.copyOfRange;

/**
 * This class would benefit from multiple inheritance/traits massively:
 * Dimension 1: In/Out adjacency
 * Dimension 2: Read/WriteBuffered/WritePersisted adjacency
 *
 * The interfaces reflect this well, while the implementation aims to minimise code duplication
 */
public abstract class ThingAdjacencyImpl<EDGE_VIEW extends ThingEdge.View<EDGE_VIEW>> implements ThingAdjacency {

    abstract ThingVertex owner();

    abstract EDGE_VIEW getView(ThingEdge edge);

    InfixIID.Thing infixIID(Encoding.Edge.Thing encoding) {
        return isOut() ?
                InfixIID.Thing.of(encoding.forward()) :
                InfixIID.Thing.of(encoding.backward());
    }

    InfixIID.Thing infixIID(Encoding.Edge.Thing encoding, IID roleTypeIID) {
        if (!(roleTypeIID instanceof VertexIID.Type)) throw TypeDBException.of(ILLEGAL_ARGUMENT);
        return infixIID(encoding, (VertexIID.Type) roleTypeIID);
    }

    InfixIID.Thing infixIID(Encoding.Edge.Thing encoding, VertexIID.Type lookahead) {
        return isOut() ?
                InfixIID.Thing.of(encoding.forward(), lookahead) :
                InfixIID.Thing.of(encoding.backward(), lookahead);
    }

    EdgeViewIID.Thing viewIID(Encoding.Edge.Thing encoding, ThingVertex adjacent) {
        return EdgeViewIID.Thing.of(owner().iid(), infixIID(encoding), adjacent.iid());
    }

    EdgeViewIID.Thing viewIID(Encoding.Edge.Thing encoding, ThingVertex adjacent, ThingVertex optimised) {
        return EdgeViewIID.Thing.of(
                owner().iid(), infixIID(encoding, optimised.iid().type()),
                adjacent.iid(), optimised.iid().key()
        );
    }

    Key.Prefix<EdgeViewIID.Thing> viewIIDPrefix(Encoding.Edge.Thing encoding, List<IID> lookahead) {
        List<IID> iids = lookaheadWithEncoding(encoding, lookahead);
        assert iids.size() > 0 && iids.get(0) instanceof InfixIID.Thing;
        return EdgeViewIID.Thing.prefix(owner().iid(), (InfixIID.Thing) iids.get(0), iids.subList(1, iids.size()));
    }

    List<IID> lookaheadWithEncoding(Encoding.Edge.Thing encoding, List<IID> lookahead) {
        if (lookahead.size() == 0) return List.of(infixIID(encoding));
        List<IID> withEncoding;
        if (encoding.isOptimisation()) {
            InfixIID.Thing infixIID = infixIID(encoding, lookahead.get(0));
            withEncoding = new ArrayList<>(lookahead);
            withEncoding.set(0, infixIID);
        } else {
            withEncoding = new ArrayList<>(lookahead.size() + 1);
            withEncoding.add(infixIID(encoding));
            withEncoding.addAll(lookahead);
        }
        return withEncoding;
    }

    ThingEdgeImpl.Persisted newPersistedEdge(EdgeViewIID.Thing iid) {
        return new ThingEdgeImpl.Persisted(owner().graph(), iid);
    }

    Forwardable<EDGE_VIEW, Order.Asc> iteratePersistedViews(Encoding.Edge.Thing encoding, List<IID> lookahead) {
        assert encoding != ROLEPLAYER || lookahead.size() >= 1;
        Key.Prefix<EdgeViewIID.Thing> prefix = viewIIDPrefix(encoding, lookahead);
        return owner().graph().storage().iterate(prefix, ASC).mapSorted(
                kv -> getView(newPersistedEdge(EdgeViewIID.Thing.of(kv.key().bytes()))),
                edgeView -> KeyValue.of(edgeView.iid(), empty()),
                ASC
        );
    }

    List<IID> concat(IID iid, IID... iids) {
        IID[] concat = new IID[1 + iids.length];
        concat[0] = iid;
        System.arraycopy(iids, 0, concat, 1, iids.length);
        return List.of(concat);
    }

    public static abstract class Read<EDGE_VIEW extends ThingEdge.View<EDGE_VIEW>> extends ThingAdjacencyImpl<EDGE_VIEW> {

        final ThingVertex owner;

        Read(ThingVertex owner) {
            this.owner = owner;
        }

        @Override
        ThingVertex owner() {
            return owner;
        }

        @Override
        public ThingEdge edge(Encoding.Edge.Thing encoding, ThingVertex adjacent) {
            EdgeViewIID.Thing iid = viewIID(encoding, adjacent);
            if (owner.graph().storage().get(iid) == null) return null;
            else return newPersistedEdge(iid);
        }

        @Override
        public ThingEdge edge(Encoding.Edge.Thing encoding, ThingVertex adjacent, ThingVertex optimised) {
            EdgeViewIID.Thing iid = viewIID(encoding, adjacent, optimised);
            if (owner.graph().storage().get(iid) == null) return null;
            else return newPersistedEdge(iid);
        }

        @Override
        public UnsortedEdgeIterator edge(Encoding.Edge.Thing.Optimised encoding) {
            Key.Prefix<EdgeViewIID.Thing> prefix = EdgeViewIID.Thing.prefix(owner().iid(), infixIID(encoding));
            return new UnsortedEdgeIterator(owner.graph().storage().iterate(prefix, ASC)
                    .map(kv -> newPersistedEdge(EdgeViewIID.Thing.of(kv.key().bytes()))));
        }

        public static class In extends Read<ThingEdge.View.Backward> implements ThingAdjacency.In {

            public In(ThingVertex owner) {
                super(owner);
            }

            @Override
            public InEdgeIterator edge(Encoding.Edge.Thing.Base encoding, IID... lookAhead) {
                return new InEdgeIteratorImpl(iteratePersistedViews(encoding, List.of(lookAhead)), owner, encoding);
            }

            @Override
            public InEdgeIterator edge(Encoding.Edge.Thing.Optimised encoding, TypeVertex roleType, IID... lookAhead) {
                return new InEdgeIteratorImpl(
                        iteratePersistedViews(encoding, concat(roleType.iid(), lookAhead)), owner, encoding, roleType
                );
            }

            @Override
            ThingEdge.View.Backward getView(ThingEdge edge) {
                return edge.backwardView();
            }
        }

        public static class Out extends Read<ThingEdge.View.Forward> implements ThingAdjacency.Out {

            public Out(ThingVertex owner) {
                super(owner);
            }

            @Override
            public OutEdgeIterator edge(Encoding.Edge.Thing.Base encoding, IID... lookAhead) {
                return new OutEdgeIteratorImpl(iteratePersistedViews(encoding, List.of(lookAhead)), owner, encoding);
            }

            @Override
            public OutEdgeIterator edge(Encoding.Edge.Thing.Optimised encoding, TypeVertex roleType, IID... lookAhead) {
                return new OutEdgeIteratorImpl(
                        iteratePersistedViews(encoding, concat(roleType.iid(), lookAhead)), owner, encoding, roleType
                );
            }

            @Override
            ThingEdge.View.Forward getView(ThingEdge edge) {
                return edge.forwardView();
            }
        }
    }

    public static abstract class Write<EDGE_VIEW extends ThingEdge.View<EDGE_VIEW>>
            extends ThingAdjacencyImpl<EDGE_VIEW> implements ThingAdjacency.Write {

        final ThingVertex.Write owner;
        final ConcurrentMap<List<IID>, ConcurrentSet<List<IID>>> infixes;
        // TODO: we can simplify this to ignore the idea of reasoning in write transactions
        final ConcurrentMap<List<IID>, ConcurrentNavigableMap<EDGE_VIEW, ThingEdgeImpl.Buffered>> edges;

        Write(ThingVertex.Write owner) {
            this.owner = owner;
            this.infixes = new ConcurrentHashMap<>();
            this.edges = new ConcurrentHashMap<>();
        }

        @Override
        ThingVertex owner() {
            return owner;
        }

        List<IID> infixTails(ThingEdge edge) {
            if (edge.encoding().isOptimisation()) {
                if (isOut()) {
                    return List.of(edge.forwardView().iid().infix().asRolePlayer().tail().get(), edge.toIID().prefix(), edge.toIID().type(), edge.toIID().key());
                } else {
                    return List.of(edge.backwardView().iid().infix().asRolePlayer().tail().get(), edge.fromIID().prefix(), edge.fromIID().type(), edge.fromIID().key());
                }
            } else {
                if (isOut()) return List.of(edge.toIID().prefix(), edge.toIID().type());
                else return List.of(edge.fromIID().prefix(), edge.fromIID().type());
            }
        }

        Forwardable<EDGE_VIEW, Order.Asc> iterateBufferedViews(Encoding.Edge.Thing encoding, List<IID> lookahead) {
            ConcurrentNavigableMap<EDGE_VIEW, ThingEdgeImpl.Buffered> result;
            List<IID> iidsWithEncoding = lookaheadWithEncoding(encoding, lookahead);
            if (lookahead.size() == encoding.lookAhead()) {
                return (result = edges.get(iidsWithEncoding)) != null ? iterateSorted(result.keySet(), ASC) : emptySorted();
            }

            assert lookahead.size() < encoding.lookAhead();
            Set<List<IID>> extendedIIDs = new HashSet<>();
            extendedIIDs.add(iidsWithEncoding);
            for (int i = lookahead.size(); i < encoding.lookAhead() && !extendedIIDs.isEmpty(); i++) {
                Set<List<IID>> newIIDs = new HashSet<>();
                for (List<IID> iids : extendedIIDs) {
                    Set<List<IID>> someNewIIDs = infixes.get(iids);
                    if (someNewIIDs != null) newIIDs.addAll(someNewIIDs);
                }
                extendedIIDs = newIIDs;
            }

            return iterate(extendedIIDs).mergeMapForwardable(iid -> {
                ConcurrentNavigableMap<EDGE_VIEW, ThingEdgeImpl.Buffered> res;
                return (res = edges.get(iid)) != null ? iterateSorted(res.keySet(), ASC) : emptySorted();
            }, ASC);
        }

        @Override
        public ThingEdge edge(Encoding.Edge.Thing encoding, ThingVertex adjacent, ThingVertex optimised) {
            assert encoding.isOptimisation();
            Predicate<ThingEdge> predicate = isOut()
                    ? e -> e.to().equals(adjacent) && e.forwardView().iid().suffix().equals(optimised.iid().key())
                    : e -> e.from().equals(adjacent) && e.backwardView().iid().suffix().equals(optimised.iid().key());
            Forwardable<EDGE_VIEW, Order.Asc> iterator = iterateBufferedViews(
                    encoding, List.of(optimised.iid().type(), adjacent.iid().prefix(), adjacent.iid().type())
            );
            iterator.forward(isOut() ?
                    getView(new ThingEdgeImpl.Target(encoding, owner, adjacent, optimised.type())) :
                    getView(new ThingEdgeImpl.Target(encoding, adjacent, owner, optimised.type()))
            );
            ThingEdge edge = null;
            while (iterator.hasNext()) {
                if (predicate.test(edge = iterator.next().edge())) break;
                else edge = null;
            }
            iterator.recycle();
            return edge;
        }

        @Override
        public ThingEdge edge(Encoding.Edge.Thing encoding, ThingVertex adjacent) {
            assert !encoding.isOptimisation();
            Predicate<ThingEdge> predicate = isOut() ? e -> e.to().equals(adjacent) : e -> e.from().equals(adjacent);
            Forwardable<EDGE_VIEW, Order.Asc> iterator = iterateBufferedViews(
                    encoding, List.of(adjacent.iid().prefix(), adjacent.iid().type())
            );
            iterator.forward(isOut() ?
                    getView(new ThingEdgeImpl.Target(encoding, owner, adjacent, null)) :
                    getView(new ThingEdgeImpl.Target(encoding, adjacent, owner, null))
            );
            ThingEdge edge = null;
            while (iterator.hasNext()) {
                if (predicate.test(edge = iterator.next().edge())) break;
                else edge = null;
            }
            iterator.recycle();
            return edge;
        }

        private void put(Encoding.Edge.Thing encoding, ThingEdgeImpl.Buffered edge, List<IID> infixes,
                         boolean isReflexive) {
            assert encoding.lookAhead() == infixes.size();
            List<IID> infixIID = List.of(infixIID(encoding));
            for (int i = 0; i < encoding.lookAhead(); i++) {
                this.infixes.computeIfAbsent(infixIID, x -> new ConcurrentSet<>()).add(
                        infixIID = lookaheadWithEncoding(encoding, infixes.subList(0, i + 1))
                );
            }

            this.edges.compute(infixIID, (iid, bufferedEdges) -> {
                EDGE_VIEW edgeView = getView(edge);
                if (bufferedEdges == null) bufferedEdges = new ConcurrentSkipListMap<>();
                bufferedEdges.compute(edgeView, (view, existingEdge) -> {
                    if (existingEdge == null) {
                        if (isOut()) owner.graph().edgeCreated(edge); // only record creation in one direction
                        return edge;
                    } else {
                        assert existingEdge.isInferred() == edge.isInferred();
                        return existingEdge;
                    }
                });
                return bufferedEdges;
            });

            assert !owner.isDeleted();
            owner.setModified();
            if (isReflexive) {
                if (isOut()) ((ThingAdjacencyImpl.Write<?>) edge.to().ins()).putNonReflexive(edge);
                else ((ThingAdjacencyImpl.Write<?>) edge.from().outs()).putNonReflexive(edge);
            }
        }

        private void putNonReflexive(ThingEdgeImpl.Buffered edge) {
            put(edge.encoding(), edge, infixTails(edge), false);
        }

        @Override
        public ThingEdgeImpl put(Encoding.Edge.Thing encoding, ThingVertex.Write adjacent, boolean isInferred) {
            assert !encoding.isOptimisation();
            ThingEdgeImpl.Buffered edge = isOut()
                    ? new ThingEdgeImpl.Buffered(encoding, owner, adjacent, isInferred)
                    : new ThingEdgeImpl.Buffered(encoding, adjacent, owner, isInferred);
            List<IID> infixes = List.of(adjacent.iid().prefix(), adjacent.iid().type());
            put(encoding, edge, infixes, true);

            return edge;
        }

        @Override
        public ThingEdge put(Encoding.Edge.Thing encoding, ThingVertex.Write adjacent, ThingVertex.Write optimised,
                             boolean isInferred) {
            assert encoding.isOptimisation();
            ThingEdgeImpl.Buffered edge = isOut()
                    ? new ThingEdgeImpl.Buffered(encoding, owner, adjacent, optimised, isInferred)
                    : new ThingEdgeImpl.Buffered(encoding, adjacent, owner, optimised, isInferred);
            List<IID> infixes = List.of(optimised.iid().type(), adjacent.iid().prefix(), adjacent.iid().type(), adjacent.iid().key());
            put(encoding, edge, infixes, true);
            return edge;
        }

        @Override
        public void remove(ThingEdge edge) {
            List<IID> lookahead = lookaheadWithEncoding(edge.encoding(), infixTails(edge));
            if (edges.containsKey(lookahead)) {
                edges.get(lookahead).remove(getView(edge));
                owner.setModified();
            }
        }

        @Override
        public void deleteAll() {
            iterate(Encoding.Edge.Thing.Base.values()).forEachRemaining(this::delete);
            iterate(Encoding.Edge.Thing.Optimised.values()).forEachRemaining(this::delete);
        }

        @Override
        public void commit() {
            iterate(edges.values()).flatMap(edgeMap -> iterate(edgeMap.values()))
                    .filter(e -> !e.isInferred()).forEachRemaining(Edge::commit);
        }

        public static abstract class Buffered<EDGE_VIEW extends ThingEdge.View<EDGE_VIEW>>
                extends ThingAdjacencyImpl.Write<EDGE_VIEW> {

            Buffered(ThingVertex.Write owner) {
                super(owner);
            }

            @Override
            public UnsortedEdgeIterator edge(Encoding.Edge.Thing.Optimised encoding) {
                return new UnsortedEdgeIterator(iterateBufferedViews(encoding, List.of()).map(ThingEdge.View::edge));
            }

            @Override
            public void delete(Encoding.Edge.Thing encoding) {
                iterateBufferedViews(encoding, List.of()).forEachRemaining(comparableEdge -> comparableEdge.edge().delete());
            }

            @Override
            public void delete(Encoding.Edge.Thing encoding, IID... lookAhead) {
                iterateBufferedViews(encoding, List.of(lookAhead)).forEachRemaining(comparableEdge -> comparableEdge.edge().delete());
            }

            public static class In extends Buffered<ThingEdge.View.Backward> implements ThingAdjacency.Write.In {

                public In(ThingVertex.Write owner) {
                    super(owner);
                }

                @Override
                ThingEdge.View.Backward getView(ThingEdge edge) {
                    return edge.backwardView();
                }

                @Override
                public InEdgeIterator edge(Encoding.Edge.Thing.Base encoding, IID... lookahead) {
                    return new InEdgeIteratorImpl(iterateBufferedViews(encoding, List.of(lookahead)), owner, encoding);
                }

                @Override
                public InEdgeIterator edge(Encoding.Edge.Thing.Optimised encoding, TypeVertex roleType,
                                           IID... lookahead) {
                    return new InEdgeIteratorImpl(
                            iterateBufferedViews(ROLEPLAYER, concat(roleType.iid(), lookahead)), owner, encoding, roleType
                    );
                }
            }

            public static class Out extends Buffered<ThingEdge.View.Forward> implements ThingAdjacency.Write.Out {

                public Out(ThingVertex.Write owner) {
                    super(owner);
                }

                @Override
                ThingEdge.View.Forward getView(ThingEdge edge) {
                    return edge.forwardView();
                }

                @Override
                public OutEdgeIterator edge(Encoding.Edge.Thing.Base encoding, IID... lookahead) {
                    return new OutEdgeIteratorImpl(iterateBufferedViews(encoding, List.of(lookahead)), owner, encoding);
                }

                @Override
                public OutEdgeIterator edge(Encoding.Edge.Thing.Optimised encoding, TypeVertex roleType,
                                            IID... lookahead) {
                    return new OutEdgeIteratorImpl(
                            iterateBufferedViews(ROLEPLAYER, concat(roleType.iid(), lookahead)), owner, encoding, roleType
                    );
                }

            }
        }

        public static abstract class Persisted<EDGE_VIEW extends ThingEdge.View<EDGE_VIEW>>
                extends ThingAdjacencyImpl.Write<EDGE_VIEW> {

            Persisted(ThingVertex.Write owner) {
                super(owner);
            }

            @Override
            public UnsortedEdgeIterator edge(Encoding.Edge.Thing.Optimised encoding) {
                return new UnsortedEdgeIterator(iterateEdges(encoding));
            }

            private FunctionalIterator<ThingEdge> iterateEdges(Encoding.Edge.Thing encoding, IID... lookahead) {
                Key.Prefix<EdgeViewIID.Thing> prefix = viewIIDPrefix(encoding, List.of(lookahead));
                FunctionalIterator<ThingEdge> storageIterator = owner.graph().storage().iterate(prefix, ASC)
                        .map(keyValue -> newPersistedEdge(EdgeViewIID.Thing.of(keyValue.key().bytes())));
                FunctionalIterator<ThingEdge> bufferedIterator = iterateBufferedViews(encoding, List.of(lookahead))
                        .map(ThingEdge.View::edge);
                return link(bufferedIterator, storageIterator).distinct(); // note: has edges can be persisted and buffered
            }

            Forwardable<EDGE_VIEW, Order.Asc> edgeIterator(Encoding.Edge.Thing encoding, List<IID> lookahead) {
                assert encoding != ROLEPLAYER || lookahead.size() >= 1;
                Forwardable<EDGE_VIEW, Order.Asc> storageIter = iteratePersistedViews(encoding, lookahead);
                Forwardable<EDGE_VIEW, Order.Asc> bufferedIter = iterateBufferedViews(encoding, lookahead);
                return bufferedIter.merge(storageIter).distinct(); // note: has edges can be persisted and buffered
            }

            @Override
            public ThingEdge edge(Encoding.Edge.Thing encoding, ThingVertex adjacent) {
                assert !encoding.isOptimisation();
                ThingEdge edge = super.edge(encoding, adjacent);
                if (edge != null) return edge;

                EdgeViewIID.Thing edgeIID = viewIID(encoding, adjacent);
                if (owner.graph().storage().get(edgeIID) == null) return null;
                else return newPersistedEdge(edgeIID);
            }

            @Override
            public ThingEdge edge(Encoding.Edge.Thing encoding, ThingVertex adjacent, ThingVertex optimised) {
                assert encoding.isOptimisation();
                ThingEdge edge = super.edge(encoding, adjacent, optimised);
                if (edge != null) return edge;

                EdgeViewIID.Thing edgeIID = viewIID(encoding, adjacent, optimised);
                if (owner.graph().storage().get(edgeIID) == null) return null;
                else return newPersistedEdge(edgeIID);
            }

            @Override
            public void delete(Encoding.Edge.Thing encoding) {
                iterateEdges(encoding).forEachRemaining(Edge::delete);
            }

            @Override
            public void delete(Encoding.Edge.Thing encoding, IID... lookAhead) {
                iterateEdges(encoding, lookAhead).forEachRemaining(Edge::delete);
            }

            public static class In extends Persisted<ThingEdge.View.Backward> implements ThingAdjacency.Write.In {

                public In(ThingVertex.Write owner) {
                    super(owner);
                }

                @Override
                ThingEdge.View.Backward getView(ThingEdge edge) {
                    return edge.backwardView();
                }

                @Override
                public InEdgeIterator edge(Encoding.Edge.Thing.Base encoding, IID... lookahead) {
                    return new InEdgeIteratorImpl(edgeIterator(encoding, List.of(lookahead)), owner, encoding);
                }

                @Override
                public InEdgeIterator edge(Encoding.Edge.Thing.Optimised encoding, TypeVertex roleType,
                                           IID... lookahead) {
                    return new InEdgeIteratorImpl(
                            edgeIterator(ROLEPLAYER, concat(roleType.iid(), lookahead)), owner, encoding, roleType
                    );
                }
            }

            public static class Out extends Persisted<ThingEdge.View.Forward> implements ThingAdjacency.Write.Out {

                public Out(ThingVertex.Write owner) {
                    super(owner);
                }

                @Override
                ThingEdge.View.Forward getView(ThingEdge edge) {
                    return edge.forwardView();
                }

                @Override
                public OutEdgeIterator edge(Encoding.Edge.Thing.Base encoding, IID... lookahead) {
                    return new OutEdgeIteratorImpl(edgeIterator(encoding, List.of(lookahead)), owner, encoding);
                }

                @Override
                public OutEdgeIterator edge(Encoding.Edge.Thing.Optimised encoding, TypeVertex roleType,
                                            IID... lookahead) {
                    return new OutEdgeIteratorImpl(
                            edgeIterator(ROLEPLAYER, concat(roleType.iid(), lookahead)), owner, encoding, roleType
                    );
                }
            }
        }
    }
}
