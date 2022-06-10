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

package com.vaticle.typedb.core.traversal.scanner;

import com.vaticle.typedb.core.common.collection.KeyValue;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.AbstractFunctionalIterator;
import com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.Forwardable;
import com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.Order;
import com.vaticle.typedb.core.graph.GraphManager;
import com.vaticle.typedb.core.graph.vertex.ThingVertex;
import com.vaticle.typedb.core.graph.vertex.Vertex;
import com.vaticle.typedb.core.traversal.Traversal;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typedb.core.traversal.common.VertexMap;
import com.vaticle.typedb.core.traversal.procedure.GraphProcedure;
import com.vaticle.typedb.core.traversal.procedure.ProcedureEdge;
import com.vaticle.typedb.core.traversal.procedure.ProcedureVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.RESOURCE_CLOSED;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.ASC;
import static com.vaticle.typedb.core.common.iterator.sorted.SortedIterators.Forwardable.intersect;
import static com.vaticle.typedb.core.common.iterator.sorted.SortedIterators.Forwardable.iterateSorted;

public class GraphIterator extends AbstractFunctionalIterator<VertexMap> {

    private static final Logger LOG = LoggerFactory.getLogger(GraphIterator.class);

    private final GraphManager graphMgr;
    private final GraphProcedure procedure;
    private final Traversal.Parameters params;
    private final Set<Identifier.Variable.Retrievable> filter;
    private final Map<ProcedureVertex<?, ?>, VertexTraverser> vertexTraversers;
    private final Scopes scopes;
    private final Vertex<?, ?> start;
    private final SortedSet<ProcedureVertex<?, ?>> toTraverse;
    private final SortedSet<ProcedureVertex<?, ?>> toRevisit;
    private Direction direction;
    private IteratorState iteratorState;

    private enum Direction {TRAVERSE, REVISIT}

    private enum IteratorState {INIT, EMPTY, FETCHED, COMPLETED}

    public GraphIterator(GraphManager graphMgr, Vertex<?, ?> start, GraphProcedure procedure,
                         Traversal.Parameters params, Set<Identifier.Variable.Retrievable> filter) {
        assert procedure.vertexCount() > 1;
        this.graphMgr = graphMgr;
        this.procedure = procedure;
        this.params = params;
        this.filter = filter;
        this.start = start;

        this.iteratorState = IteratorState.INIT;
        this.scopes = new Scopes();
        this.toTraverse = new TreeSet<>(Comparator.comparing(ProcedureVertex::order));
        this.toRevisit = new TreeSet<>(Comparator.comparing(ProcedureVertex::order));
        this.vertexTraversers = new HashMap<>();
        for (ProcedureVertex<?, ?> procedureVertex : procedure.vertices()) {
            vertexTraversers.put(procedureVertex, new VertexTraverser(procedureVertex));
        }
    }

    @Override
    public VertexMap next() {
        if (!hasNext()) throw new NoSuchElementException();
        iteratorState = IteratorState.EMPTY;
        return toVertexMap();
    }

    private VertexMap toVertexMap() {
        Map<Identifier.Variable.Retrievable, Vertex<?, ?>> answer = new HashMap<>();
        for (ProcedureVertex<?, ?> procedureVertex : procedure.vertices()) {
            if (procedureVertex.id().isRetrievable() && filter.contains(procedureVertex.id().asVariable().asRetrievable())) {
                answer.put(procedureVertex.id().asVariable().asRetrievable(), vertexTraversers.get(procedureVertex).vertex());
            }
        }

        return VertexMap.of(answer);
    }

    @Override
    public boolean hasNext() {
        try {
            if (iteratorState == IteratorState.COMPLETED) return false;
            else if (iteratorState == IteratorState.FETCHED) return true;
            else if (iteratorState == IteratorState.INIT) {
                initialiseStart();
                if (computeAnswer()) iteratorState = IteratorState.FETCHED;
                else setCompleted();
            } else if (iteratorState == IteratorState.EMPTY) {
                initialiseEnds();
                if (computeAnswer()) iteratorState = IteratorState.FETCHED;
                else setCompleted();
            } else {
                throw TypeDBException.of(ILLEGAL_STATE);
            }
            return iteratorState == IteratorState.FETCHED;
        } catch (Throwable e) {
            // note: catching runtime exception until we can gracefully interrupt running queries on tx close
            if (e instanceof TypeDBException && ((TypeDBException) e).code().isPresent() &&
                    ((TypeDBException) e).code().get().equals(RESOURCE_CLOSED.code())) {
                LOG.debug("Transaction was closed during graph iteration");
            } else {
                LOG.error("Parameters: " + params.toString());
                LOG.error("GraphProcedure: " + procedure.toString());
            }
            throw e;
        }
    }

    private void setCompleted() {
        iteratorState = IteratorState.COMPLETED;
        recycle();
    }

    private void initialiseStart() {
        toTraverse.add(procedure.startVertex());
        direction = Direction.TRAVERSE;
    }

    private void initialiseEnds() {
        assert toTraverse.isEmpty();
        toRevisit.addAll(procedure.endVertices());
        direction = Direction.REVISIT;
    }

    private boolean computeAnswer() {
        while ((direction == Direction.TRAVERSE && !toTraverse.isEmpty()) ||
                (direction == Direction.REVISIT && !toRevisit.isEmpty())) {
            ProcedureVertex<?, ?> vertex;
            if (direction == Direction.TRAVERSE) {
                toTraverse.remove(vertex = toTraverse.first());
                traverse(vertex);
            } else {
                toRevisit.remove(vertex = toRevisit.last());
                revisit(vertex);
            }
        }
        return direction == Direction.TRAVERSE;
    }

    private void traverse(ProcedureVertex<?, ?> procedureVertex) {
        VertexTraverser vertexTraverser = vertexTraversers.get(procedureVertex);
        boolean verified = false;
        Set<ProcedureVertex<?, ?>> verifyFailureCauses = new HashSet<>();
        while (!verified && vertexTraverser.hasNextVertex()) {
            vertexTraverser.takeNextVertex();
            if (vertexTraverser.verifyLoops()
                    && verifyTraversedScopes(procedureVertex, verifyFailureCauses)
                    && verifyEdgeLookahead(procedureVertex, verifyFailureCauses)) {
                verified = true;
            } else {
                vertexTraverser.clearCurrentVertex();
                removeFromSuccessors(procedureVertex);
            }
        }
        toRevisit.addAll(verifyFailureCauses);
        if (verified) procedureVertex.outs().forEach(edge -> toTraverse.add(edge.to()));
        else failed(procedureVertex);
    }

    private void removeFromSuccessors(ProcedureVertex<?, ?> procedureVertex) {
        procedureVertex.outs().forEach(edge -> {
            vertexTraversers.get(edge.to()).removeEdgeInput(edge);
            removeFromSuccessors(edge.to());
        });
    }

    private void failed(ProcedureVertex<?, ?> procedureVertex) {
        procedureVertex.ins().forEach(edge -> toRevisit.add(edge.from()));
        vertexTraversers.get(procedureVertex).reset();
        toTraverse.add(procedureVertex);
        direction = Direction.REVISIT;
    }

    private boolean verifyTraversedScopes(ProcedureVertex<?, ?> procedureVertex, Set<ProcedureVertex<?, ?>> verifyFailureCauses) {
        if (procedureVertex.id().isScoped()) {
            Scopes.Scoped scoped = scopes.get(procedureVertex.id().asScoped().scope());
            return verifyScopedUpToOrder(scoped, procedureVertex.order(), verifyFailureCauses);
        } else if (procedureVertex.isThing() && !procedureVertex.asThing().inRolePlayers().isEmpty()) {
            for (ProcedureEdge<?, ?> edge : procedureVertex.asThing().inRolePlayers()) {
                assert edge.isRolePlayer();
                Scopes.Scoped scoped = scopes.get(edge.asRolePlayer().scope());
                if (!verifyScopedUpToOrder(scoped, procedureVertex.order(), verifyFailureCauses)) return false;
            }
        }
        return true;
    }

    private boolean verifyScopedUpToOrder(Scopes.Scoped scoped, int order, Set<ProcedureVertex<?, ?>> verifyFailureCauses) {
        if (!scoped.isValidUpTo(order)) {
            scoped.scopedOrdersUpTo(order).forEach(scopedOrder -> {
                if (scopedOrder != order) verifyFailureCauses.add(procedure.vertex(scopedOrder));
            });
            return false;
        }
        return true;
    }

    private boolean verifyEdgeLookahead(ProcedureVertex<?, ?> procedureVertex, Set<ProcedureVertex<?, ?>> verifyFailureCauses) {
        VertexTraverser vertexTraverser = vertexTraversers.get(procedureVertex);
        Set<ProcedureEdge<?, ?>> verified = new HashSet<>();
        for (ProcedureEdge<?, ?> edge : procedureVertex.orderedOuts()) {
            VertexTraverser toVertexTraverser = vertexTraversers.get(edge.to());
            toVertexTraverser.addEdgeInput(edge, vertexTraverser.vertex());
            verified.add(edge);
            if (!toVertexTraverser.hasNextVertex()) {
                verified.forEach(e -> vertexTraversers.get(e.to()).removeEdgeInput(e));
                toVertexTraverser.edgeInputs().forEach(e -> verifyFailureCauses.add(e.from()));
                return false;
            }
        }
        return true;
    }

    private void revisit(ProcedureVertex<?, ?> procedureVertex) {
        removeFromSuccessors(procedureVertex);
        iterate(procedureVertex.transitiveOuts()).map(ProcedureEdge::to).forEachRemaining(toTraverse::remove);
        toTraverse.add(procedureVertex);
        direction = Direction.TRAVERSE;
    }

    @Override
    public void recycle() {
        vertexTraversers.values().forEach(VertexTraverser::clear);
    }

    private class VertexTraverser {

        private final ProcedureVertex<?, ?> procedureVertex;
        private final Map<ProcedureEdge<?, ?>, Vertex<?, ?>> edgeInputs;
        private final SortedMap<Map<ProcedureEdge<?, ?>, Vertex<?, ?>>, Vertex<?, ?>> edgeInputIntersectionCache;
        private Forwardable<Vertex<?, ?>, Order.Asc> iterator;
        private Vertex<?, ?> vertex;

        private VertexTraverser(ProcedureVertex<?, ?> procedureVertex) {
            this.procedureVertex = procedureVertex;
            this.edgeInputs = new HashMap<>();
            this.edgeInputIntersectionCache = new TreeMap<>(Comparator.comparing(Map::size));
        }

        private void takeNextVertex() {
            assert hasNextVertex();
            vertex = getIterator().next();
        }

        private boolean hasNextVertex() {
            return getIterator().hasNext();
        }

        private Vertex<?, ?> vertex() {
            return vertex;
        }

        private Set<ProcedureEdge<?, ?>> edgeInputs() {
            return edgeInputs.keySet();
        }

        private boolean verifyLoops() {
            assert vertex != null;
            for (ProcedureEdge<?, ?> edge : procedureVertex.loops()) {
                if (!isClosure(edge, vertex, vertex)) return false;
            }
            return true;
        }

        private boolean isClosure(ProcedureEdge<?, ?> edge, Vertex<?, ?> fromVertex, Vertex<?, ?> toVertex) {
            if (edge.isRolePlayer()) {
                Scopes.Scoped scoped = scopes.getOrInitialise(edge.asRolePlayer().scope());
                return edge.asRolePlayer().isClosure(graphMgr, fromVertex, toVertex, params, scoped);
            } else {
                return edge.isClosure(graphMgr, fromVertex, toVertex, params);
            }
        }

        private void addEdgeInput(ProcedureEdge<?, ?> edge, Vertex<?, ?> fromVertex) {
            assert !edgeInputs.containsKey(edge);
            if (iterator != null && iterator.hasNext()) {
                edgeInputIntersectionCache.put(new HashMap<>(edgeInputs), iterator.peek());
                iterator = intersect(branch(fromVertex, edge), iterator);
            }
            edgeInputs.put(edge, fromVertex);
            vertex = null;
        }

        private void removeEdgeInput(ProcedureEdge<?, ?> edge) {
            edgeInputIntersectionCache.remove(edgeInputs);
            edgeInputs.remove(edge);
            reset();
        }

        public void clearCurrentVertex() {
            vertex = null;
        }

        private void clear() {
            edgeInputs.clear();
            edgeInputIntersectionCache.clear();
            reset();
        }

        private void reset() {
            if (iterator != null) {
                iterator.recycle();
                iterator = null;
            }
            vertex = null;
        }

        private Forwardable<Vertex<?, ?>, Order.Asc> getIterator() {
            assert procedureVertex.isStartingVertex() || !edgeInputs.isEmpty();
            if (iterator == null) {
                if (procedureVertex.isStartingVertex()) iterator = createIteratorFromStart();
                else {
                    iterator = createIteratorFromEdges();
                    lastIntersection().ifPresent(value -> iterator.forward(value));
                }
            }
            return iterator;
        }

        private Forwardable<Vertex<?, ?>, Order.Asc> createIteratorFromStart() {
            if (procedure.startVertex().id().isScoped()) {
                Scopes.Scoped scoped = scopes.getOrInitialise(procedure.startVertex().id().asScoped().scope());
                scoped.record(procedure.startVertex(), start.asThing().asThing());
            }
            return iterateSorted(ASC, start);
        }

        private Forwardable<Vertex<?, ?>, Order.Asc> createIteratorFromEdges() {
            List<Forwardable<Vertex<?, ?>, Order.Asc>> iters = new ArrayList<>();
            edgeInputs.forEach((procedureEdge, vertex) -> iters.add(branch(vertex, procedureEdge)));
            if (iters.size() == 1) return iters.get(0);
            else return intersect(iterate(iters), ASC);
        }

        private Optional<Vertex<?, ?>> lastIntersection() {
            if (edgeInputIntersectionCache.isEmpty()) return Optional.empty();
            else return Optional.ofNullable(edgeInputIntersectionCache.get(edgeInputIntersectionCache.lastKey()));
        }

        private Forwardable<Vertex<?, ?>, Order.Asc> branch(Vertex<?, ?> fromVertex, ProcedureEdge<?, ?> edge) {
            Forwardable<? extends Vertex<?, ?>, Order.Asc> toIter;
            if (edge.to().id().isScoped()) {
                Identifier.Variable scope = edge.to().id().asScoped().scope();
                Scopes.Scoped scoped = scopes.getOrInitialise(scope);
                toIter = edge.branch(graphMgr, fromVertex, params).mapSorted(
                        role -> {
                            scoped.record(edge.to(), role.asThing().asThing());
                            return role;
                        },
                        role -> role,
                        ASC
                );
            } else if (edge.isRolePlayer()) {
                Identifier.Variable scope = edge.asRolePlayer().scope();
                Scopes.Scoped scoped = scopes.getOrInitialise(scope);
                toIter = edge.asRolePlayer().branchEdge(graphMgr, fromVertex, params).mapSorted(
                        thingAndRole -> {
                            scoped.record(edge, thingAndRole.value());
                            return thingAndRole.key();
                        },
                        key -> KeyValue.of(key, null),
                        ASC
                );
            } else {
                toIter = edge.branch(graphMgr, fromVertex, params);
            }
            if (!edge.to().id().isName() && edge.to().outs().isEmpty() && edge.to().ins().size() == 1) {
                // TODO: This optimisation can apply to more situations, such as to
                //       an entire tree, where none of the leaves are referenced by name
                toIter = toIter.limit(1);
            }

            // TODO: this cast is a tradeoff between losing a lot of type safety in the ProcedureEdge branch() return type vs having a cast
            return (Forwardable<Vertex<?, ?>, Order.Asc>) toIter;
        }

    }

    public static class Scopes {

        private final Map<Identifier.Variable, Scoped> scoped;

        private Scopes() {
            this.scoped = new HashMap<>();
        }

        private Scoped getOrInitialise(Identifier.Variable scope) {
            return scoped.computeIfAbsent(scope, s -> new Scoped());
        }

        private Scoped get(Identifier.Variable scope) {
            assert scoped.containsKey(scope);
            return scoped.get(scope);
        }

        public static class Scoped {

            private final Map<ProcedureVertex<?, ?>, ThingVertex> vertexSources;
            private final Map<ProcedureEdge<?, ?>, ThingVertex> edgeSources;

            private Scoped() {
                vertexSources = new HashMap<>();
                edgeSources = new HashMap<>();
            }

            private boolean containsSource(ProcedureVertex<?, ?> vertex) {
                return vertexSources.containsKey(vertex);
            }

            private boolean containsSource(ProcedureEdge<?, ?> edge) {
                return edgeSources.containsKey(edge);
            }

            public void record(ProcedureEdge<?, ?> source, ThingVertex role) {
                assert source.isRolePlayer() && role.type().isRoleType();
                edgeSources.put(source, role);
            }

            public void record(ProcedureVertex<?, ?> source, ThingVertex role) {
                assert source.id().isScoped() && role.type().isRoleType();
                vertexSources.put(source, role);
            }

            private boolean isValidUpTo(int order) {
                Set<ThingVertex> roles = new HashSet<>();
                for (Map.Entry<ProcedureEdge<?, ?>, ThingVertex> entry : edgeSources.entrySet()) {
                    if (entry.getKey().to().order() <= order) {
                        if (roles.contains(entry.getValue())) return false;
                        else roles.add(entry.getValue());
                    }
                }
                for (Map.Entry<ProcedureVertex<?, ?>, ThingVertex> entry : vertexSources.entrySet()) {
                    if (entry.getKey().order() <= order) {
                        if (roles.contains(entry.getValue())) return false;
                        else roles.add(entry.getValue());
                    }
                }
                return true;
            }

            private Set<Integer> scopedOrdersUpTo(int order) {
                Set<Integer> orders = new HashSet<>();
                for (Map.Entry<ProcedureEdge<?, ?>, ThingVertex> entry : edgeSources.entrySet()) {
                    if (entry.getKey().to().order() <= order) {
                        orders.add(entry.getKey().to().order());
                    }
                }
                for (Map.Entry<ProcedureVertex<?, ?>, ThingVertex> entry : vertexSources.entrySet()) {
                    if (entry.getKey().order() <= order) {
                        orders.add(entry.getKey().order());
                    }
                }
                return orders;
            }
        }
    }
}
