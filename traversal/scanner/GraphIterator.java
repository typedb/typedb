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
import java.util.function.Function;

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
    private final Vertex<?, ?> initial;
    private final SortedSet<ProcedureVertex<?, ?>> toTraverse;
    private final SortedSet<ProcedureVertex<?, ?>> toRevisit;
    private Direction direction;
    private IteratorState iteratorState;

    private enum Direction {TRAVERSE, REVISIT}

    private enum IteratorState {INIT, EMPTY, FETCHED, COMPLETED}

    public GraphIterator(GraphManager graphMgr, Vertex<?, ?> initial, GraphProcedure procedure,
                         Traversal.Parameters params, Set<Identifier.Variable.Retrievable> filter) {
        assert procedure.vertexCount() > 1;
        this.graphMgr = graphMgr;
        this.procedure = procedure;
        this.params = params;
        this.filter = filter;
        this.initial = initial;

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
                initialiseStarts();
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

    private void initialiseStarts() {
        toTraverse.addAll(procedure.startVertices());
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
            }
            edgeInputs.put(edge, fromVertex);
            reset();
        }

        private void removeEdgeInput(ProcedureEdge<?, ?> edge) {
            edgeInputIntersectionCache.remove(edgeInputs);
            edgeInputs.remove(edge);
            reset();
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
            clearCurrentVertex();
        }

        private void clearCurrentVertex() {
            vertex = null;
        }

        private Forwardable<Vertex<?, ?>, Order.Asc> getIterator() {
            assert procedureVertex.isStartingVertex() || !edgeInputs.isEmpty();
            if (iterator == null) {
                if (procedureVertex.equals(procedure.initialVertex())) iterator = createIteratorFromInitial();
                else if (procedureVertex.isStartingVertex()) iterator = createIteratorFromStart();
                else {
                    iterator = createIteratorFromEdges();
                    cachedIntersection().ifPresent(value -> iterator.forward(value));
                }
                // TODO: we may only need to find one valid answer if all dependents are not included in the filter and also find an answer
            }
            return iterator;
        }

        private Forwardable<Vertex<?, ?>, Order.Asc> createIteratorFromInitial() {
            if (procedureVertex.id().isScoped()) {
                Scopes.Scoped scoped = scopes.getOrInitialise(procedureVertex.id().asScoped().scope());
                scoped.record(procedureVertex, initial.asThing());
            }
            return iterateSorted(ASC, initial);
        }

        private Forwardable<Vertex<?, ?>, Order.Asc> createIteratorFromStart() {
            assert procedureVertex.isStartingVertex();
            if (procedureVertex.id().isScoped()) {
                Scopes.Scoped scoped = scopes.getOrInitialise(procedureVertex.id().asScoped().scope());
                return ((Forwardable<Vertex<?, ?>, ?>) procedureVertex.iterator(graphMgr, params)).mapSorted(v -> {
                    scoped.record(procedureVertex, v.asThing());
                    return v;
                }, v -> v, ASC);
            } else {
                return (Forwardable<Vertex<?, ?>, Order.Asc>) procedureVertex.iterator(graphMgr, params);
            }
        }

        private Forwardable<Vertex<?, ?>, Order.Asc> createIteratorFromEdges() {
            Map<Vertex<?, ?>, List<ProcedureEdge.Native.Thing.RolePlayer>> overlappingRPEdges = new HashMap<>();
            List<ProcedureEdge<?, ?>> otherEdges = new ArrayList<>();
            edgeInputs.forEach((edge, vertex) -> {
                if (edge.isRolePlayer()) {
                    overlappingRPEdges.computeIfAbsent(vertex, v -> new ArrayList<>()).add(edge.asRolePlayer());
                } else otherEdges.add(edge);
            });
            return intersectEdges(overlappingRPEdges, otherEdges);
        }

        private Forwardable<Vertex<?, ?>, Order.Asc> intersectEdges(Map<Vertex<?, ?>, List<ProcedureEdge.Native.Thing.RolePlayer>> rpEdges,
                                                                    List<ProcedureEdge<?, ?>> otherEdges) {
            List<Forwardable<Vertex<?, ?>, Order.Asc>> iterators = new ArrayList<>();
            otherEdges.forEach(e -> iterators.add(branch(edgeInputs.get(e), e)));
            rpEdges.forEach((vertex, edges) -> {
                if (edges.size() == 1) {
                    iterators.add(branch(edgeInputs.get(edges.get(0)), edges.get(0))
                            .mapSorted(KeyValue::key, thing -> KeyValue.of(thing.asThing(), null), ASC));
                } else iterators.add(intersectOverlapping(edges));
            });
            if (iterators.size() == 1) return iterators.get(0);
            else return intersect(iterate(iterators), ASC);
        }

        /**
         * Overlapping RolePlayer edges (ie. from the same vertex are either):
         * identical -- (upper: $x, upper: $y); $x is $y;
         * strict subsets/supersets of each other -- (role: $x, upper: $y); $x is $y; -- solved by sorting most restrictive iterators first
         * completely disjoint -- (upper: $x, lower: $x); $x isa $y;
         * It is impossible for two RolePlayer edges to return partially overlapping iterators.
         */
        private Forwardable<Vertex<?, ?>, Order.Asc> intersectOverlapping(List<ProcedureEdge.Native.Thing.RolePlayer> edges) {
            assert edges.size() > 1 && iterate(edges).map(edgeInputs::get).toSet().size() == 1;
            ArrayList<Forwardable<Vertex<?, ?>, Order.Asc>> iterators = new ArrayList<>(edges.size());
            edges.stream().sorted(Comparator.comparing(e -> e.roleTypes().size()))
                    .forEach(e -> {
                        // rare cases have RP edges in different directions from the same vertex, so look up scope each time
                        Scopes.Scoped scoped = scopes.getOrInitialise(e.scope());
                        iterators.add(branch(edgeInputs.get(e), e)
                                .filter(thingAndRole -> isRoleAvailable(thingAndRole.value(), e, scoped, edges))
                                .mapSorted(KeyValue::key, v -> KeyValue.of(v.asThing(), null), ASC));
                    });
            // WARN: we rely on the intersection operating in order of the iterators provided...
            return intersect(iterate(iterators), ASC);
        }

        private boolean isRoleAvailable(ThingVertex role, ProcedureEdge.Native.Thing.RolePlayer sourceEdge,
                                        Scopes.Scoped scope, List<ProcedureEdge.Native.Thing.RolePlayer> edges) {
            for (ProcedureEdge.Native.Thing.RolePlayer e : edges) {
                if (!e.equals(sourceEdge)) {
                    Optional<ThingVertex> otherRole = scope.getRole(e);
                    if (otherRole.isPresent() && otherRole.get().equals(role)) return false;
                }
            }
            return true;
        }

        private Optional<Vertex<?, ?>> cachedIntersection() {
            if (edgeInputIntersectionCache.isEmpty()) return Optional.empty();
            else return Optional.ofNullable(edgeInputIntersectionCache.get(edgeInputIntersectionCache.lastKey()));
        }

        private Forwardable<Vertex<?, ?>, Order.Asc> branch(Vertex<?, ?> fromVertex, ProcedureEdge<?, ?> edge) {
            assert !edge.isRolePlayer();
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
            } else {
                toIter = edge.branch(graphMgr, fromVertex, params);
            }

            return (Forwardable<Vertex<?, ?>, Order.Asc>) toIter;
        }

        private Forwardable<KeyValue<ThingVertex, ThingVertex>, Order.Asc> branch(Vertex<?, ?> fromVertex,
                                                                                  ProcedureEdge.Native.Thing.RolePlayer edge) {
            Identifier.Variable scope = edge.asRolePlayer().scope();
            Scopes.Scoped scoped = scopes.getOrInitialise(scope);
            return edge.asRolePlayer().branchEdge(graphMgr, fromVertex, params)
                    .mapSorted(
                            thingAndRole -> {
                                scoped.record(edge, thingAndRole.value());
                                return thingAndRole;
                            },
                            Function.identity(),
                            ASC
                    );
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

            public void record(ProcedureEdge<?, ?> source, ThingVertex role) {
                assert source.isRolePlayer() && role.type().isRoleType();
                edgeSources.put(source, role);
            }

            public void record(ProcedureVertex<?, ?> source, ThingVertex role) {
                assert source.id().isScoped() && role.type().isRoleType();
                vertexSources.put(source, role);
            }

            private Optional<ThingVertex> getRole(ProcedureEdge<?, ?> edge) {
                return Optional.ofNullable(edgeSources.get(edge));
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
