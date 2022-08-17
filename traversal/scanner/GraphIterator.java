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
import com.vaticle.typedb.core.traversal.graph.TraversalEdge;
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
import java.util.SortedSet;
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
    private final Map<Identifier.Variable, Scope> scopes;
    private final Map<ProcedureVertex<?, ?>, VertexTraverser> vertexTraversers;
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
        this.toTraverse = new TreeSet<>(Comparator.comparing(ProcedureVertex::order));
        this.toRevisit = new TreeSet<>(Comparator.comparing(ProcedureVertex::order));
        this.scopes = new HashMap<>();
        this.vertexTraversers = new HashMap<>();
        setup();
        this.iteratorState = IteratorState.INIT;
    }

    private void setup() {
        for (int pos = 0; pos < procedure.vertexCount(); pos++) {
            ProcedureVertex<?, ?> procedureVertex = procedure.vertex(pos);
            VertexTraverser traverser = new VertexTraverser(procedureVertex);
            vertexTraversers.put(procedureVertex, traverser);
            if (procedureVertex.isThing() && procedureVertex.asThing().isScope()) {
                scopes.put(procedureVertex.id().asVariable(), new Scope());
            }
        }
        iterate(procedure.vertices()).filter(v -> v.isThing() && v.asThing().isScope())
                .forEachRemaining(v -> setupImplicitDependencies(v.outs()));
    }

    private void setupImplicitDependencies(Set<ProcedureEdge<?, ?>> edges) {
        assert iterate(edges).map(TraversalEdge::from).toSet().size() <= 1;
        Set<ProcedureEdge<?, ?>> forwardRPs = iterate(edges).filter(e -> e.isRolePlayer() && e.direction().isForward()).toSet();
        iterate(forwardRPs).forEachRemaining(rp1 ->
                iterate(forwardRPs).forEachRemaining(rp2 -> {
                    if (rp1.to().order() < rp2.to().order() && rp1.asRolePlayer().overlaps(rp2.asRolePlayer(), params)) {
                        setupImplicitDependency(rp1.to(), rp2.to());
                    }
                })
        );
    }

    private void setupImplicitDependency(ProcedureVertex<?, ?> from, ProcedureVertex<?, ?> to) {
        vertexTraversers.get(from).implicitDependents.add(to);
        vertexTraversers.get(to).implicitDependees.add(from);
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
                traverseVertex(vertex);
            } else {
                toRevisit.remove(vertex = toRevisit.last());
                revisit(vertex);
            }
        }
        return direction == Direction.TRAVERSE;
    }

    private void traverseVertex(ProcedureVertex<?, ?> procedureVertex) {
        VertexTraverser vertexTraverser = vertexTraversers.get(procedureVertex);
        if (vertexTraverser.findNextVertex()) {
            vertexTraverser.procedureVertex.outs().forEach(e -> {
                toTraverse.add(e.to());
                vertexTraversers.get(e.to()).reset();
            });
            vertexTraverser.implicitDependents.forEach(v -> {
                toTraverse.add(v);
                vertexTraversers.get(v).reset();
            });
        } else {
            vertexTraverser.clear();
            vertexTraverser.procedureVertex.ins().forEach(e -> recordRevisit(procedureVertex, e.from()));
            vertexTraverser.implicitDependees.forEach(toRevisit::add);
            toTraverse.add(procedureVertex);
            direction = Direction.REVISIT;
        }
    }

    private void recordRevisit(ProcedureVertex<?, ?> from, ProcedureVertex<?, ?> revisit) {
        toRevisit.add(revisit);
        iterate(vertexTraversers.get(revisit).implicitDependents)
                .filter(v -> v.order() < from.order()).forEachRemaining(toRevisit::add);
    }

    private void revisit(ProcedureVertex<?, ?> procedureVertex) {
        toTraverse.add(procedureVertex);
        direction = Direction.TRAVERSE;
    }

    @Override
    public void recycle() {
        vertexTraversers.values().forEach(VertexTraverser::clear);
    }

    private class VertexTraverser {

        private final ProcedureVertex<?, ?> procedureVertex;
        private final Set<ProcedureVertex<?, ?>> implicitDependents;
        private final Set<ProcedureVertex<?, ?>> implicitDependees;
        private Forwardable<Vertex<?, ?>, Order.Asc> iterator;
        private Vertex<?, ?> vertex;

        private VertexTraverser(ProcedureVertex<?, ?> procedureVertex) {
            this.procedureVertex = procedureVertex;
            this.implicitDependents = new HashSet<>();
            this.implicitDependees = new HashSet<>();
        }

        private boolean findNextVertex() {
            Forwardable<Vertex<?, ?>, Order.Asc> iterator = getIterator();
            while (iterator.hasNext()) {
                vertex = getIterator().next();
                if (verifyLoops()) return true;
            }
            return false;
        }

        private Vertex<?, ?> vertex() {
            return vertex;
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
                Scope scope = scopes.get(edge.asRolePlayer().scope());
                return edge.asRolePlayer().isClosure(graphMgr, fromVertex, toVertex, params, scope);
            } else {
                return edge.isClosure(graphMgr, fromVertex, toVertex, params);
            }
        }

        private void clear() {
            reset();
            if (procedureVertex.id().isScoped()) {
                scopes.get(procedureVertex.id().asScoped().scope()).remove(procedureVertex);
            } else {
                iterate(procedureVertex.ins()).filter(ProcedureEdge::isRolePlayer).forEachRemaining(e -> {
                    Scope scope = scopes.get(e.asRolePlayer().scope());
                    scope.remove(e);
                });
            }
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
            if (iterator == null) {
                if (procedureVertex.equals(procedure.initialVertex())) iterator = createIteratorFromInitial();
                else if (procedureVertex.isStartingVertex()) iterator = createIteratorFromStart();
                else iterator = createIteratorFromEdges();
                // TODO: we may only need to find one valid answer if all dependents are not included in the filter and also find an answer
            }
            return iterator;
        }

        private Forwardable<Vertex<?, ?>, Order.Asc> createIteratorFromInitial() {
            if (procedureVertex.id().isScoped()) {
                Scope scope = scopes.get(procedureVertex.id().asScoped().scope());
                scope.record(procedureVertex, initial.asThing());
            }
            return iterateSorted(ASC, initial);
        }

        private Forwardable<Vertex<?, ?>, Order.Asc> createIteratorFromStart() {
            assert procedureVertex.isStartingVertex();
            if (procedureVertex.id().isScoped()) {
                Scope scope = scopes.get(procedureVertex.id().asScoped().scope());
                return ((Forwardable<Vertex<?, ?>, ?>) procedureVertex.iterator(graphMgr, params)).mapSorted(v -> {
                    scope.record(procedureVertex, v.asThing());
                    return v;
                }, v -> v, ASC);
            } else {
                return (Forwardable<Vertex<?, ?>, Order.Asc>) procedureVertex.iterator(graphMgr, params);
            }
        }

        private Forwardable<Vertex<?, ?>, Order.Asc> createIteratorFromEdges() {
            List<Forwardable<Vertex<?, ?>, Order.Asc>> iterators = new ArrayList<>();
            procedureVertex.ins().forEach(edge -> iterators.add(branch(vertexTraversers.get(edge.from()).vertex(), edge)));
            if (iterators.size() == 1) return iterators.get(0);
            else return intersect(iterate(iterators), ASC);
        }

        private Forwardable<Vertex<?, ?>, Order.Asc> branch(Vertex<?, ?> fromVertex, ProcedureEdge<?, ?> edge) {
            Forwardable<? extends Vertex<?, ?>, Order.Asc> toIter;
            if (edge.to().id().isScoped()) {
                Scope scope = scopes.get(procedureVertex.id().asScoped().scope());
                toIter = edge.branch(graphMgr, fromVertex, params)
                        .filter(role -> {
                            Optional<ProcedureVertex<?, ?>> source = scope.getRoleVertexSource(role.asThing());
                            return source.isEmpty();
                        }).mapSorted(
                                role -> {
                                    scope.record(procedureVertex, role.asThing());
                                    return role;
                                },
                                role -> role,
                                ASC
                        );
            } else if (edge.isRolePlayer()) {
                Scope scope = scopes.get(edge.asRolePlayer().scope());
                toIter = edge.asRolePlayer().branchEdge(graphMgr, fromVertex, params)
                        .filter(thingAndRole -> {
                            Optional<ProcedureEdge<?, ?>> source = scope.getRoleEdgeSource(thingAndRole.value());
                            return source.isEmpty();
                        }).mapSorted(
                                thingAndRole -> {
                                    scope.record(edge, thingAndRole.value());
                                    return thingAndRole.key();
                                },
                                thing -> KeyValue.of(thing, null),
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

    public static class Scope {

        private final Map<ProcedureVertex<?, ?>, ThingVertex> vertexSources;
        private final Map<ProcedureEdge<?, ?>, ThingVertex> edgeSources;

        private Scope() {
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

        public Optional<ProcedureVertex<?, ?>> getRoleVertexSource(ThingVertex role) {
            for (Map.Entry<ProcedureVertex<?, ?>, ThingVertex> entry : vertexSources.entrySet()) {
                if (entry.getValue().equals(role)) return Optional.of(entry.getKey());
            }
            return Optional.empty();
        }

        public Optional<ProcedureEdge<?, ?>> getRoleEdgeSource(ThingVertex role) {
            for (Map.Entry<ProcedureEdge<?, ?>, ThingVertex> entry : edgeSources.entrySet()) {
                if (entry.getValue().equals(role)) return Optional.of(entry.getKey());
            }
            return Optional.empty();
        }

        public void remove(ProcedureEdge<?, ?> edgeSource) {
            edgeSources.remove(edgeSource);
        }

        public void remove(ProcedureVertex<?, ?> vertexSource) {
            vertexSources.remove(vertexSource);
        }
    }
}
