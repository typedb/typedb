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
import java.util.SortedSet;
import java.util.TreeSet;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.RESOURCE_CLOSED;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.ASC;
import static com.vaticle.typedb.core.common.iterator.sorted.SortedIterators.Forwardable.intersect;
import static com.vaticle.typedb.core.common.iterator.sorted.SortedIterators.Forwardable.iterateSorted;

/**
 * We have to include the optimisation to only find an answer if a connected predecessor or successor is retrieved
 *  and we only have to find 1 answer if all successors are not filtered in
 */
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
        // set up scopes
        iterate(procedure.vertices()).filter(v -> v.isThing() && v.asThing().isScope())
                .forEachRemaining(v -> scopes.put(v.id().asVariable(), new Scope()));
        // set up traversers
        iterate(procedure.vertices()).forEachRemaining(v -> vertexTraversers.put(v, new VertexTraverser(v)));
        // add implicit dependencies between traversers
        iterate(procedure.vertices()).filter(v -> v.isThing() && v.asThing().isScope())
                .forEachRemaining(this::setupImplicitDependencies);
    }

    /**
     * We make explicit hidden dependencies caused by filtering roles through scopes. A vertex that must backtrack
     * to another vertex in order to find permutations of role instances is annotated with an implicit dependency.
     *
     * Specifically, this occurs when traversing over two sibling edges that can take on an overlapping set of roles.
     * In this case, the two destinations vertices have an implicit dependency.
     * It can also happen between a pair of vertices that can take on overlapping Role instances.
     */
    private void setupImplicitDependencies(ProcedureVertex<?, ?> vertex) {
        Set<ProcedureEdge<?, ?>> rpEdges = iterate(vertex.outs()).filter(e -> e.isRolePlayer() && e.direction().isForward()).toSet();
        iterate(rpEdges).forEachRemaining(rp1 -> iterate(rpEdges).forEachRemaining(rp2 -> {
            if (rp1.to().order() < rp2.to().order() && rp1.asRolePlayer().overlaps(rp2.asRolePlayer(), params)) {
                setupImplicitDependency(rp1.to(), rp2.to());
            }
        }));
        Set<ProcedureVertex.Thing> roleVerticesOut = iterate(vertex.outs()).filter(e -> e.isRelating() && e.direction().isForward())
                .map(e -> e.asRelating().to()).toSet();
        iterate(roleVerticesOut).forEachRemaining(v1 -> iterate(roleVerticesOut).forEachRemaining(v2 -> {
            if (v1.order() < v2.order() && v1.overlaps(v2, params)) setupImplicitDependency(v1, v2);
        }));
        Set<ProcedureVertex.Thing> roleVerticesIn = iterate(vertex.ins()).filter(e -> e.isRelating() && e.direction().isBackward())
                .map(e -> e.asRelating().from()).toSet();
        iterate(roleVerticesIn).forEachRemaining(v1 -> iterate(roleVerticesIn).forEachRemaining(v2 -> {
            if (v1.order() < v2.order() && v1.overlaps(v2, params)) setupImplicitDependency(v1, v2);
        }));
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
        if (vertexTraverser.findNextVertex()) success(vertexTraverser);
        else failed(vertexTraverser);
    }

    private void success(VertexTraverser vertexTraverser) {
        for (ProcedureVertex<?, ?> vertex : vertexTraverser.procedureVertex.dependents()) {
            toTraverse.add(vertex);
            vertexTraversers.get(vertex).clear();
        }
    }

    private void failed(VertexTraverser vertexTraverser) {
        toRevisit.addAll(vertexTraverser.procedureVertex.dependees());
        toRevisit.addAll(vertexTraverser.implicitDependees);
        vertexTraverser.clear();
        toTraverse.add(vertexTraverser.procedureVertex);
        direction = Direction.REVISIT;
    }

    private void revisit(ProcedureVertex<?, ?> procedureVertex) {
        toTraverse.add(procedureVertex);
        vertexTraversers.get(procedureVertex).implicitDependents.forEach(implicit -> vertexTraversers.get(implicit).clear());
        direction = Direction.TRAVERSE;
    }

    @Override
    public void recycle() {
        vertexTraversers.values().forEach(VertexTraverser::clear);
    }

    private class VertexTraverser {

        private final ProcedureVertex<?, ?> procedureVertex;
        private final Scope localScope;
        private final Set<ProcedureVertex<?, ?>> implicitDependents;
        private final Set<ProcedureVertex<?, ?>> implicitDependees;
        private Forwardable<Vertex<?, ?>, Order.Asc> iterator;
        private Vertex<?, ?> vertex;

        private VertexTraverser(ProcedureVertex<?, ?> procedureVertex) {
            this.procedureVertex = procedureVertex;
            this.localScope = procedureVertex.id().isScoped() ? scopes.get(procedureVertex.id().asScoped().scope()) : null;
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

        void clear() {
            if (iterator != null) {
                iterator.recycle();
                iterator = null;
            }
            clearCurrentVertex();
            clearScopes();
        }

        private void clearCurrentVertex() {
            vertex = null;
        }

        private void clearScopes() {
            if (localScope != null) localScope.removeSource(procedureVertex);
            else {
                for (ProcedureEdge<?, ?> edge : procedureVertex.ins()) {
                    if (edge.isRolePlayer()) {
                        Scope scope = scopes.get(edge.asRolePlayer().scope());
                        scope.removeSource(edge);
                    }
                }
            }
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
            if (localScope != null) localScope.record(procedureVertex, initial.asThing());
            return iterateSorted(ASC, initial);
        }

        private Forwardable<Vertex<?, ?>, Order.Asc> createIteratorFromStart() {
            assert procedureVertex.isStartingVertex();
            if (procedureVertex.id().isScoped()) {
                return applyLocalScope((Forwardable<Vertex<?, ?>, Order.Asc>) procedureVertex.iterator(graphMgr, params));
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
            if (procedureVertex.id().isScoped()) {
                return applyLocalScope((Forwardable<Vertex<?, ?>, Order.Asc>) edge.branch(graphMgr, fromVertex, params));
            } else if (edge.isRolePlayer()) {
                return applyEdgeScope(edge.asRolePlayer().branchEdge(graphMgr, fromVertex, params), edge);
            } else {
                return (Forwardable<Vertex<?, ?>, Order.Asc>) edge.branch(graphMgr, fromVertex, params);
            }
        }

        private Forwardable<Vertex<?, ?>, Order.Asc> applyLocalScope(Forwardable<Vertex<?, ?>, Order.Asc> roles) {
            return roles.filter(role -> {
                Optional<ProcedureVertex<?, ?>> source = localScope.getRoleVertexSource(role.asThing());
                return source.isEmpty() || source.get().equals(procedureVertex);
            }).mapSorted(
                    role -> {
                        localScope.record(procedureVertex, role.asThing());
                        return role;
                    },
                    Vertex::asThing,
                    ASC
            );
        }

        private Forwardable<Vertex<?, ?>, Order.Asc> applyEdgeScope(Forwardable<KeyValue<ThingVertex, ThingVertex>, Order.Asc> iterator,
                                                                    ProcedureEdge<?, ?> edge) {
            Scope scope = scopes.get(edge.asRolePlayer().scope());
            return iterator.filter(thingAndRole -> {
                Optional<ProcedureEdge<?, ?>> source = scope.getRoleEdgeSource(thingAndRole.value());
                return source.isEmpty();
            }).mapSorted(
                    thingAndRole -> {
                        scope.record(edge, thingAndRole.value());
                        return thingAndRole.key();
                    },
                    thing -> KeyValue.of(thing.asThing(), null),
                    ASC
            );
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

        public void removeSource(ProcedureEdge<?, ?> edgeSource) {
            edgeSources.remove(edgeSource);
        }

        public void removeSource(ProcedureVertex<?, ?> vertexSource) {
            vertexSources.remove(vertexSource);
        }
    }
}
