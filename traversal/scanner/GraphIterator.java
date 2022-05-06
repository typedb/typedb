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
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
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
    private final VertexScanner[] vertexScanners;
    private final TreeSet<Integer> forward;
    private final TreeSet<Integer> backward;
    private final Scopes scopes;
    private final Vertex<?, ?> start;
    private State state;
    private StepDirection stepDirection;

    private enum State {INIT, EMPTY, FETCHED, COMPLETED}

    private enum StepDirection {FORWARD, BACKWARD}

    public GraphIterator(GraphManager graphMgr, Vertex<?, ?> start, GraphProcedure procedure,
                         Traversal.Parameters params, Set<Identifier.Variable.Retrievable> filter) {
        System.out.println(procedure);
        assert procedure.vertexCount() > 1;
        this.graphMgr = graphMgr;
        this.procedure = procedure;
        this.params = params;
        this.filter = filter;
        this.scopes = new Scopes();
        this.state = State.INIT;
        this.start = start;

        this.forward = new TreeSet<>();
        this.backward = new TreeSet<>();
        this.stepDirection = StepDirection.FORWARD;
        this.vertexScanners = new VertexScanner[procedure.vertexCount()];
        for (int i = 0; i < vertexScanners.length; i++) {
            vertexScanners[i] = new VertexScanner(procedure.vertex(i));
        }
    }

    @Override
    public VertexMap next() {
        if (!hasNext()) throw new NoSuchElementException();
        state = State.EMPTY;
        return toVertexMap();
    }

    private VertexMap toVertexMap() {
        Map<Identifier.Variable.Retrievable, Vertex<?, ?>> answer = new HashMap<>();
        for (int pos = 0; pos < vertexScanners.length; pos++) {
            ProcedureVertex<?, ?> procedureVertex = procedure.vertex(pos);
            if (procedureVertex.id().isRetrievable() && filter.contains(procedureVertex.id().asVariable().asRetrievable())) {
                answer.put(procedureVertex.id().asVariable().asRetrievable(), vertexScanners[pos].currentVertex());
            }
        }

        return VertexMap.of(answer);
    }

    private void setCompleted() {
        state = State.COMPLETED;
        recycle();
    }

    @Override
    public boolean hasNext() {
        try {
            if (state == State.COMPLETED) return false;
            else if (state == State.FETCHED) return true;
            else if (state == State.INIT) {
                initialiseStart();
                if (computeNext()) state = State.FETCHED;
                else setCompleted();
            } else if (state == State.EMPTY) {
                initialiseEnds();
                if (computeNext()) state = State.FETCHED;
                else setCompleted();
            } else {
                throw TypeDBException.of(ILLEGAL_STATE);
            }
            return state == State.FETCHED;
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

    private void initialiseStart() {
        forward.add(0);
        stepDirection = StepDirection.FORWARD;
    }

    private void initialiseEnds() {
        assert forward.isEmpty();
        procedure.endVertices().forEach(v -> backward.add(v.order()));
        stepDirection = StepDirection.BACKWARD;
    }

    private boolean computeNext() {
        while ((stepDirection == StepDirection.FORWARD && !forward.isEmpty()) ||
                (stepDirection == StepDirection.BACKWARD && !backward.isEmpty())) {
            if (stepDirection == StepDirection.FORWARD) stepForward(forward.pollFirst());
            else stepBackward(backward.pollLast());
        }
        return stepDirection == StepDirection.FORWARD;
    }

    private void stepForward(int pos) {
        System.out.println("Forward at pos: " + pos);
        if (pos == vertexScanners.length) return;
        VertexScanner vertexScanner = vertexScanners[pos];
        boolean success = vertexScanner.hasVertex();
        transferScanFailureCauses(vertexScanner);
        if (success) forward.add(pos + 1);
        else {
            vertexScanner.resetIterator();
            stepDirection = StepDirection.BACKWARD;
        }
    }

    private void transferScanFailureCauses(VertexScanner vertexScanner) {
        vertexScanner.vertexFailureCauses().forEach(cause -> {
            if (cause.procedureVertex.order() < vertexScanner.procedureVertex.order()) {
                backward.add(cause.procedureVertex.order());
            }
        });
        vertexScanner.clearFailureCauses();
    }

    private void stepBackward(int pos) {
        System.out.println("Backtracking at pos: " + pos);
        VertexScanner vertexScanner = vertexScanners[pos];

        vertexScanner.removeAsInput();
        vertexScanner.transitiveOutputVertices().forEachRemaining(scanner -> {
            forward.remove(scanner.procedureVertex.order());
            scanner.removeAsInput();
        });

        forward.add(pos);
        vertexScanner.clearVertex();
        stepDirection = StepDirection.FORWARD;
    }

    @Override
    public void recycle() {
        for (VertexScanner vertex : vertexScanners) {
            vertex.reset();
        }
    }

    private class VertexScanner {

        private final ProcedureVertex<?, ?> procedureVertex;
        private final Set<ProcedureEdge<?, ?>> inputProcedureEdges;
        private final Set<VertexScanner> failureCauses;
        private Forwardable<Vertex<?, ?>, Order.Asc> iterator;
        private Vertex<?, ?> vertex;
        private boolean isVertexVerified;

        private VertexScanner(ProcedureVertex<?, ?> procedureVertex) {
            this.procedureVertex = procedureVertex;
            this.inputProcedureEdges = new HashSet<>();
            this.failureCauses = new HashSet<>();
            this.isVertexVerified = false;
        }

        private boolean hasVertex() {
            Forwardable<Vertex<?, ?>, Order.Asc> iter = getIterator();
            while (!isVertexVerified && iter.hasNext()) {
                vertex = iter.next();
                if (verifyClosures() && verifyScopes() && verifyOutEdges()) {
                    isVertexVerified = true;
                }
            }
            if (!isVertexVerified) inputVertices().forEachRemaining(failureCauses::add);
            return isVertexVerified;
        }

        private boolean mayHaveVertex() {
            return getIterator().hasNext();
        }

        private Vertex<?, ?> currentVertex() {
            return vertex;
        }

        private void clearVertex() {
            vertex = null;
            isVertexVerified = false;
        }

        private Set<VertexScanner> vertexFailureCauses() {
            return failureCauses;
        }

        private void clearFailureCauses() {
            failureCauses.clear();
        }

        private boolean verifyClosures() {
            assert vertex != null;
            for (ProcedureEdge<?, ?> edge : procedureVertex.loops()) {
                if (!isClosure(edge, vertex, vertex)) {
                    inputVertices().forEachRemaining(failureCauses::add);
                    return false;
                }
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

        private boolean verifyScopes() {
            boolean invalid = false;
            for (Identifier.Variable id : procedureVertex.scopedBy()) {
                Scopes.Scoped scoped = scopes.get(id);
                if (!scoped.isValidUpTo(procedureVertex.order())) {
                    scoped.scopedOrdersUpTo(procedureVertex.order()).forEach(order -> failureCauses.add(vertexScanners[order]));
                    invalid = true;
                }
            }
            return !invalid;
        }

        private boolean verifyOutEdges() {
            Set<ProcedureEdge<?, ?>> edges = new HashSet<>();
            for (ProcedureEdge<?, ?> edge : procedureVertex.orderedOuts()) {
                if (!edge.to().equals(procedureVertex)) {
                    VertexScanner toExplorer = vertexScanners[edge.to().order()];
                    toExplorer.addInputEdge(edge);
                    edges.add(edge);
                    if (!toExplorer.mayHaveVertex()) {
                        edges.forEach(e -> vertexScanners[e.to().order()].removeInputEdge(e));
                        toExplorer.inputVertices().forEachRemaining(failureCauses::add);
                        return false;
                    }
                }
            }
            return true;
        }

        private FunctionalIterator<VertexScanner> inputVertices() {
            return iterate(inputProcedureEdges).map(e -> vertexScanners[e.from().order()]);
        }

        private FunctionalIterator<VertexScanner> transitiveOutputVertices() {
            return iterate(procedureVertex.transitiveOuts()).map(vertex -> vertexScanners[vertex.order()])
                    .filter(scanner -> !scanner.equals(this));
        }

        private void addInputEdge(ProcedureEdge<?, ?> edge) {
            assert !inputProcedureEdges.contains(edge);
            inputProcedureEdges.add(edge);
            if (iterator != null) {
                iterator = intersect(branch(vertexScanners[edge.from().order()].currentVertex(), edge), iterator);
            }
            isVertexVerified = false;
            vertex = null;
        }

        private void removeInputEdge(ProcedureEdge<?, ?> edge) {
            inputProcedureEdges.remove(edge);
            resetIterator();
        }

        private void removeAsInput() {
            procedureVertex.outs().forEach(e -> vertexScanners[e.to().order()].removeInputEdge(e));
        }

        private void reset() {
            inputProcedureEdges.clear();
            resetIterator();
        }

        private void resetIterator() {
            if (iterator != null) {
                iterator.recycle();
                iterator = null;
            }
            vertex = null;
            isVertexVerified = false;
        }

        private Forwardable<Vertex<?, ?>, Order.Asc> getIterator() {
            assert !inputProcedureEdges.isEmpty() || procedureVertex.isStartingVertex();
            if (iterator == null) {
                if (procedureVertex.isStartingVertex()) iterator = createIteratorFromStart();
                else iterator = createIteratorFromInputs();
            }
            return iterator;
        }

        private Forwardable<Vertex<?, ?>, Order.Asc> createIteratorFromStart() {
            if (procedure.startVertex().id().isScoped()) {
                Scopes.Scoped scoped = scopes.getOrInitialise(procedure.startVertex().id().asScoped().scope());
                recordScoped(scoped, procedure.startVertex(), start.asThing());
            }
            return iterateSorted(ASC, start);
        }

        private Forwardable<Vertex<?, ?>, Order.Asc> createIteratorFromInputs() {
            List<Forwardable<Vertex<?, ?>, Order.Asc>> iters = new ArrayList<>();
            for (ProcedureEdge<?, ?> edge : inputProcedureEdges) {
                VertexScanner vertexScanner = vertexScanners[edge.from().order()];
                iters.add(branch(vertexScanner.currentVertex(), edge));
            }
            if (iters.size() == 1) return iters.get(0);
            else return intersect(iterate(iters), ASC);
        }

        private Forwardable<Vertex<?, ?>, Order.Asc> branch(Vertex<?, ?> fromVertex, ProcedureEdge<?, ?> edge) {
            Forwardable<? extends Vertex<?, ?>, Order.Asc> toIter;
            if (edge.to().id().isScoped()) {
                Identifier.Variable scope = edge.to().id().asScoped().scope();
                Scopes.Scoped scoped = scopes.getOrInitialise(scope);
                toIter = edge.branch(graphMgr, fromVertex, params).mapSorted(
                        role -> {
                            recordScoped(scoped, edge.to(), role.asThing());
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
                            recordScoped(scoped, edge, thingAndRole.value());
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

        private void recordScoped(Scopes.Scoped scoped, ProcedureVertex<?, ?> source, ThingVertex role) {
            if (scoped.containsSource(source)) scoped.replace(source, role.asThing());
            else scoped.record(source, role.asThing(), source.order());
        }

        private void recordScoped(Scopes.Scoped scoped, ProcedureEdge<?, ?> source, ThingVertex role) {
            if (scoped.containsSource(source)) scoped.replace(source, role);
            else scoped.record(source, role, source.to().order());
        }
    }

    public static class Scopes {

        private final Map<Identifier.Variable, Scoped> scoped;

        public Scopes() {
            this.scoped = new HashMap<>();
        }

        public Scoped getOrInitialise(Identifier.Variable scope) {
            return scoped.computeIfAbsent(scope, s -> new Scoped());
        }

        public Scoped get(Identifier.Variable scope) {
            assert scoped.containsKey(scope);
            return scoped.get(scope);
        }

        public static class Scoped {

            private final Map<ProcedureVertex<?, ?>, ThingVertex> vertexSources;
            private final Map<ProcedureEdge<?, ?>, ThingVertex> edgeSources;
            private final Map<ProcedureVertex<?, ?>, Integer> vertexSourceOrders;
            private final Map<ProcedureEdge<?, ?>, Integer> edgeSourceOrders;

            private Scoped() {
                vertexSources = new HashMap<>();
                edgeSources = new HashMap<>();
                vertexSourceOrders = new HashMap<>();
                edgeSourceOrders = new HashMap<>();
            }

            public boolean containsSource(ProcedureVertex<?, ?> vertex) {
                return vertexSources.containsKey(vertex);
            }

            public boolean containsSource(ProcedureEdge<?, ?> edge) {
                return edgeSources.containsKey(edge);
            }

            public void record(ProcedureEdge<?, ?> source, ThingVertex role, int sourceOrder) {
                assert source.isRolePlayer() && role.type().isRoleType();
                edgeSources.put(source, role);
                edgeSourceOrders.put(source, sourceOrder);
            }

            public void record(ProcedureVertex<?, ?> source, ThingVertex role, int sourceOrder) {
                assert source.id().isScoped() && role.type().isRoleType();
                vertexSources.put(source, role);
                vertexSourceOrders.put(source, sourceOrder);
            }

            public void replace(ProcedureEdge<?, ?> edge, ThingVertex role) {
                assert edge.isRolePlayer() && role.type().isRoleType();
                edgeSources.put(edge, role);
            }

            public void replace(ProcedureVertex<?, ?> vertex, ThingVertex role) {
                assert vertex.id().isScoped() && role.type().isRoleType();
                vertexSources.put(vertex, role);
            }

            public void clear() {
                vertexSources.clear();
                edgeSources.clear();
            }

            // TODO: this needs to become a sub-linear operation with some clever data structures...
            public boolean isValidUpTo(int orderInclusive) {
                Set<ThingVertex> roles = new HashSet<>();
                int expectedRoles = 0;
                for (Map.Entry<ProcedureEdge<?, ?>, ThingVertex> entry : edgeSources.entrySet()) {
                    if (edgeSourceOrders.get(entry.getKey()) <= orderInclusive) {
                        expectedRoles++;
                        roles.add(entry.getValue());
                    }
                }
                for (Map.Entry<ProcedureVertex<?, ?>, ThingVertex> entry : vertexSources.entrySet()) {
                    if (vertexSourceOrders.get(entry.getKey()) <= orderInclusive) {
                        expectedRoles++;
                        roles.add(entry.getValue());
                    }
                }
                return roles.size() == expectedRoles;
            }

            public Set<Integer> scopedOrdersUpTo(int orderInclusive) {
                Set<Integer> orders = new HashSet<>();
                for (Map.Entry<ProcedureEdge<?, ?>, ThingVertex> entry : edgeSources.entrySet()) {
                    if (edgeSourceOrders.get(entry.getKey()) <= orderInclusive) {
                        orders.add(edgeSourceOrders.get(entry.getKey()));
                    }
                }
                for (Map.Entry<ProcedureVertex<?, ?>, ThingVertex> entry : vertexSources.entrySet()) {
                    if (vertexSourceOrders.get(entry.getKey()) <= orderInclusive) {
                        orders.add(vertexSourceOrders.get(entry.getKey()));
                    }
                }
                return orders;
            }
        }
    }
}
