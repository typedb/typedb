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
import java.util.NavigableMap;
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
    private final Scopes scopes;
    private final Vertex<?, ?> start;
    private final TreeSet<Integer> verify;
    private final TreeSet<Integer> retry;
    private Operation operation;
    private State state;

    private enum State {INIT, EMPTY, FETCHED, COMPLETED}

    private enum Operation {VERIFY, RETRY}

    public GraphIterator(GraphManager graphMgr, Vertex<?, ?> start, GraphProcedure procedure,
                         Traversal.Parameters params, Set<Identifier.Variable.Retrievable> filter) {
        assert procedure.vertexCount() > 1;
        this.graphMgr = graphMgr;
        this.procedure = procedure;
        this.params = params;
        this.filter = filter;
        this.start = start;

        this.state = State.INIT;
        this.scopes = new Scopes();
        this.verify = new TreeSet<>();
        this.retry = new TreeSet<>();
        this.operation = Operation.VERIFY;
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

    @Override
    public boolean hasNext() {
        try {
            if (state == State.COMPLETED) return false;
            else if (state == State.FETCHED) return true;
            else if (state == State.INIT) {
                initialiseStart();
                if (computeAnswer()) state = State.FETCHED;
                else setCompleted();
            } else if (state == State.EMPTY) {
                initialiseEnds();
                if (computeAnswer()) state = State.FETCHED;
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

    private void setCompleted() {
        state = State.COMPLETED;
        recycle();
    }

    private void initialiseStart() {
        verify.add(0);
        operation = Operation.VERIFY;
    }

    private void initialiseEnds() {
        assert verify.isEmpty();
        procedure.endVertices().forEach(v -> retry.add(v.order()));
        operation = Operation.RETRY;
    }

    private boolean computeAnswer() {
        while ((operation == Operation.VERIFY && !verify.isEmpty()) ||
                (operation == Operation.RETRY && !retry.isEmpty())) {
            if (operation == Operation.VERIFY) verify(verify.pollFirst());
            else prepareRetry(retry.pollLast());
        }
        return operation == Operation.VERIFY;
    }

    private void verify(int pos) {
        if (pos == vertexScanners.length) return;
        VertexScanner vertexScanner = vertexScanners[pos];
        boolean verified = vertexScanner.hasVerifiedVertex();
        transferVerificationFailureCauses(vertexScanner);
        if (verified) verify.add(pos + 1);
        else {
            vertexScanner.resetScan();
            operation = Operation.RETRY;
        }
    }

    private void transferVerificationFailureCauses(VertexScanner vertexScanner) {
        vertexScanner.verifyFailureCauses().forEach(cause -> {
            if (cause.procedureVertex.order() < vertexScanner.procedureVertex.order()) {
                retry.add(cause.procedureVertex.order());
            }
        });
        vertexScanner.clearVerifyFailureCauses();
    }

    private void prepareRetry(int pos) {
        VertexScanner vertexScanner = vertexScanners[pos];
        vertexScanner.clearCurrentVertex();
        vertexScanner.transitiveOutputScanners().forEachRemaining(v ->
                verify.remove(v.procedureVertex.order())
        );
        verify.add(pos);
        operation = Operation.VERIFY;
    }

    @Override
    public void recycle() {
        for (VertexScanner vertex : vertexScanners) {
            vertex.clearInputs();
        }
    }

    private class VertexScanner {

        private final ProcedureVertex<?, ?> procedureVertex;
        private final Set<ProcedureEdge<?, ?>> inputs;
        private final Set<VertexScanner> failureCauses;
        private Forwardable<Vertex<?, ?>, Order.Asc> iterator;
        private Vertex<?, ?> vertex;
        private boolean isVertexVerified;

        private VertexScanner(ProcedureVertex<?, ?> procedureVertex) {
            this.procedureVertex = procedureVertex;
            this.inputs = new HashSet<>();
            this.failureCauses = new HashSet<>();
            this.isVertexVerified = false;
        }

        private boolean hasVerifiedVertex() {
            Forwardable<Vertex<?, ?>, Order.Asc> iter = getIterator();
            while (!isVertexVerified && iter.hasNext()) {
                vertex = iter.next();
                if (verifyClosures() && verifyScopes() && verifyOutEdges()) {
                    isVertexVerified = true;
                }
            }
            if (!isVertexVerified) inputScanners().forEachRemaining(failureCauses::add);
            return isVertexVerified;
        }

        private boolean mayHaveVertex() {
            return getIterator().hasNext();
        }

        private Vertex<?, ?> currentVertex() {
            return vertex;
        }

        private Set<VertexScanner> verifyFailureCauses() {
            return failureCauses;
        }

        private void clearVerifyFailureCauses() {
            failureCauses.clear();
        }

        private boolean verifyClosures() {
            assert vertex != null;
            for (ProcedureEdge<?, ?> edge : procedureVertex.loops()) {
                if (!isClosure(edge, vertex, vertex)) {
                    inputScanners().forEachRemaining(failureCauses::add);
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
                    toExplorer.addInput(edge);
                    edges.add(edge);
                    if (!toExplorer.mayHaveVertex()) {
                        edges.forEach(e -> vertexScanners[e.to().order()].removeInput(e));
                        toExplorer.inputScanners().forEachRemaining(failureCauses::add);
                        return false;
                    }
                }
            }
            return true;
        }

        private FunctionalIterator<VertexScanner> inputScanners() {
            return iterate(inputs).map(e -> vertexScanners[e.from().order()]);
        }

        private FunctionalIterator<VertexScanner> transitiveOutputScanners() {
            return iterate(procedureVertex.transitiveOuts()).map(vertex -> vertexScanners[vertex.order()])
                    .filter(scanner -> !scanner.equals(this));
        }

        private void addInput(ProcedureEdge<?, ?> edge) {
            assert !inputs.contains(edge);
            inputs.add(edge);
            if (iterator != null) {
                iterator = intersect(branch(vertexScanners[edge.from().order()].currentVertex(), edge), iterator);
            }
            isVertexVerified = false;
            vertex = null;
        }

        private void removeInput(ProcedureEdge<?, ?> edge) {
            inputs.remove(edge);
            resetScan();
        }

        private void clearInputs() {
            inputs.clear();
            resetScan();
        }

        public void clearCurrentVertex() {
            if (vertex != null) {
                vertex = null;
                isVertexVerified = false;
                removeAsInputScanner();
                transitiveOutputScanners().forEachRemaining(VertexScanner::removeAsInputScanner);
            }
        }

        private void removeAsInputScanner() {
            procedureVertex.outs().forEach(e -> vertexScanners[e.to().order()].removeInput(e));
        }

        private void resetScan() {
            if (iterator != null) {
                iterator.recycle();
                iterator = null;
            }
            vertex = null;
            isVertexVerified = false;
        }

        private Forwardable<Vertex<?, ?>, Order.Asc> getIterator() {
            assert !inputs.isEmpty() || procedureVertex.isStartingVertex();
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
            for (ProcedureEdge<?, ?> edge : inputs) {
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
            private final Map<ProcedureVertex<?, ?>, Integer> vertexSourceOrders;
            private final Map<ProcedureEdge<?, ?>, Integer> edgeSourceOrders;

            private Scoped() {
                vertexSources = new HashMap<>();
                edgeSources = new HashMap<>();
                vertexSourceOrders = new HashMap<>();
                edgeSourceOrders = new HashMap<>();
            }

            private boolean containsSource(ProcedureVertex<?, ?> vertex) {
                return vertexSources.containsKey(vertex);
            }

            private boolean containsSource(ProcedureEdge<?, ?> edge) {
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

            private void replace(ProcedureEdge<?, ?> edge, ThingVertex role) {
                assert edge.isRolePlayer() && role.type().isRoleType();
                edgeSources.put(edge, role);
            }

            private void replace(ProcedureVertex<?, ?> vertex, ThingVertex role) {
                assert vertex.id().isScoped() && role.type().isRoleType();
                vertexSources.put(vertex, role);
            }

            private boolean isValidUpTo(int order) {
                Set<ThingVertex> roles = new HashSet<>();
                for (Map.Entry<ProcedureEdge<?, ?>, ThingVertex> entry : edgeSources.entrySet()) {
                    if (edgeSourceOrders.get(entry.getKey()) <= order) {
                        if (roles.contains(entry.getValue())) return false;
                        else roles.add(entry.getValue());
                    }
                }
                for (Map.Entry<ProcedureVertex<?, ?>, ThingVertex> entry : vertexSources.entrySet()) {
                    if (vertexSourceOrders.get(entry.getKey()) <= order) {
                        if (roles.contains(entry.getValue())) return false;
                        else roles.add(entry.getValue());
                    }
                }
                return true;
            }

            private Set<Integer> scopedOrdersUpTo(int order) {
                Set<Integer> orders = new HashSet<>();
                for (Map.Entry<ProcedureEdge<?, ?>, ThingVertex> entry : edgeSources.entrySet()) {
                    if (edgeSourceOrders.get(entry.getKey()) <= order) {
                        orders.add(edgeSourceOrders.get(entry.getKey()));
                    }
                }
                for (Map.Entry<ProcedureVertex<?, ?>, ThingVertex> entry : vertexSources.entrySet()) {
                    if (vertexSourceOrders.get(entry.getKey()) <= order) {
                        orders.add(vertexSourceOrders.get(entry.getKey()));
                    }
                }
                return orders;
            }
        }
    }
}
