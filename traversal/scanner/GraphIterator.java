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
    private final TreeSet<Integer> forward;
    private final TreeSet<Integer> backward;
    private final VertexExplorer[] vertices;
    private final Scopes scopes;
    private final int vertexCount;
    private final Vertex<?, ?> start;
    private State state;
    private StepDirection stepDirection;

    private enum State {INIT, EMPTY, FETCHED, COMPLETED}

    private enum StepDirection {FORWARD, BACKWARD}

    public GraphIterator(GraphManager graphMgr, Vertex<?, ?> start, GraphProcedure procedure,
                         Traversal.Parameters params, Set<Identifier.Variable.Retrievable> filter) {
        assert procedure.vertexCount() > 1;
        this.graphMgr = graphMgr;
        this.procedure = procedure;
        this.params = params;
        this.filter = filter;
        this.scopes = new Scopes();
        this.state = State.INIT;
        this.start = start;
        this.vertexCount = procedure.vertexCount();

        this.forward = new TreeSet<>();
        this.backward = new TreeSet<>();
        this.stepDirection = StepDirection.FORWARD;
        this.vertices = new VertexExplorer[vertexCount];
        for (int i = 0; i < vertexCount; i++) {
            vertices[i] = new VertexExplorer(procedure.vertex(i));
        }
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
        forward.clear();
        backward.clear();
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
        if (pos == vertexCount) return;
        VertexExplorer vertexExplorer = vertices[pos];
        boolean success = vertexExplorer.tryFindVertex();
        recordVertexFailureCauses(vertexExplorer);
        if (success) forward.add(pos + 1);
        else {
            vertexExplorer.reset();
            stepDirection = StepDirection.BACKWARD;
        }
    }

    private void recordVertexFailureCauses(VertexExplorer vertexExplorer) {
        vertexExplorer.failureCauses().forEach(cause -> {
            if (cause.vertex.order() < vertexExplorer.vertex.order()) {
                backward.add(cause.vertex.order());
            }
        });
        vertexExplorer.clearFailureCauses();
    }

    private void stepBackward(int pos) {
        VertexExplorer vertexExplorer = vertices[pos];
        vertexExplorer.vertex.transitiveOuts().forEach(v -> {
            forward.remove(v.order());
            vertices[v.order()].clearinputs();
        });
        forward.add(pos);
        vertexExplorer.prepareRetry();
        stepDirection = StepDirection.FORWARD;
    }

    private class VertexExplorer {

        private final ProcedureVertex<?, ?> vertex;
        private final Set<ProcedureEdge<?, ?>> inputEdges;
        private final Set<VertexExplorer> failureCauses;
        private Forwardable<Vertex<?, ?>, Order.Asc> iterator;
        private Vertex<?, ?> answer;
        private boolean isVerified;

        private VertexExplorer(ProcedureVertex<?, ?> vertex) {
            this.vertex = vertex;
            this.inputEdges = new HashSet<>();
            this.failureCauses = new HashSet<>();
            this.isVerified = false;
        }

        private boolean tryFindVertex() {
            Forwardable<Vertex<?, ?>, Order.Asc> iter = getIterator();
            while (!isVerified && iter.hasNext()) {
                answer = iter.next();
                if (verifyLocal() && verifyOuts()) {
                    isVerified = true;
                }
            }
            if (!isVerified) inputVertices().forEachRemaining(failureCauses::add);
            return isVerified;
        }

        private Vertex<?, ?> currentAnswer() {
            return answer;
        }

        private void prepareRetry() {
            answer = null;
            isVerified = false;
        }

        private Set<VertexExplorer> failureCauses() {
            return failureCauses;
        }

        private void clearFailureCauses() {
            failureCauses.clear();
        }

        private boolean hasUnverifiedVertex() {
            return getIterator().hasNext();
        }

        private boolean verifyLocal() {
            assert answer != null;

            // TODO the vertex should hold onto loop edges
            for (ProcedureEdge<?, ?> edge : vertex.outs()) {
                if (edge.to().equals(vertex) && !isClosure(edge, answer, answer)) {
                    inputVertices().forEachRemaining(failureCauses::add);
                    return false;
                }
            }

            boolean invalidScope = false;
            for (Identifier.Variable id : vertex.scopedBy()) {
                Scopes.Scoped scoped = scopes.get(id);
                if (!scoped.isValidUpTo(vertex.order())) {
                    scoped.scopedOrdersUpTo(vertex.order()).forEach(order -> failureCauses.add(vertices[order]));
                    invalidScope = true;
                }
            }
            return !invalidScope;
        }

        private boolean isClosure(ProcedureEdge<?, ?> edge, Vertex<?, ?> fromVertex, Vertex<?, ?> toVertex) {
            if (edge.isRolePlayer()) {
                Scopes.Scoped scoped = scopes.getOrInitialise(edge.asRolePlayer().scope());
                return edge.asRolePlayer().isClosure(graphMgr, fromVertex, toVertex, params, scoped);
            } else {
                return edge.isClosure(graphMgr, fromVertex, toVertex, params);
            }
        }

        private FunctionalIterator<VertexExplorer> inputVertices() {
            return iterate(inputEdges).map(e -> vertices[e.from().order()]);
        }

        private boolean verifyOuts() {
            Set<ProcedureEdge<?, ?>> resetIfFail = new HashSet<>();
            for (ProcedureEdge<?, ?> edge : vertex.orderedOuts()) {
                ProcedureVertex<?, ?> to = edge.to();
                if (!to.equals(vertex)) {
                    VertexExplorer toExplorer = vertices[to.order()];
                    toExplorer.addInputEdge(edge);
                    resetIfFail.add(edge);
                    if (!toExplorer.hasUnverifiedVertex()) {
                        resetIfFail.forEach(e -> vertices[e.to().order()].removeInputEdge(e));
                        toExplorer.inputVertices().forEachRemaining(failureCauses::add);
                        return false;
                    }
                }
            }
            return true;
        }

        private void addInputEdge(ProcedureEdge<?, ?> edge) {
            inputEdges.add(edge);
            if (iterator != null) {
                iterator = intersect(branch(vertices[edge.from().order()].currentAnswer(), edge), iterator);
            }
            isVerified = false;
            answer = null;
        }

        private void removeInputEdge(ProcedureEdge<?, ?> edge) {
            assert inputEdges.contains(edge);
            inputEdges.remove(edge);
            if (iterator != null) {
                iterator.recycle();
                iterator = null;
            }
            answer = null;
            isVerified = false;
            // TODO we don't need to re-verify because we can technically guarantee we only remove the failing edge
            //      in fact we could seek to the last answer that we held on to to avoid recomputing a partial intersection?
        }

        public void clearinputs() {
            inputEdges.clear();
            if (iterator != null) {
                iterator.recycle();
                iterator = null;
            }
            answer = null;
            isVerified = false;
        }

        private Forwardable<Vertex<?, ?>, Order.Asc> getIterator() {
            assert !inputEdges.isEmpty() || vertex.isStartingVertex();
            if (iterator == null) {
                if (vertex.isStartingVertex()) {
                    if (procedure.startVertex().id().isScoped()) {
                        Scopes.Scoped scoped = scopes.getOrInitialise(procedure.startVertex().id().asScoped().scope());
                        recordScoped(scoped, procedure.startVertex(), start.asThing());
                    }
                    iterator = iterateSorted(ASC, start);
                } else {
                    List<Forwardable<Vertex<?, ?>, Order.Asc>> iters = new ArrayList<>();
                    for (ProcedureEdge<?, ?> edge : inputEdges) {
                        VertexExplorer vertexExplorer = vertices[edge.from().order()];
                        iters.add(branch(vertexExplorer.currentAnswer(), edge));
                    }
                    if (iters.size() == 1) iterator = iters.get(0);
                    else iterator = intersect(iterate(iters), ASC);
                }
            }
            return iterator;
        }

        public void reset() {
            if (iterator != null) {
                iterator.recycle();
                iterator = null;
            }
            answer = null;
            isVerified = false;
        }

        public void recycle() {
            if (iterator != null) {
                iterator.recycle();
                iterator = null;
            }
        }
    }

    @Override
    public VertexMap next() {
        if (!hasNext()) throw new NoSuchElementException();
        state = State.EMPTY;
        return toVertexMap();
    }

    private void setCompleted() {
        state = State.COMPLETED;
        recycle();
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

    private VertexMap toVertexMap() {
        Map<Identifier.Variable.Retrievable, Vertex<?, ?>> answer = new HashMap<>();
        for (int pos = 0; pos < vertexCount; pos++) {
            ProcedureVertex<?, ?> procedureVertex = procedure.vertex(pos);
            if (procedureVertex.id().isRetrievable() && filter.contains(procedureVertex.id().asVariable().asRetrievable())) {
                answer.put(procedureVertex.id().asVariable().asRetrievable(), vertices[pos].currentAnswer());
            }
        }

        return VertexMap.of(answer);
    }

    @Override
    public void recycle() {
        for (VertexExplorer vertex : vertices) {
            vertex.recycle();
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

            public Set<Integer> scopedOrdersUpTo(int order) {
                Set<Integer> orders = new HashSet<>();
                for (Map.Entry<ProcedureEdge<?, ?>, ThingVertex> entry : edgeSources.entrySet()) {
                    if (edgeSourceOrders.get(entry.getKey()) < order) {
                        orders.add(edgeSourceOrders.get(entry.getKey()));
                    }
                }
                for (Map.Entry<ProcedureVertex<?, ?>, ThingVertex> entry : vertexSources.entrySet()) {
                    if (vertexSourceOrders.get(entry.getKey()) < order) {
                        orders.add(vertexSourceOrders.get(entry.getKey()));
                    }
                }
                return orders;
            }
        }
    }
}
