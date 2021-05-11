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

package com.vaticle.typedb.core.traversal.iterator;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.AbstractFunctionalIterator;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.graph.GraphManager;
import com.vaticle.typedb.core.graph.vertex.ThingVertex;
import com.vaticle.typedb.core.graph.vertex.Vertex;
import com.vaticle.typedb.core.traversal.Traversal;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typedb.core.traversal.common.Identifier.Variable.Retrievable;
import com.vaticle.typedb.core.traversal.common.VertexMap;
import com.vaticle.typedb.core.traversal.procedure.GraphProcedure;
import com.vaticle.typedb.core.traversal.procedure.ProcedureEdge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeMap;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.RESOURCE_CLOSED;
import static java.util.stream.Collectors.toMap;

public class GraphIterator extends AbstractFunctionalIterator<VertexMap> {

    private static final Logger LOG = LoggerFactory.getLogger(GraphIterator.class);

    private final GraphManager graphMgr;
    private final GraphProcedure procedure;
    private final Traversal.Parameters params;
    private final Set<Retrievable> filter;
    private final Map<Identifier, FunctionalIterator<? extends Vertex<?, ?>>> iterators;
    private final Map<Identifier, Vertex<?, ?>> answer;
    private final Scopes scopes;
    private final BranchSeekStack branchSeekStack;
    private final int edgeCount;
    private int computeNextSeekPos;
    private State state;

    enum State {INIT, EMPTY, FETCHED, COMPLETED}

    public GraphIterator(GraphManager graphMgr, Vertex<?, ?> start, GraphProcedure procedure,
                         Traversal.Parameters params, Set<Retrievable> filter) {
        assert procedure.edgesCount() > 0;
        this.graphMgr = graphMgr;
        this.procedure = procedure;
        this.params = params;
        this.filter = filter;
        this.edgeCount = procedure.edgesCount();
        this.iterators = new HashMap<>();
        this.scopes = new Scopes();
        this.branchSeekStack = new BranchSeekStack(edgeCount);
        this.state = State.INIT;
        this.answer = new HashMap<>();

        Identifier startId = procedure.startVertex().id();
        this.answer.put(startId, start);
        if (startId.isScoped()) {
            Identifier.Variable scope = startId.asScoped().scope();
            Scopes.Scoped scoped = scopes.getOrInitialise(scope);
            scoped.push(start.asThing(), 0);
        }
    }

    @Override
    public boolean hasNext() {
        try {
            if (state == State.COMPLETED) return false;
            else if (state == State.FETCHED) return true;
            else if (state == State.INIT) {
                if (computeFirst(1)) state = State.FETCHED;
                else state = State.COMPLETED;
            } else if (state == State.EMPTY) {
                computeNextSeekPos = edgeCount;
                if (computeNext(edgeCount)) state = State.FETCHED;
                else state = State.COMPLETED;
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

    private boolean computeFirst(int pos) {
        if (answer.containsKey(procedure.edge(pos).to().id())) return computeFirstClosure(pos);
        else return computeFirstBranch(pos);
    }

    private boolean computeFirstBranch(int pos) {
        ProcedureEdge<?, ?> edge = procedure.edge(pos);
        Identifier toID = edge.to().id();
        Identifier fromId = edge.from().id();
        FunctionalIterator<? extends Vertex<?, ?>> toIter = branch(answer.get(fromId), edge);

        if (toIter.hasNext()) {
            iterators.put(toID, toIter);
            answer.put(toID, toIter.next());
            if (pos == edgeCount) return true;
            while (!computeFirst(pos + 1)) {
                if (pos == branchSeekStack.peekLastPos()) {
                    branchSeekStack.popLastPos();
                    if (toIter.hasNext()) answer.put(toID, toIter.next());
                    else {
                        popScope(pos);
                        answer.remove(toID);
                        branchFailure(edge);
                        return false;
                    }
                } else {
                    popScope(pos);
                    answer.remove(toID);
                    toIter.recycle();
                    return false;
                }
            }
            return true;
        } else {
            branchFailure(edge);
            return false;
        }
    }

    private boolean computeFirstClosure(int pos) {
        ProcedureEdge<?, ?> edge = procedure.edge(pos);
        if (isClosure(edge, answer.get(edge.from().id()), answer.get(edge.to().id()))) {
            if (pos == edgeCount) return true;
            else if (computeFirst(pos + 1)) return true;
            else {
                popScope(pos);
                return false;
            }
        } else {
            closureFailure(edge);
            return false;
        }
    }

    private void branchFailure(ProcedureEdge<?, ?> edge) {
        if (edge.onlyStartsFromRelation()) {
            assert edge.from().id().isVariable();
            scopes.get(edge.from().id().asVariable()).edgeOrders().forEach(pos -> {
                if (!procedure.edge(pos).isClosureEdge()) branchSeekStack.addSeek(pos);
            });
        }
        branchSeekStack.addSeeks(edge.from().dependedEdgeOrders());
    }

    private void closureFailure(ProcedureEdge<?, ?> edge) {
        assert edge.from().id().isVariable();
        if (edge.onlyStartsFromRelation()) {
            branchSeekStack.addSeeks(scopes.get(edge.from().id().asVariable()).edgeOrders());
            branchSeekStack.addSeeks(edge.to().dependedEdgeOrders());
        } else if (edge.onlyEndsAtRelation()) {
            branchSeekStack.addSeeks(edge.from().dependedEdgeOrders());
            branchSeekStack.addSeeks(scopes.get(edge.to().id().asVariable()).edgeOrders());
        } else {
            branchSeekStack.addSeeks(edge.to().dependedEdgeOrders());
            branchSeekStack.addSeeks(edge.from().dependedEdgeOrders());
        }
    }

    private boolean computeNext(int pos) {
        if (pos == 0) return false;

        ProcedureEdge<?, ?> edge = procedure.edge(pos);
        Identifier toID = edge.to().id();

        if (pos == computeNextSeekPos) {
            computeNextSeekPos = edgeCount;
        } else if (pos > computeNextSeekPos) {
            if (!edge.isClosureEdge()) iterators.get(toID).recycle();
            if (!backTrack(pos)) return false;

            if (edge.isClosureEdge()) {
                Vertex<?, ?> fromVertex = answer.get(edge.from().id());
                Vertex<?, ?> toVertex = answer.get(edge.to().id());
                if (isClosure(edge, fromVertex, toVertex)) return true;
                else return computeNextClosure(pos);
            } else {
                FunctionalIterator<? extends Vertex<?, ?>> toIter = branch(answer.get(edge.from().id()), edge);
                iterators.put(toID, toIter);
            }
        }

        if (edge.isClosureEdge()) {
            return computeNextClosure(pos);
        } else if (iterators.get(toID).hasNext()) {
            answer.put(toID, iterators.get(toID).next());
            return true;
        } else {
            return computeNextBranch(pos);
        }
    }

    private boolean computeNextClosure(int pos) {
        ProcedureEdge<?, ?> edge = procedure.edge(pos);
        do {

            if (backTrack(pos)) {
                Vertex<?, ?> fromVertex = answer.get(edge.from().id());
                Vertex<?, ?> toVertex = answer.get(edge.to().id());
                if (isClosure(edge, fromVertex, toVertex)) return true;
            } else {
                return false;
            }
        } while (true);
    }

    private boolean computeNextBranch(int pos) {
        ProcedureEdge<?, ?> edge = procedure.edge(pos);
        FunctionalIterator<? extends Vertex<?, ?>> newIter;

        do {
            if (backTrack(pos)) {
                Vertex<?, ?> fromVertex = answer.get(edge.from().id());
                newIter = branch(fromVertex, edge);
                if (!newIter.hasNext()) {
                    if (edge.onlyStartsFromRelation() && !scopes.get(edge.from().id().asVariable()).isEmpty()) {
                        computeNextSeekPos = scopes.get(edge.from().id().asVariable()).lastEdgeOrder();
                    } else if (!edge.from().ins().isEmpty()) {
                        computeNextSeekPos = edge.from().branchEdge().order();
                    } else {
                        assert edge.from().isStartingVertex() && !edge.onlyStartsFromRelation();
                        computeNextSeekPos = 0;
                    }
                }
            } else {
                return false;
            }
        } while (!newIter.hasNext());
        iterators.put(edge.to().id(), newIter);
        answer.put(edge.to().id(), newIter.next());
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

    private FunctionalIterator<? extends Vertex<?, ?>> branch(Vertex<?, ?> fromVertex, ProcedureEdge<?, ?> edge) {
        FunctionalIterator<? extends Vertex<?, ?>> toIter;
        if (edge.to().id().isScoped()) {
            Identifier.Variable scope = edge.to().id().asScoped().scope();
            Scopes.Scoped scoped = scopes.getOrInitialise(scope);
            toIter = edge.branch(graphMgr, fromVertex, params).filter(role -> {
                if (scoped.contains(role.asThing())) return false;
                else {
                    if (scoped.orderVisited(edge.order())) scoped.replaceLast(role.asThing(), edge.order());
                    else scoped.push(role.asThing(), edge.order());
                    return true;
                }
            });
        } else if (edge.isRolePlayer()) {
            Identifier.Variable scope = edge.asRolePlayer().scope();
            Scopes.Scoped scoped = scopes.getOrInitialise(scope);
            toIter = edge.asRolePlayer().branchEdge(graphMgr, fromVertex, params).filter(e -> {
                if (scoped.contains(e.optimised().get())) return false;
                else {
                    if (scoped.orderVisited(edge.order())) scoped.replaceLast(e.optimised().get(), edge.order());
                    else scoped.push(e.optimised().get(), edge.order());
                    return true;
                }
            }).map(e -> edge.direction().isForward() ? e.to() : e.from());
        } else {
            toIter = edge.branch(graphMgr, fromVertex, params);
        }
        if (!edge.to().id().isName() && edge.to().outs().isEmpty() && edge.to().ins().size() == 1) {
            // TODO: This optimisation can apply to more situations, such as to
            //       an entire tree, where none of the leaves are referenced by name
            toIter = toIter.limit(1);
        }
        return toIter;
    }

    private boolean backTrack(int pos) {
        popScope(pos);
        return computeNext(pos - 1);
    }

    private void popScope(int pos) {
        ProcedureEdge<?, ?> edge = procedure.edge(pos);
        if (edge.to().id().isScoped()) {
            Identifier.Variable scope = edge.to().id().asScoped().scope();
            if (scopes.get(scope).orderVisited(pos)) scopes.get(scope).popLast();
        } else if (edge.isRolePlayer()) {
            Identifier.Variable scope = edge.asRolePlayer().scope();
            if (scopes.get(scope).orderVisited(pos)) scopes.get(scope).popLast();
        }
    }

    @Override
    public VertexMap next() {
        if (!hasNext()) throw new NoSuchElementException();
        state = State.EMPTY;
        return toVertexMap(answer);
    }

    private VertexMap toVertexMap(Map<Identifier, Vertex<?, ?>> answer) {
        return VertexMap.of(
                answer.entrySet().stream()
                        .filter(e -> e.getKey().isRetrievable() && filter.contains(e.getKey().asVariable().asRetrievable()))
                        .collect(toMap(e -> e.getKey().asVariable().asRetrievable(), Map.Entry::getValue))
        );
    }

    @Override
    public void recycle() {}

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

            Set<ThingVertex> roles;
            TreeMap<Integer, ThingVertex> visited;

            private Scoped() {
                this.roles = new HashSet<>();
                visited = new TreeMap<>();
            }

            public Collection<Integer> edgeOrders() {
                return visited.keySet();
            }

            public boolean isEmpty() {
                assert roles.size() == visited.size();
                return visited.isEmpty();
            }

            public Integer lastEdgeOrder() {
                assert !isEmpty();
                return visited.lastKey();
            }

            public boolean contains(ThingVertex roleVertex) {
                return roles.contains(roleVertex);
            }

            public void popLast() {
                ThingVertex poppedRole = visited.remove(lastEdgeOrder());
                assert roles.contains(poppedRole);
                roles.remove(poppedRole);
            }

            public void push(ThingVertex role, int order) {
                assert (isEmpty() || order > lastEdgeOrder()) && !roles.contains(role);
                visited.put(order, role);
                roles.add(role);
            }

            public void replaceLast(ThingVertex newRole, int order) {
                assert order == lastEdgeOrder();
                ThingVertex oldRole = visited.remove(order);
                assert roles.contains(oldRole);
                roles.remove(oldRole);
                visited.put(order, newRole);
                roles.add(newRole);
            }

            public boolean orderVisited(int order) {
                return visited.containsKey(order);
            }
        }
    }

    private class BranchSeekStack {

        private boolean[] seek;
        private int lastPos;

        private BranchSeekStack(int size) {
            seek = new boolean[size];
            lastPos = 0;
        }

        private void addSeeks(Collection<Integer> seeks) {
            seeks.forEach(this::setSeek);
        }

        public void addSeek(Integer pos) {
            setSeek(pos);
        }

        private void setSeek(int pos) {
            assert !procedure.edge(pos).isClosureEdge();
            seek[pos - 1] = true;
            if (pos > lastPos) lastPos = pos;
        }

        private int popLastPos() {
            assert lastPos > 0;
            seek[lastPos - 1] = false;
            int currentLastPos = lastPos;

            for (int p = lastPos - 1; p >= 0; p--) {
                if (p > 0 && seek[p - 1]) {
                    lastPos = p;
                    break;
                } else if (p == 0) {
                    lastPos = 0;
                }
            }
            return currentLastPos;
        }

        private int peekLastPos() {
            return lastPos;
        }
    }
}
