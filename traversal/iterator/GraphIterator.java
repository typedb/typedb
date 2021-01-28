/*
 * Copyright (C) 2021 Grakn Labs
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

package grakn.core.traversal.iterator;

import grakn.core.common.exception.GraknException;
import grakn.core.common.iterator.AbstractResourceIterator;
import grakn.core.common.iterator.ResourceIterator;
import grakn.core.graph.GraphManager;
import grakn.core.graph.vertex.ThingVertex;
import grakn.core.graph.vertex.Vertex;
import grakn.core.traversal.Traversal;
import grakn.core.traversal.common.Identifier;
import grakn.core.traversal.common.VertexMap;
import grakn.core.traversal.procedure.GraphProcedure;
import grakn.core.traversal.procedure.ProcedureEdge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.Set;

import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.common.iterator.Iterators.iterate;
import static java.util.stream.Collectors.toMap;

public class GraphIterator extends AbstractResourceIterator<VertexMap> {

    private static final Logger LOG = LoggerFactory.getLogger(GraphIterator.class);

    private final GraphManager graphMgr;
    private final GraphProcedure procedure;
    private final Traversal.Parameters params;
    private final List<Identifier.Variable.Name> filter;
    private final Map<Identifier, ResourceIterator<? extends Vertex<?, ?>>> iterators;
    private final Map<Identifier, Vertex<?, ?>> answer;
    private final Map<Identifier.Variable, Set<ThingVertex>> scoped;
    private final Map<Identifier.Variable, PriorityQueue<Integer>> scopedEdgeOrders;
    private final Map<Identifier, ThingVertex> roles;
    private final SeekStack seekStack;
    private final int edgeCount;
    private int computeNextSeekPos;
    private State state;

    enum State {INIT, EMPTY, FETCHED, COMPLETED}

    public GraphIterator(GraphManager graphMgr, Vertex<?, ?> start, GraphProcedure procedure,
                         Traversal.Parameters params, List<Identifier.Variable.Name> filter) {
        assert procedure.edgesCount() > 0;
        this.graphMgr = graphMgr;
        this.procedure = procedure;
        this.params = params;
        this.filter = filter;
        this.edgeCount = procedure.edgesCount();
        this.iterators = new HashMap<>();
        this.scoped = new HashMap<>();
        this.scopedEdgeOrders = new HashMap<>();
        this.roles = new HashMap<>();
        this.answer = new HashMap<>();
        this.answer.put(procedure.startVertex().id(), start);
        this.seekStack = new SeekStack(edgeCount);
        this.state = State.INIT;
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
                throw GraknException.of(ILLEGAL_STATE);
            }
            return state == State.FETCHED;
        } catch (Throwable e) {
            LOG.error("Parameters: " + params.toString());
            LOG.error("GraphProcedure: " + procedure.toString());
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
        ResourceIterator<? extends Vertex<?, ?>> toIter = branch(answer.get(edge.from().id()), edge);

        if (toIter.hasNext()) {
            iterators.put(toID, toIter);
            answer.put(toID, toIter.next());
            if (pos == edgeCount) return true;
            while (!computeFirst(pos + 1)) {
                if (pos == seekStack.peekLastPos()) {
                    seekStack.popLastPos();
                    if (edge.onlyStartsFromRelation()) {
                        scopedEdgeOrders.get(edge.from().id().asVariable()).remove(pos);
                    }
                    if (toIter.hasNext()) answer.put(toID, toIter.next());
                    else {
                        backTrackCleanUp(pos);
                        answer.remove(toID);
                        if (edge.onlyStartsFromRelation()) {
                            seekStack.addSeeks(iterate(scopedEdgeOrders.get(edge.from().id().asVariable())).toSet());
                        } else {
                            seekStack.addSeeks(edge.from().dependedEdgeOrders());
                        }
                        return false;
                    }
                } else {
                    backTrackCleanUp(pos);
                    answer.remove(toID);
                    toIter.recycle();
                    return false;
                }
            }
            return true;
        } else {
            if (edge.onlyStartsFromRelation()) {
                seekStack.addSeeks(iterate(scopedEdgeOrders.get(edge.from().id().asVariable())).toSet());
            } else {
                seekStack.addSeeks(edge.from().dependedEdgeOrders());
            }
            return false;
        }
    }

    private boolean computeFirstClosure(int pos) {
        ProcedureEdge<?, ?> edge = procedure.edge(pos);
        if (isClosure(edge, answer.get(edge.from().id()), answer.get(edge.to().id()))) {
            if (pos == edgeCount) return true;
            else return computeFirst(pos + 1);
        } else {
            if (edge.onlyStartsFromRelation()) {
                seekStack.addSeeks(iterate(scopedEdgeOrders.get(edge.from().id().asVariable())).toSet());
                seekStack.addSeeks(edge.to().dependedEdgeOrders());
            }
            if (edge.onlyEndsAtRelation()) {
                seekStack.addSeeks(edge.from().dependedEdgeOrders());
                seekStack.addSeeks(iterate(scopedEdgeOrders.get(edge.to().id().asVariable())).toSet());
            }

            return false;
        }
    }

    private boolean computeNext(int pos) {
        if (pos == 0) return false;

        ProcedureEdge<?, ?> edge = procedure.edge(pos);
        Identifier toID = edge.to().id();

        if (pos == computeNextSeekPos) {
            computeNextSeekPos = edgeCount;
            // this seek may have been set by the scoped seeks
            if (edge.onlyStartsFromRelation()) {
                scopedEdgeOrders.get(edge.from().id().asVariable()).remove(pos);
            }
        } else if (pos > computeNextSeekPos) {
            if (!edge.isClosureEdge()) iterators.get(toID).recycle();
            if (!backTrack(pos)) return false;

            if (edge.isClosureEdge()) {
                Vertex<?, ?> fromVertex = answer.get(edge.from().id());
                Vertex<?, ?> toVertex = answer.get(edge.to().id());
                if (isClosure(edge, fromVertex, toVertex)) return true;
                else return computeNextClosure(pos);
            } else {
                ResourceIterator<? extends Vertex<?, ?>> toIter = branch(answer.get(edge.from().id()), edge);
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
        ResourceIterator<? extends Vertex<?, ?>> newIter;

        do {
            if (backTrack(pos)) {
                Vertex<?, ?> fromVertex = answer.get(edge.from().id());
                newIter = branch(fromVertex, edge);
                if (!newIter.hasNext()) {
                    if (edge.onlyStartsFromRelation() && !scopedEdgeOrders.get(edge.from().id().asVariable()).isEmpty()) {
                        Integer lastRoleGenerator = scopedEdgeOrders.get(edge.from().id().asVariable()).peek();
                        computeNextSeekPos = lastRoleGenerator;
                    } else if (!edge.from().ins().isEmpty()) {
                        computeNextSeekPos = edge.from().branchEdge().order();
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
            Set<ThingVertex> withinScope = scoped.computeIfAbsent(edge.asRolePlayer().scope(), id -> new HashSet<>());
            PriorityQueue<Integer> scopedEdgeOrder = scopedEdgeOrders.computeIfAbsent(edge.asRolePlayer().scope(), id -> new PriorityQueue<>());
            scopedEdgeOrder.add(edge.order());
            return edge.asRolePlayer().isClosure(graphMgr, fromVertex, toVertex, params, withinScope);
        } else {
            return edge.isClosure(graphMgr, fromVertex, toVertex, params);
        }
    }

    private ResourceIterator<? extends Vertex<?, ?>> branch(Vertex<?, ?> fromVertex, ProcedureEdge<?, ?> edge) {
        ResourceIterator<? extends Vertex<?, ?>> toIter;
        if (edge.to().id().isScoped()) {
            Identifier.Variable scope = edge.to().id().asScoped().scope();
            Set<ThingVertex> withinScope = scoped.computeIfAbsent(scope, id -> new HashSet<>());
            PriorityQueue<Integer> scopedEdgeOrder = scopedEdgeOrders.computeIfAbsent(scope, id -> new PriorityQueue<>());
            toIter = edge.branch(graphMgr, fromVertex, params).filter(role -> {
                if (withinScope.contains(role.asThing())) return false;
                else {
                    removePreviousScopedRole(scope, edge.to().id(), edge);
                    withinScope.add(role.asThing());
                    scopedEdgeOrder.add(edge.order());
                    roles.put(edge.to().id(), role.asThing());
                    return true;
                }
            });
        } else if (edge.isRolePlayer()) {
            Identifier.Variable scope = edge.asRolePlayer().scope();
            Set<ThingVertex> withinScope = scoped.computeIfAbsent(scope, id -> new HashSet<>());
            PriorityQueue<Integer> scopedEdgeOrder = scopedEdgeOrders.computeIfAbsent(scope, id -> new PriorityQueue<>());
            toIter = edge.asRolePlayer().branchEdge(graphMgr, fromVertex, params).filter(e -> {
                if (withinScope.contains(e.optimised().get())) return false;
                else {
                    removePreviousScopedRole(scope, edge.to().id(), edge);
                    withinScope.add(e.optimised().get());
                    scopedEdgeOrder.add(edge.order());
                    roles.put(edge.to().id(), e.optimised().get());
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
        backTrackCleanUp(pos);
        return computeNext(pos - 1);
    }

    private void backTrackCleanUp(int pos) {
        ProcedureEdge<?, ?> edge = procedure.edge(pos);
        Identifier toId = edge.to().id();
        if (roles.containsKey(toId)) {
            if (edge.isRolePlayer()) removePreviousScopedRole(edge.asRolePlayer().scope(), toId, edge);
            else if (toId.isScoped()) removePreviousScopedRole(toId.asScoped().scope(), toId, edge);
        }
    }

    private void removePreviousScopedRole(Identifier.Variable scope, Identifier toId, ProcedureEdge<?, ?> edge) {
        ThingVertex previousRole = roles.remove(toId);
        if (previousRole != null) {
            assert scoped.containsKey(scope);
            scoped.get(scope).remove(previousRole);
            scopedEdgeOrders.get(scope).remove(edge.order());
        }
    }

    @Override
    public VertexMap next() {
        if (!hasNext()) throw new NoSuchElementException();
        state = State.EMPTY;
        return toReferenceMap(answer);
    }

    private VertexMap toReferenceMap(Map<Identifier, Vertex<?, ?>> answer) {
        return VertexMap.of(
                answer.entrySet().stream()
                        .filter(e -> e.getKey().isName() && filter.contains(e.getKey().asVariable().asName()))
                        .collect(toMap(k -> k.getKey().asVariable().reference(), Map.Entry::getValue))
        );
    }

    @Override
    public void recycle() {}

    private static class SeekStack {

        private boolean[] seek;
        private int lastPos;

        private SeekStack(int size) {
            seek = new boolean[size];
            lastPos = 0;
        }

        private void addSeeks(Set<Integer> seeks) {
            seeks.forEach(this::setSeek);
        }

        private void setSeek(int pos) {
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
