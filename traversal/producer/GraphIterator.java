/*
 * Copyright (C) 2020 Grakn Labs
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

package grakn.core.traversal.producer;

import grakn.core.common.exception.GraknException;
import grakn.core.common.iterator.ResourceIterator;
import grakn.core.graph.GraphManager;
import grakn.core.graph.vertex.Vertex;
import grakn.core.traversal.Traversal;
import grakn.core.traversal.common.Identifier;
import grakn.core.traversal.common.VertexMap;
import grakn.core.traversal.procedure.Procedure;
import grakn.core.traversal.procedure.ProcedureEdge;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static java.util.stream.Collectors.toMap;

public class GraphIterator implements ResourceIterator<VertexMap> {

    private final Procedure procedure;
    private final Traversal.Parameters parameters;
    private final Map<Identifier, ResourceIterator<? extends Vertex<?, ?>>> iterators;
    private final Map<Identifier, Vertex<?, ?>> answer;
    private final SeekStack computeFirstSeekStack;
    private final int edgeCount;
    private final GraphManager graphMgr;
    private int computeNextSeekPos;
    private State state;

    enum State {INIT, EMPTY, FETCHED, COMPLETED}

    public GraphIterator(GraphManager graphMgr, Vertex<?, ?> start, Procedure procedure, Traversal.Parameters parameters) {
        assert procedure.edgesCount() > 0;
        this.graphMgr = graphMgr;
        this.procedure = procedure;
        this.parameters = parameters;
        this.edgeCount = procedure.edgesCount();
        this.iterators = new HashMap<>();
        this.answer = new HashMap<>();
        this.answer.put(procedure.startVertex().id(), start);
        this.computeFirstSeekStack = new SeekStack(edgeCount);
        this.state = State.INIT;
    }

    @Override
    public boolean hasNext() {
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
    }

    private boolean computeFirst(int pos) {
        if (answer.containsKey(procedure.edge(pos).to().id())) return computeFirstClosure(pos);
        else return computeFirstBranch(pos);
    }

    private boolean computeFirstBranch(int pos) {
        ProcedureEdge<?, ?> edge = procedure.edge(pos);
        Identifier toID = edge.to().id();
        ResourceIterator<? extends Vertex<?, ?>> toIter =
                edge.branchFrom(graphMgr, answer.get(edge.from().id()), parameters);
        if (!toID.isNamedReference() && edge.to().outs().isEmpty() && edge.to().ins().size() == 1) {
            toIter = toIter.limit(1);
        }

        if (toIter.hasNext()) {
            iterators.put(toID, toIter);
            answer.put(toID, toIter.next());
            if (pos == edgeCount) return true;
            while (!computeFirst(pos + 1)) {
                if (pos == computeFirstSeekStack.peekLastPos()) {
                    computeFirstSeekStack.popLastPos();
                    if (toIter.hasNext()) answer.put(toID, toIter.next());
                    else {
                        answer.remove(toID);
                        computeFirstSeekStack.addSeeks(edge.from().dependedEdgeOrders());
                        return false;
                    }
                } else {
                    answer.remove(toID);
                    toIter.recycle();
                    return false;
                }
            }
            return true;
        } else {
            computeFirstSeekStack.addSeeks(edge.from().dependedEdgeOrders());
            return false;
        }
    }

    private boolean computeFirstClosure(int pos) {
        ProcedureEdge<?, ?> edge = procedure.edge(pos);
        if (edge.isClosure(graphMgr, answer.get(edge.from().id()), answer.get(edge.to().id()), parameters)) {
            if (pos == edgeCount) return true;
            else return computeFirst(pos + 1);
        } else {
            computeFirstSeekStack.addSeeks(edge.from().dependedEdgeOrders());
            computeFirstSeekStack.addSeeks(edge.to().dependedEdgeOrders());
            return false;
        }
    }

    private boolean computeNext(int pos) {
        ProcedureEdge<?, ?> edge = procedure.edge(pos);
        Identifier toID = edge.to().id();

        if (pos == computeNextSeekPos) {
            computeNextSeekPos = edgeCount;
        } else if (pos > computeNextSeekPos) {
            if (!edge.isClosureEdge()) iterators.get(toID).recycle();
            if (!computeNext(pos - 1)) return false;
            else if (edge.isClosureEdge()) {
                Vertex<?, ?> fromVertex = answer.get(edge.from().id());
                Vertex<?, ?> toVertex = answer.get(edge.to().id());
                if (edge.isClosure(graphMgr, fromVertex, toVertex, parameters)) return true;
                else return computeNextClosure(pos);
            } else {
                iterators.put(toID, edge.branchFrom(graphMgr, answer.get(edge.from().id()), parameters));
            }
        } else if (edge.isClosureEdge()) {
            return computeNextClosure(pos);
        }

        if (iterators.get(toID).hasNext()) {
            answer.put(toID, iterators.get(toID).next());
            return true;
        } else {
            return computeNextBranch(pos);
        }
    }

    private boolean computeNextClosure(int pos) {
        ProcedureEdge<?, ?> edge = procedure.edge(pos);
        do {
            if (computeNext(pos - 1)) {
                Vertex<?, ?> fromVertex = answer.get(edge.from().id());
                Vertex<?, ?> toVertex = answer.get(edge.to().id());
                if (edge.isClosure(graphMgr, fromVertex, toVertex, parameters)) return true;
            } else {
                return false;
            }
        } while (true);
    }

    private boolean computeNextBranch(int pos) {
        ProcedureEdge<?, ?> edge = procedure.edge(pos);
        ResourceIterator<? extends Vertex<?, ?>> newIter;
        do {
            if (computeNext(pos - 1)) {
                Vertex<?, ?> fromVertex = answer.get(edge.from().id());
                newIter = edge.branchFrom(graphMgr, fromVertex, parameters);
                if (!newIter.hasNext()) {
                    assert !edge.from().ins().isEmpty();
                    computeNextSeekPos = edge.from().branchEdge().order();
                }
            } else {
                return false;
            }
        } while (!newIter.hasNext());
        iterators.put(edge.to().id(), newIter);
        answer.put(edge.to().id(), newIter.next());
        return true;
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
                        .filter(e -> e.getKey().isNamedReference())
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
            lastPos = 1;
        }

        private void addSeeks(Set<Integer> seeks) {
            seeks.forEach(this::setSeek);
        }

        private void setSeek(int pos) {
            seek[pos - 1] = true;
            if (pos > lastPos) lastPos = pos;
        }

        private int popLastPos() {
            seek[lastPos - 1] = false;
            int currentLastPos = lastPos;

            for (int p = lastPos - 1; p >= 0; p--) {
                if (p > 1 && seek[p - 1]) {
                    lastPos = p;
                    break;
                }
            }
            return currentLastPos;
        }

        private int peekLastPos() {
            return lastPos;
        }
    }
}
