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
import com.vaticle.typedb.core.common.iterator.sorted.SortedIterator;
import com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.Forwardable;
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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.RESOURCE_CLOSED;
import static com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.ASC;
import static java.util.stream.Collectors.toMap;

public class GraphIter extends AbstractFunctionalIterator<VertexMap> {

    private static final Logger LOG = LoggerFactory.getLogger(GraphIter.class);

    private final GraphManager graphMgr;
    private final GraphProcedure procedure;
    private final Traversal.Parameters params;
    private final Set<Identifier.Variable.Retrievable> filter;
    private final Map<Identifier, Vertex<?, ?>> answer;
    private final Scopes scopes;
    private final Vertex<?, ?> start;
    private final int vertexCount;
    private State state;

    enum State {INIT, EMPTY, FETCHED, COMPLETED}

    public GraphIter(GraphManager graphMgr, Vertex<?, ?> start, GraphProcedure procedure,
                         Traversal.Parameters params, Set<Identifier.Variable.Retrievable> filter) {
        assert procedure.vertexCount() > 1;
        this.graphMgr = graphMgr;
        this.procedure = procedure;
        this.params = params;
        this.filter = filter;
        this.scopes = new Scopes();
        this.state = State.INIT;
        this.answer = new HashMap<>();
        this.vertexCount = procedure.vertexCount();
        this.start = start;
    }

    @Override
    public boolean hasNext() {
        try {
            if (state == State.COMPLETED) return false;
            else if (state == State.FETCHED) return true;
            else if (state == State.INIT) {
                if (computeStart() && computeFirst(1)) state = State.FETCHED;
                else setCompleted();
            } else if (state == State.EMPTY) {
                if (computeNext(vertexCount)) state = State.FETCHED;
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

    private boolean computeStart() {
        ProcedureVertex<?, ?> startVertex = procedure.startVertex();
        // TODO: vertices should cache their self-edges
        for (ProcedureEdge<?, ?> edge : startVertex.ins()) {
            if (edge.from().equals(startVertex) && !isClosure(edge, start, start)) {
                return false;
            }
        }

        this.answer.put(startVertex.id(), start);
        if (startVertex.id().isScoped()) {
            Scopes.Scoped scoped = scopes.getOrInitialise(startVertex.id().asScoped().scope());
            scoped.record(start.asThing(), startVertex);
        }
        return true;
    }

    private boolean computeFirst(int pos) {

    }

    private boolean computeNext(int pos) {
        return false;
    }

    @Override
    public VertexMap next() {
        if (!hasNext()) throw new NoSuchElementException();
        state = State.EMPTY;
        return toVertexMap(answer);
    }

    private void setCompleted() {
        state = State.COMPLETED;
        recycle();
    }

    private boolean isClosure(ProcedureEdge<?, ?> edge, Vertex<?, ?> fromVertex, Vertex<?, ?> toVertex) {
        if (edge.isRolePlayer()) {
            Scopes.Scoped scoped = scopes.getOrInitialise(edge.asRolePlayer().scope());
            return edge.asRolePlayer().isClosure(graphMgr, fromVertex, toVertex, params, scoped);
        } else {
            return edge.isClosure(graphMgr, fromVertex, toVertex, params);
        }
    }

    private Forwardable<? extends Vertex<?, ?>, SortedIterator.Order.Asc> branch(Vertex<?, ?> fromVertex, ProcedureEdge<?, ?> edge) {
        Forwardable<? extends Vertex<?, ?>, SortedIterator.Order.Asc> toIter;
        if (edge.to().id().isScoped()) {
            Identifier.Variable scope = edge.to().id().asScoped().scope();
            Scopes.Scoped scoped = scopes.getOrInitialise(scope);
            toIter = edge.branch(graphMgr, fromVertex, params).filter(role -> {
                if (scoped.contains(role.asThing())) return false;
                else {
                    if (scoped.containsSource(edge)) scoped.replace(role.asThing(), edge);
                    else scoped.record(role.asThing(), edge);
                    return true;
                }
            });
        } else if (edge.isRolePlayer()) {
            Identifier.Variable scope = edge.asRolePlayer().scope();
            Scopes.Scoped scoped = scopes.getOrInitialise(scope);
            toIter = edge.asRolePlayer().branchEdge(graphMgr, fromVertex, params).filter(thingAndRole -> {
                if (scoped.contains(thingAndRole.value())) return false;
                else {
                    if (scoped.containsSource(edge)) scoped.replace(thingAndRole.value(), edge);
                    else scoped.record(thingAndRole.value(), edge);
                    return true;
                }
            }).mapSorted(KeyValue::key, key -> KeyValue.of(key, null), ASC);
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

    private VertexMap toVertexMap(Map<Identifier, Vertex<?, ?>> answer) {
        return VertexMap.of(
                answer.entrySet().stream()
                        .filter(e -> e.getKey().isRetrievable() && filter.contains(e.getKey().asVariable().asRetrievable()))
                        .collect(toMap(e -> e.getKey().asVariable().asRetrievable(), Map.Entry::getValue))
        );
    }

    @Override
    public void recycle() {
        iterators.values().forEach(FunctionalIterator::recycle);
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

            Set<ThingVertex> roles;
            Map<ProcedureVertex<?, ?>, ThingVertex> vertexSources;
            Map<ProcedureEdge<?, ?>, ThingVertex> edgeSources;

            private Scoped() {
                roles = new HashSet<>();
                vertexSources = new HashMap<>();
                edgeSources = new HashMap<>();
            }

            public boolean isEmpty() {
                return roles.isEmpty();
            }

            public boolean contains(ThingVertex roleVertex) {
                return roles.contains(roleVertex);
            }

            public boolean containsSource(ProcedureVertex<?, ?> vertex) {
                return vertexSources.containsKey(vertex);
            }

            public boolean containsSource(ProcedureEdge<?, ?> edge) {
                return edgeSources.containsKey(edge);
            }

            public void record(ThingVertex role, ProcedureEdge<?, ?> edge) {
                assert !roles.contains(role) && edge.isRolePlayer();
                roles.add(role);
                edgeSources.put(edge, role);
            }

            public void record(ThingVertex role, ProcedureVertex<?, ?> vertex) {
                assert !roles.contains(role) && vertex.id().isScoped();
                roles.add(role);
                vertexSources.put(vertex, role);
            }

            public void replace(ThingVertex role, ProcedureEdge<?, ?> edge) {
                assert edge.isRolePlayer();
                ThingVertex oldRole = edgeSources.remove(edge);
                assert roles.contains(oldRole);
                roles.remove(oldRole);
                edgeSources.put(edge, role);
                roles.add(role);
            }

            public void replace(ThingVertex role, ProcedureVertex<?, ?> vertex) {
                assert vertex.id().isScoped();
                ThingVertex oldRole = vertexSources.remove(vertex);
                assert roles.contains(oldRole);
                roles.remove(oldRole);
                vertexSources.put(vertex, role);
                roles.add(role);
            }
        }
    }
}
