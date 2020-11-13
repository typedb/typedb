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

package grakn.core.traversal;

import com.google.ortools.linearsolver.MPSolver;
import com.google.ortools.linearsolver.MPSolverParameters;
import grakn.core.common.concurrent.ManagedBlockingQueue;
import grakn.core.common.exception.GraknException;
import grakn.core.common.iterator.Iterators;
import grakn.core.common.iterator.ResourceIterator;
import grakn.core.graph.GraphManager;
import grakn.core.graph.SchemaGraph;
import grakn.core.graph.util.Encoding;
import grakn.core.graph.vertex.Vertex;
import graql.lang.common.GraqlArg;
import graql.lang.common.GraqlToken;
import graql.lang.pattern.variable.Reference;

import javax.annotation.Nullable;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.ortools.linearsolver.MPSolver.ResultStatus.ABNORMAL;
import static com.google.ortools.linearsolver.MPSolver.ResultStatus.FEASIBLE;
import static com.google.ortools.linearsolver.MPSolver.ResultStatus.INFEASIBLE;
import static com.google.ortools.linearsolver.MPSolver.ResultStatus.OPTIMAL;
import static com.google.ortools.linearsolver.MPSolver.ResultStatus.UNBOUNDED;
import static com.google.ortools.linearsolver.MPSolverParameters.IncrementalityValues.INCREMENTALITY_ON;
import static com.google.ortools.linearsolver.MPSolverParameters.IntegerParam.INCREMENTALITY;
import static com.google.ortools.linearsolver.MPSolverParameters.IntegerParam.PRESOLVE;
import static com.google.ortools.linearsolver.MPSolverParameters.PresolveValues.PRESOLVE_ON;
import static grakn.core.common.exception.ErrorMessage.Internal.UNEXPECTED_PLANNING_ERROR;
import static grakn.core.graph.util.Encoding.Edge.ISA;
import static grakn.core.graph.util.Encoding.Edge.Thing.HAS;
import static grakn.core.graph.util.Encoding.Edge.Thing.PLAYING;
import static grakn.core.graph.util.Encoding.Edge.Thing.RELATING;
import static grakn.core.graph.util.Encoding.Edge.Thing.ROLEPLAYER;
import static grakn.core.graph.util.Encoding.Edge.Type.OWNS;
import static grakn.core.graph.util.Encoding.Edge.Type.OWNS_KEY;
import static grakn.core.graph.util.Encoding.Edge.Type.PLAYS;
import static grakn.core.graph.util.Encoding.Edge.Type.RELATES;
import static grakn.core.graph.util.Encoding.Edge.Type.SUB;
import static java.util.stream.Collectors.toList;

public class Traversal {

    private final TraversalParameters parameters;
    private final Structure structure;
    private List<Planner> planners;

    public Traversal() {
        structure = new Structure();
        parameters = new TraversalParameters();
    }

    public Identifier.Generated newIdentifier() {
        return structure.newIdentifier();
    }

    void initialisePlanner(TraversalCache cache) {
        planners = structure.graphs().stream().map(p1 -> cache.get(p1, p2 -> {
            // TODO
            return new Planner();
        })).collect(toList());
    }

    ResourceIterator<Map<Reference, Vertex<?, ?>>> execute(GraphManager graphMgr) {
        if (planners.size() == 1) {
            planners.get(0).optimise(graphMgr.schema());
            return planners.get(0).procedure().execute(graphMgr, parameters);
        } else {
            return Iterators.cartesian(planners.stream().map(planner -> {
                planner.optimise(graphMgr.schema());
                return planner.procedure().execute(graphMgr, parameters);
            }).collect(toList())).map(list -> {
                Map<Reference, Vertex<?, ?>> answer = new HashMap<>();
                list.forEach(answer::putAll);
                return answer;
            });
        }
    }

    public void is(Identifier.Variable concept1, Identifier.Variable concept2) {
        structure.edge(new TraversalEdge.Type.Equal(), concept1, concept2);
    }

    public void has(Identifier.Variable thing, Identifier.Variable attribute) {
        structure.edge(new TraversalEdge.Type.Encoded(HAS), thing, attribute);
    }

    public void isa(Identifier thing, Identifier.Variable type) {
        isa(thing, type, true);
    }

    public void isa(Identifier thing, Identifier.Variable type, boolean isTransitive) {
        structure.edge(new TraversalEdge.Type.Encoded(ISA), thing, type, isTransitive);
    }

    public void relating(Identifier.Variable relation, Identifier.Generated role) {
        structure.edge(new TraversalEdge.Type.Encoded(RELATING), relation, role);
    }

    public void playing(Identifier.Variable thing, Identifier.Generated role) {
        structure.edge(new TraversalEdge.Type.Encoded(PLAYING), thing, role);
    }

    public void rolePlayer(Identifier.Variable relation, Identifier.Variable player) {
        rolePlayer(relation, player, new String[]{});
    }

    public void rolePlayer(Identifier.Variable relation, Identifier.Variable player, String[] labels) {
        structure.edge(new TraversalEdge.Type.Encoded(ROLEPLAYER), relation, player, labels);
    }

    public void owns(Identifier.Variable thingType, Identifier.Variable attributeType, boolean isKey) {
        if (isKey) structure.edge(new TraversalEdge.Type.Encoded(OWNS_KEY), thingType, attributeType);
        else structure.edge(new TraversalEdge.Type.Encoded(OWNS), thingType, attributeType);
    }

    public void plays(Identifier.Variable thingType, Identifier.Variable roleType) {
        structure.edge(new TraversalEdge.Type.Encoded(PLAYS), thingType, roleType);
    }

    public void relates(Identifier.Variable relationType, Identifier.Variable roleType) {
        structure.edge(new TraversalEdge.Type.Encoded(RELATES), relationType, roleType);
    }

    public void sub(Identifier.Variable subType, Identifier.Variable superType, boolean isTransitive) {
        structure.edge(new TraversalEdge.Type.Encoded(SUB), subType, superType, isTransitive);
    }

    public void iid(Identifier.Variable thing, byte[] iid) {
        parameters.putIID(thing, iid);
        structure.vertex(thing).property(new TraversalProperty.IID(thing));
    }

    public void type(Identifier.Variable thing, String[] labels) {
        structure.vertex(thing).property(new TraversalProperty.Type(labels));
    }

    public void isAbstract(Identifier.Variable type) {
        structure.vertex(type).property(new TraversalProperty.Abstract());
    }

    public void label(Identifier.Variable type, String label, @Nullable String scope) {
        structure.vertex(type).property(new TraversalProperty.Label(label, scope));
    }

    public void regex(Identifier.Variable type, String regex) {
        structure.vertex(type).property(new TraversalProperty.Regex(regex));
    }

    public void valueType(Identifier.Variable attributeType, GraqlArg.ValueType valueType) {
        structure.vertex(attributeType).property(new TraversalProperty.ValueType(Encoding.ValueType.of(valueType)));
    }

    public void value(Identifier.Variable attribute, GraqlToken.Comparator comparator, String value) {
        parameters.pushValue(attribute, comparator, value);
        structure.vertex(attribute).property(new TraversalProperty.Value(comparator, attribute));
    }

    public void value(Identifier.Variable attribute, GraqlToken.Comparator.Equality comparator, Boolean value) {
        parameters.pushValue(attribute, comparator, value);
        structure.vertex(attribute).property(new TraversalProperty.Value(comparator, attribute));
    }

    public void value(Identifier.Variable attribute, GraqlToken.Comparator.Equality comparator, Long value) {
        parameters.pushValue(attribute, comparator, value);
        structure.vertex(attribute).property(new TraversalProperty.Value(comparator, attribute));
    }

    public void value(Identifier.Variable attribute, GraqlToken.Comparator.Equality comparator, Double value) {
        parameters.pushValue(attribute, comparator, value);
        structure.vertex(attribute).property(new TraversalProperty.Value(comparator, attribute));
    }

    public void value(Identifier.Variable attribute, GraqlToken.Comparator.Equality comparator, LocalDateTime value) {
        parameters.pushValue(attribute, comparator, value);
        structure.vertex(attribute).property(new TraversalProperty.Value(comparator, attribute));
    }

    public void value(Identifier.Variable attribute1, GraqlToken.Comparator.Equality comparator, Identifier.Variable attribute2) {
        structure.edge(new TraversalEdge.Type.Comparator(comparator), attribute1, attribute2);
    }

    static class Structure {

        private final Map<Identifier, TraversalVertex.Structure> vertices;
        private final Set<TraversalEdge.Structure> edges;
        private int generatedIdentifierCount;
        private List<Structure> patterns;

        private Structure() {
            vertices = new HashMap<>();
            edges = new HashSet<>();
            generatedIdentifierCount = 0;
        }

        private TraversalVertex.Structure vertex(Identifier identifier) {
            return vertices.computeIfAbsent(identifier, i -> new TraversalVertex.Structure(i, this));
        }

        private Identifier.Generated newIdentifier() {
            return Identifier.Generated.of(generatedIdentifierCount++);
        }

        private void edge(TraversalEdge.Type type, Identifier from, Identifier to) {
            edge(type, from, to, false, new String[]{});
        }

        private void edge(TraversalEdge.Type type, Identifier from, Identifier to, boolean isTransitive) {
            edge(type, from, to, isTransitive, new String[]{});
        }

        private void edge(TraversalEdge.Type type, Identifier from, Identifier to, String[] labels) {
            edge(type, from, to, false, labels);
        }

        private void edge(TraversalEdge.Type type, Identifier from, Identifier to, boolean isTransitive, String[] labels) {
            TraversalVertex.Structure fromVertex = vertex(from);
            TraversalVertex.Structure toVertex = vertex(to);
            TraversalEdge.Structure edge = new TraversalEdge.Structure(type, fromVertex, toVertex, isTransitive, labels);
            edges.add(edge);
            fromVertex.out(edge);
            toVertex.in(edge);
        }

        private List<Structure> graphs() {
            if (patterns == null) {
                patterns = new ArrayList<>();
                while (!vertices.isEmpty()) {
                    Structure newPattern = new Structure();
                    splitGraph(vertices.values().iterator().next(), newPattern);
                    patterns.add(newPattern);
                }
            }
            return patterns;
        }

        private void splitGraph(TraversalVertex.Structure vertex, Structure newPattern) {
            if (!vertices.containsKey(vertex.identifier())) return;

            this.vertices.remove(vertex.identifier());
            newPattern.vertices.put(vertex.identifier(), vertex);
            vertex.outs().forEach(outgoing -> {
                if (this.edges.contains(outgoing)) {
                    this.edges.remove(outgoing);
                    newPattern.edges.add(outgoing);
                    splitGraph(outgoing.to(), newPattern);
                }
            });
            vertex.ins().forEach(incoming -> {
                if (this.edges.contains(incoming)) {
                    this.edges.remove(incoming);
                    newPattern.edges.add(incoming);
                    splitGraph(incoming.from(), newPattern);
                }
            });
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            else if (o == null || getClass() != o.getClass()) return false;

            Structure that = (Structure) o;
            return (this.vertices.equals(that.vertices) && this.edges.equals(that.edges));
        }

        @Override
        public int hashCode() {
            return Objects.hash(this.vertices, this.edges);
        }
    }

    static class Planner {

        private static final long TIME_LIMIT_MILLIS = 100;

        private final Map<Identifier, TraversalVertex.Planner> vertices;
        private final MPSolver solver;
        private final MPSolverParameters parameters;
        private final AtomicBoolean isOptimising;
        private final ManagedBlockingQueue<Procedure> procedureHolder;
        private MPSolver.ResultStatus resultStatus;
        private Procedure procedure;
        private boolean isUpToDate;
        private long totalDuration;
        private long snapshot;

        private Planner() {
            vertices = new ConcurrentHashMap<>();
            solver = MPSolver.createSolver("SCIP");
            isOptimising = new AtomicBoolean(false);
            parameters = new MPSolverParameters();
            parameters.setIntegerParam(PRESOLVE, PRESOLVE_ON.swigValue());
            parameters.setIntegerParam(INCREMENTALITY, INCREMENTALITY_ON.swigValue());
            resultStatus = MPSolver.ResultStatus.NOT_SOLVED;
            procedureHolder = new ManagedBlockingQueue<>(1);
            totalDuration = 0L;
        }

        MPSolver solver() {
            return solver;
        }

        private void setUpToDate(boolean isUpToDate) {
            this.isUpToDate = isUpToDate;
        }

        private boolean isUpToDate() {
            return isUpToDate;
        }

        private boolean isPlanned() {
            return resultStatus == FEASIBLE || resultStatus == OPTIMAL;
        }

        private boolean isOptimal() {
            return resultStatus == OPTIMAL;
        }

        private boolean isError() {
            return resultStatus == INFEASIBLE || resultStatus == UNBOUNDED || resultStatus == ABNORMAL;
        }

        private Procedure procedure() {
            if (procedure == null) {
                try {
                    procedure = procedureHolder.take();
                } catch (InterruptedException e) {
                    throw GraknException.of(e);
                }
            }
            return procedure;
        }

        private void optimise(SchemaGraph schema) {
            if (isOptimising.compareAndSet(false, true)) {
                updateCost(schema);
                if (!isUpToDate() || !isOptimal()) {
                    do {
                        totalDuration += TIME_LIMIT_MILLIS;
                        solver.setTimeLimit(totalDuration);
                        resultStatus = solver.solve(parameters);
                        if (isError()) throw GraknException.of(UNEXPECTED_PLANNING_ERROR);
                    } while (!isPlanned());
                    exportProcedure();
                    isUpToDate = true;
                }
                isOptimising.set(false);
            }
        }

        private void updateCost(SchemaGraph schema) {
            if (schema.snapshot() < snapshot) {
                snapshot = schema.snapshot();

                // TODO: update the cost of every traversal vertex and edge
            }
        }

        private void exportProcedure() {
            Procedure newPlan = new Procedure();

            // TODO: extract Traversal Procedure from the MPVariables of Traversal Planner

            procedureHolder.clear();
            try {
                procedureHolder.put(newPlan);
            } catch (InterruptedException e) {
                throw GraknException.of(e);
            }
            procedure = null;
        }
    }

    static class Procedure {

        private final Map<Identifier, TraversalVertex.Procedure> vertices;

        private Procedure() {
            vertices = new ConcurrentHashMap<>();
        }

        private ResourceIterator<Map<Reference, Vertex<?, ?>>> execute(GraphManager graphMgr, TraversalParameters parameters) {
            return null; // TODO
        }
    }
}
