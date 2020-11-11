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
import grakn.core.common.exception.GraknException;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

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

public class Traversal {

    private final TraversalParameters parameters;
    private final Pattern pattern;
    private Planner planner;

    public Traversal() {
        pattern = new Pattern();
        parameters = new TraversalParameters();
    }

    public Identifier.Generated newIdentifier() {
        return pattern.newIdentifier();
    }

    void initialisePlanner(TraversalCache cache) {
        planner = cache.get(pattern, p -> {
            // TODO
            return new Planner();
        });
    }

    ResourceIterator<Map<Reference, Vertex<?, ?>>> execute(GraphManager graphMgr) {
        planner.updateCost(graphMgr.schema());
        if (!planner.isOptimal()) planner.optimise();
        return planner.plan().execute(graphMgr, parameters);
    }

    public void is(Identifier.Variable concept1, Identifier.Variable concept2) {
        pattern.edge(new TraversalEdge.Type.Equal(), concept1, concept2);
    }

    public void has(Identifier.Variable thing, Identifier.Variable attribute) {
        pattern.edge(new TraversalEdge.Type.Encoded(HAS), thing, attribute);
    }

    public void isa(Identifier thing, Identifier.Variable type) {
        isa(thing, type, true);
    }

    public void isa(Identifier thing, Identifier.Variable type, boolean isTransitive) {
        pattern.edge(new TraversalEdge.Type.Encoded(ISA), thing, type, isTransitive);
    }

    public void relating(Identifier.Variable relation, Identifier.Generated role) {
        pattern.edge(new TraversalEdge.Type.Encoded(RELATING), relation, role);
    }

    public void playing(Identifier.Variable thing, Identifier.Generated role) {
        pattern.edge(new TraversalEdge.Type.Encoded(PLAYING), thing, role);
    }

    public void rolePlayer(Identifier.Variable relation, Identifier.Variable player) {
        rolePlayer(relation, player, new String[]{});
    }

    public void rolePlayer(Identifier.Variable relation, Identifier.Variable player, String[] labels) {
        pattern.edge(new TraversalEdge.Type.Encoded(ROLEPLAYER), relation, player, labels);
    }

    public void owns(Identifier.Variable thingType, Identifier.Variable attributeType, boolean isKey) {
        if (isKey) pattern.edge(new TraversalEdge.Type.Encoded(OWNS_KEY), thingType, attributeType);
        else pattern.edge(new TraversalEdge.Type.Encoded(OWNS), thingType, attributeType);
    }

    public void plays(Identifier.Variable thingType, Identifier.Variable roleType) {
        pattern.edge(new TraversalEdge.Type.Encoded(PLAYS), thingType, roleType);
    }

    public void relates(Identifier.Variable relationType, Identifier.Variable roleType) {
        pattern.edge(new TraversalEdge.Type.Encoded(RELATES), relationType, roleType);
    }

    public void sub(Identifier.Variable subType, Identifier.Variable superType, boolean isTransitive) {
        pattern.edge(new TraversalEdge.Type.Encoded(SUB), subType, superType, isTransitive);
    }

    public void iid(Identifier.Variable thing, byte[] iid) {
        parameters.putIID(thing, iid);
        pattern.vertex(thing).property(new TraversalProperty.IID(thing));
    }

    public void type(Identifier.Variable thing, String[] labels) {
        pattern.vertex(thing).property(new TraversalProperty.Type(labels));
    }

    public void isAbstract(Identifier.Variable type) {
        pattern.vertex(type).property(new TraversalProperty.Abstract());
    }

    public void label(Identifier.Variable type, String label, @Nullable String scope) {
        pattern.vertex(type).property(new TraversalProperty.Label(label, scope));
    }

    public void regex(Identifier.Variable type, String regex) {
        pattern.vertex(type).property(new TraversalProperty.Regex(regex));
    }

    public void valueType(Identifier.Variable attributeType, GraqlArg.ValueType valueType) {
        pattern.vertex(attributeType).property(new TraversalProperty.ValueType(Encoding.ValueType.of(valueType)));
    }

    public void value(Identifier.Variable attribute, GraqlToken.Comparator comparator, String value) {
        parameters.pushValue(attribute, comparator, value);
        pattern.vertex(attribute).property(new TraversalProperty.Value(comparator, attribute));
    }

    public void value(Identifier.Variable attribute, GraqlToken.Comparator.Equality comparator, Boolean value) {
        parameters.pushValue(attribute, comparator, value);
        pattern.vertex(attribute).property(new TraversalProperty.Value(comparator, attribute));
    }

    public void value(Identifier.Variable attribute, GraqlToken.Comparator.Equality comparator, Long value) {
        parameters.pushValue(attribute, comparator, value);
        pattern.vertex(attribute).property(new TraversalProperty.Value(comparator, attribute));
    }

    public void value(Identifier.Variable attribute, GraqlToken.Comparator.Equality comparator, Double value) {
        parameters.pushValue(attribute, comparator, value);
        pattern.vertex(attribute).property(new TraversalProperty.Value(comparator, attribute));
    }

    public void value(Identifier.Variable attribute, GraqlToken.Comparator.Equality comparator, LocalDateTime value) {
        parameters.pushValue(attribute, comparator, value);
        pattern.vertex(attribute).property(new TraversalProperty.Value(comparator, attribute));
    }

    public void value(Identifier.Variable attribute1, GraqlToken.Comparator.Equality comparator, Identifier.Variable attribute2) {
        pattern.edge(new TraversalEdge.Type.Comparator(comparator), attribute1, attribute2);
    }

    static class Pattern {

        private final Map<Identifier, TraversalVertex.Pattern> vertices;
        private final Set<TraversalEdge.Pattern> edges;
        private int generatedIdentifierCount;

        private Pattern() {
            vertices = new HashMap<>();
            edges = new HashSet<>();
            generatedIdentifierCount = 0;
        }

        private TraversalVertex.Pattern vertex(Identifier identifier) {
            return vertices.computeIfAbsent(identifier, i -> new TraversalVertex.Pattern(i, this));
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
            TraversalVertex.Pattern fromVertex = vertex(from);
            TraversalVertex.Pattern toVertex = vertex(to);
            TraversalEdge.Pattern edge = new TraversalEdge.Pattern(type, fromVertex, toVertex, isTransitive, labels);
            edges.add(edge);
            fromVertex.out(edge);
            toVertex.in(edge);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            else if (o == null || getClass() != o.getClass()) return false;

            Pattern that = (Pattern) o;
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
        private MPSolver.ResultStatus resultStatus;
        private Plan plan;
        private boolean isUpToDate;
        private long totalDuration;
        private long snapshot;

        private Planner() {
            vertices = new ConcurrentHashMap<>();
            solver = MPSolver.createSolver("SCIP");
            parameters = new MPSolverParameters();
            parameters.setIntegerParam(PRESOLVE, PRESOLVE_ON.swigValue());
            parameters.setIntegerParam(INCREMENTALITY, INCREMENTALITY_ON.swigValue());
            resultStatus = MPSolver.ResultStatus.NOT_SOLVED;
            totalDuration = 0L;
        }

        MPSolver solver() {
            return solver;
        }

        private Plan plan() {
            return plan;
        }

        private void setUpToDate(boolean isUpToDate) {
            this.isUpToDate = isUpToDate;
        }

        private boolean isUpToDate() {
            return isUpToDate;
        }

        private boolean isPlanned() {
            return isUpToDate && (resultStatus == FEASIBLE || resultStatus == OPTIMAL);
        }

        private boolean isOptimal() {
            return isUpToDate && resultStatus == OPTIMAL;
        }

        private boolean isError() {
            return resultStatus == INFEASIBLE || resultStatus == UNBOUNDED || resultStatus == ABNORMAL;
        }

        private synchronized void updateCost(SchemaGraph schema) {
            if (schema.snapshot() < snapshot) {
                snapshot = schema.snapshot();

                // TODO: update the cost of every traversal vertex and edge
            }
        }

        private synchronized void optimise() {
            if (!isUpToDate() || !isOptimal()) {
                do {
                    totalDuration += TIME_LIMIT_MILLIS;
                    solver.setTimeLimit(totalDuration);
                    resultStatus = solver.solve(parameters);
                    if (isError()) throw GraknException.of(UNEXPECTED_PLANNING_ERROR);
                } while (!isPlanned());
                exportPlan();
                isUpToDate = true;
            }
        }

        private void exportPlan() {
            // TODO
        }
    }

    static class Plan {

        private final Map<Identifier, TraversalVertex.Plan> vertices;

        private Plan() {
            vertices = new ConcurrentHashMap<>();
        }

        private ResourceIterator<Map<Reference, Vertex<?, ?>>> execute(GraphManager graphMgr, TraversalParameters parameters) {
            return null; // TODO
        }
    }
}
