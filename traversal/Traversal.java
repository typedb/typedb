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
import grakn.core.graph.vertex.Vertex;
import graql.lang.common.GraqlArg;
import graql.lang.common.GraqlToken;
import graql.lang.pattern.variable.Reference;

import javax.annotation.Nullable;
import java.time.LocalDateTime;
import java.util.Map;
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

public class Traversal {

    private final TraversalParameters parameters;
    private final Pattern pattern;
    private Planner planner;

    public Traversal() {
        pattern = new Pattern();
        parameters = new TraversalParameters();
    }

    public Identifier newIdentifier() {
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

    public void has(Identifier thing, Identifier attribute) {
        TraversalVertex.Pattern thingVertex = pattern.vertex(thing);
        TraversalVertex.Pattern attributeVertex = pattern.vertex(attribute);

        // TODO
    }

    public void isa(Identifier thing, Identifier type) {
        isa(thing, type, true);
    }

    public void isa(Identifier thing, Identifier type, boolean isTransitive) {
        // TODO
    }

    public void is(Identifier first, Identifier second) {
        // TODO
    }

    public void relating(Identifier relation, Identifier role) {
        // TODO
    }

    public void playing(Identifier thing, Identifier role) {
        // TODO
    }

    public void rolePlayer(Identifier relation, Identifier player) {
        rolePlayer(relation, player, null);
    }

    public void rolePlayer(Identifier relation, Identifier player, @Nullable String roleType) {
        // TODO
    }

    public void owns(Identifier thingType, Identifier attributeType) {
        // TODO
    }

    public void plays(Identifier thingType, Identifier roleType) {
        // TODO
    }

    public void relates(Identifier relationType, Identifier roleType) {
        // TODO
    }

    public void sub(Identifier subType, Identifier superType, boolean isTransitive) {
        // TODO
    }

    public void iid(Identifier thing, byte[] iid) {
        // TODO
    }

    public void type(Identifier thing, String[] labels) {
        pattern.vertex(thing).type(labels);
    }

    public void isAbstract(Identifier type) {
        pattern.vertex(type).isAbstract();
    }

    public void label(Identifier type, String label, @Nullable String scope) {
        // TODO
    }

    public void regex(Identifier type, java.util.regex.Pattern regex) {
        // TODO
    }

    public void valueType(Identifier attributeType, GraqlArg.ValueType valueType) {
        // TODO
    }

    public void value(Identifier attribute, GraqlToken.Comparator comparator, Boolean value) {
        // TODO
    }

    public void value(Identifier attribute, GraqlToken.Comparator comparator, Long value) {
        // TODO
    }

    public void value(Identifier attribute, GraqlToken.Comparator comparator, Double value) {
        // TODO
    }

    public void value(Identifier attribute, GraqlToken.Comparator comparator, String value) {
        // TODO
    }

    public void value(Identifier attribute, GraqlToken.Comparator comparator, LocalDateTime value) {
        // TODO
    }

    public void value(Identifier attribute1, GraqlToken.Comparator comparator, Identifier attribute2) {
        // TODO
    }

    static class Pattern {

        private final Map<Identifier, TraversalVertex.Pattern> vertices;
        private int generatedIdentifierCount;

        private Pattern() {
            vertices = new ConcurrentHashMap<>();
            generatedIdentifierCount = 0;
        }

        private TraversalVertex.Pattern vertex(Identifier identifier) {
            return vertices.computeIfAbsent(identifier, i -> new TraversalVertex.Pattern(i, this));
        }

        private Identifier newIdentifier() {
            return Identifier.Generated.of(generatedIdentifierCount++);
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

        private synchronized void updateCost(SchemaGraph schema) {
            if (schema.snapshot() < snapshot) {
                snapshot = schema.snapshot();

                // TODO: update the cost of every traversal vertex and edge
            }
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
            return isUpToDate && (resultStatus == FEASIBLE || resultStatus == OPTIMAL);
        }

        private boolean isOptimal() {
            return isUpToDate && resultStatus == OPTIMAL;
        }

        private boolean isError() {
            return resultStatus == INFEASIBLE || resultStatus == UNBOUNDED || resultStatus == ABNORMAL;
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

        private Plan plan() {
            return plan;
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
