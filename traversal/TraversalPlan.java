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
import grakn.core.graph.SchemaGraph;

import java.util.HashMap;
import java.util.Map;

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

public class TraversalPlan {

    private static final long TIME_LIMIT_MILLIS = 100;
    private final Map<Identifier, TraversalVertex> vertices;
    private final MPSolver solver;
    private final MPSolverParameters parameters;
    private MPSolver.ResultStatus status;
    private long totalDuration;

    TraversalPlan() {
        vertices = new HashMap<>();
        solver = MPSolver.createSolver("SCIP");
        parameters = new MPSolverParameters();
        parameters.setIntegerParam(PRESOLVE, PRESOLVE_ON.swigValue());
        parameters.setIntegerParam(INCREMENTALITY, INCREMENTALITY_ON.swigValue());
        status = MPSolver.ResultStatus.NOT_SOLVED;
        totalDuration = 0;
    }

    TraversalVertex vertex(Identifier identifier) {
        return vertices.computeIfAbsent(identifier, i -> new TraversalVertex(i, this));
    }

    MPSolver solver() {
        return solver;
    }

    boolean isPlanned() {
        return status == FEASIBLE || status == OPTIMAL;
    }

    boolean isOptimal() {
        return status == OPTIMAL;
    }

    boolean isError() {
        return status == INFEASIBLE || status == UNBOUNDED || status == ABNORMAL;
    }

    boolean updateCostAndCheckIsOptimal(SchemaGraph schema) {
        return false;
    }

    synchronized void optimise() {
        do {
            totalDuration += TIME_LIMIT_MILLIS;
            solver.setTimeLimit(totalDuration);
            status = solver.solve(parameters);
            if (isError()) throw GraknException.of(UNEXPECTED_PLANNING_ERROR);
        } while (!isPlanned());
    }
}
