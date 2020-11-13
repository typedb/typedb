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

package grakn.core.traversal.planner;

import com.google.ortools.linearsolver.MPSolver;
import com.google.ortools.linearsolver.MPSolverParameters;
import grakn.core.common.concurrent.ManagedBlockingQueue;
import grakn.core.common.exception.GraknException;
import grakn.core.graph.SchemaGraph;
import grakn.core.traversal.Identifier;
import grakn.core.traversal.procedure.Procedure;

import java.util.Map;
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

public class Planner {

    private static final long TIME_LIMIT_MILLIS = 100;

    private final Map<Identifier, PlannerVertex> vertices;
    private final MPSolver solver;
    private final MPSolverParameters parameters;
    private final AtomicBoolean isOptimising;
    private final ManagedBlockingQueue<Procedure> procedureHolder;
    private MPSolver.ResultStatus resultStatus;
    private Procedure procedure;
    private boolean isUpToDate;
    private long totalDuration;
    private long snapshot;

    public Planner() {
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

    public Procedure procedure() {
        if (procedure == null) {
            try {
                procedure = procedureHolder.take();
            } catch (InterruptedException e) {
                throw GraknException.of(e);
            }
        }
        return procedure;
    }

    public void optimise(SchemaGraph schema) {
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
