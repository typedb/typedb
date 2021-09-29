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

package com.vaticle.typedb.core.traversal.optimiser;

import com.google.ortools.linearsolver.MPSolver;
import com.google.ortools.linearsolver.MPSolverParameters;
import com.google.ortools.linearsolver.MPVariable;
import com.vaticle.typedb.core.common.exception.TypeDBException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.ortools.linearsolver.MPSolverParameters.IncrementalityValues.INCREMENTALITY_ON;
import static com.google.ortools.linearsolver.MPSolverParameters.IntegerParam.INCREMENTALITY;
import static com.google.ortools.linearsolver.MPSolverParameters.IntegerParam.PRESOLVE;
import static com.google.ortools.linearsolver.MPSolverParameters.PresolveValues.PRESOLVE_ON;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public class Solver {

    private final List<Variable> variables;
    private final Set<Constraint> constraints;
    private final Map<Variable, Double> objectiveCoefficients;
    private SolverStatus status;
    private MPSolver solver;
    private MPSolverParameters parameters;

    private enum SolverStatus {
        INACTIVE, ACTIVE
    }

    public Solver() {
        variables = new ArrayList<>();
        constraints = new HashSet<>();
        objectiveCoefficients = new HashMap<>();
        status = SolverStatus.INACTIVE;
    }

    public synchronized ResultStatus solve(long timeLimitMillis) {
        if (status == SolverStatus.INACTIVE) activate();
        solver.setTimeLimit(timeLimitMillis);
        ResultStatus resultStatus = ResultStatus.of(solver.solve(parameters));
        variables.forEach(Variable::recordValue);
        clearInitialisation();
        return resultStatus;
    }

    public synchronized void deactivate() {
        solver.delete();
        parameters.delete();
        variables.forEach(Variable::deactivate);
        constraints.forEach(Constraint::deactivate);
        status = SolverStatus.INACTIVE;
    }

    private void activate() {
        solver = MPSolver.createSolver("SCIP");
        solver.objective().setMinimization();
        parameters = new MPSolverParameters();
        parameters.setIntegerParam(PRESOLVE, PRESOLVE_ON.swigValue());
        parameters.setIntegerParam(INCREMENTALITY, INCREMENTALITY_ON.swigValue());
        variables.forEach(var -> var.activate(solver));
        constraints.forEach(constraint -> constraint.activate(solver));
        applyObjective();
        applyInitialisation();
        status = SolverStatus.ACTIVE;
    }

    private void applyObjective() {
        objectiveCoefficients.forEach((var, coeff) -> solver.objective().setCoefficient(var.mpVariable(), coeff));
    }

    private void applyInitialisation() {
        assert iterate(variables).allMatch(Variable::hasInitial);
        MPVariable[] mpVariables = new MPVariable[variables.size()];
        double[] initialisations = new double[variables.size()];
        for (int i = 0; i < variables.size(); i++) {
            mpVariables[i] = variables.get(i).mpVariable();
            initialisations[i] = variables.get(i).getInitial();
        }
        solver.setHint(mpVariables, initialisations);
    }

    private void clearInitialisation() {
        assert status == SolverStatus.ACTIVE;
        solver.setHint(new MPVariable[0], new double[0]);
    }

    public void setObjectiveCoefficient(Variable var, double coeff) {
        objectiveCoefficients.put(var, coeff);
        if (status == SolverStatus.ACTIVE) solver.objective().setCoefficient(var.mpVariable(), coeff);
    }

    public Constraint makeConstraint(double lowerBound, double upperBound, String name) {
        assert status == SolverStatus.INACTIVE;
        Constraint constraint = new Constraint(lowerBound, upperBound, name);
        constraints.add(constraint);
        return constraint;
    }

    public IntVariable makeIntVar(double lowerBound, double upperBound, String name) {
        assert status == SolverStatus.INACTIVE;
        IntVariable var = new IntVariable(lowerBound, upperBound, name);
        variables.add(var);
        return var;
    }

    public BoolVariable makeBoolVar(String name) {
        assert status == SolverStatus.INACTIVE;
        BoolVariable var = new BoolVariable(name);
        variables.add(var);
        return var;
    }

    public enum ResultStatus {
        NOT_SOLVED, OPTIMAL, FEASIBLE, ERROR;

        static ResultStatus of(MPSolver.ResultStatus mpStatus) {
            switch (mpStatus) {
                case NOT_SOLVED:
                    return NOT_SOLVED;
                case OPTIMAL:
                    return OPTIMAL;
                case FEASIBLE:
                    return FEASIBLE;
                case INFEASIBLE:
                case UNBOUNDED:
                case ABNORMAL:
                    return ERROR;
                default:
                    throw TypeDBException.of(ILLEGAL_STATE);
            }
        }
    }

    @Override
    public String toString() {
        return "Optimiser [" + "status=" + status + ", variables=" + variables.size() +
                ", constraints=" + constraints.size() + "]" + (solver == null ? "" : solver.exportModelAsLpFormat());
    }
}
