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

public class Optimiser {

    private final List<OptimiserVariable<?>> variables;
    private final Set<OptimiserConstraint> constraints;
    private final Map<OptimiserVariable<?>, Double> objectiveCoefficients;
    private State state;
    private MPSolver solver;
    private MPSolverParameters parameters;

    private enum State {INACTIVE, ACTIVE}

    public Optimiser() {
        variables = new ArrayList<>();
        constraints = new HashSet<>();
        objectiveCoefficients = new HashMap<>();
        state = State.INACTIVE;
    }

    public synchronized ResultStatus optimise(long timeLimitMillis) {
        if (state == State.INACTIVE) activate();
        solver.setTimeLimit(timeLimitMillis);
        ResultStatus resultStatus = ResultStatus.of(solver.solve(parameters));
        variables.forEach(OptimiserVariable::recordValue);
        clearInitialisation();
        return resultStatus;
    }

    public boolean isActive() {
        return state == State.ACTIVE;
    }

    public synchronized void deactivate() {
        state = State.INACTIVE;
        constraints.forEach(OptimiserConstraint::deactivate);
        variables.forEach(OptimiserVariable::deactivate);
        parameters.delete();
        solver.delete();
    }

    private synchronized void activate() {
        solver = MPSolver.createSolver("SCIP");
        solver.objective().setMinimization();
        parameters = new MPSolverParameters();
        parameters.setIntegerParam(PRESOLVE, PRESOLVE_ON.swigValue());
        parameters.setIntegerParam(INCREMENTALITY, INCREMENTALITY_ON.swigValue());
        variables.forEach(var -> var.activate(solver));
        constraints.forEach(constraint -> constraint.activate(solver));
        applyObjective();
        applyInitialisation();
        state = State.ACTIVE;
    }

    private void applyObjective() {
        objectiveCoefficients.forEach((var, coeff) -> solver.objective().setCoefficient(var.mpVariable(), coeff));
    }

    private void applyInitialisation() {
        assert iterate(variables).allMatch(OptimiserVariable::hasInitial);
        MPVariable[] mpVariables = new MPVariable[variables.size()];
        double[] initialisations = new double[variables.size()];
        for (int i = 0; i < variables.size(); i++) {
            mpVariables[i] = variables.get(i).mpVariable();
            initialisations[i] = variables.get(i).getInitial();
        }
        solver.setHint(mpVariables, initialisations);
    }

    private void clearInitialisation() {
        assert state == State.ACTIVE;
        solver.setHint(new MPVariable[0], new double[0]);
    }

    public void setObjectiveCoefficient(OptimiserVariable<?> var, double coeff) {
        objectiveCoefficients.put(var, coeff);
        if (state == State.ACTIVE) solver.objective().setCoefficient(var.mpVariable(), coeff);
    }

    public OptimiserConstraint constraint(double lowerBound, double upperBound, String name) {
        assert state == State.INACTIVE;
        OptimiserConstraint constraint = new OptimiserConstraint(lowerBound, upperBound, name);
        constraints.add(constraint);
        return constraint;
    }

    public OptimiserVariable.Integer intVar(double lowerBound, double upperBound, String name) {
        assert state == State.INACTIVE;
        OptimiserVariable.Integer var = new OptimiserVariable.Integer(lowerBound, upperBound, name);
        variables.add(var);
        return var;
    }

    public OptimiserVariable.Boolean booleanVar(String name) {
        assert state == State.INACTIVE;
        OptimiserVariable.Boolean var = new OptimiserVariable.Boolean(name);
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
        return "Optimiser[" + "status=" + state + ", variables=" + variables.size() +
                ", constraints=" + constraints.size() + "]" +
                (state == State.ACTIVE ? solver.exportModelAsLpFormat() : "");
    }
}
