/*
 * Copyright (C) 2022 Vaticle
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

package com.vaticle.typedb.core.common.optimiser;

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
    private MPSolver solver;
    private MPSolverParameters parameters;
    private Status status;
    private boolean hasSolver;
    private double objectiveValueCache;

    public Optimiser() {
        variables = new ArrayList<>();
        constraints = new HashSet<>();
        objectiveCoefficients = new HashMap<>();
        status = Status.NOT_SOLVED;
        hasSolver = false;
    }

    public synchronized Status optimise(long timeLimitMillis) {
        if (hasSolver && constraintsChanged()) {
            constraints.forEach(constraint -> constraint.initialise(solver));
            status = Status.NOT_SOLVED;
        }
        if (isOptimal()) return status;
        else if (!hasSolver) initialiseSolver();
        solver.setTimeLimit(timeLimitMillis);
        status = Status.of(solver.solve(parameters));
        if (status != Status.NOT_SOLVED && status != Status.ERROR)
            variables.forEach(OptimiserVariable::recordValue);
        if (isOptimal()) releaseSolver();
        return status;
    }

    public boolean isOptimal() {
        return status == Status.OPTIMAL;
    }

    public boolean isFeasible() {
        return status == Status.FEASIBLE;
    }

    public boolean isError() {
        return status == Status.ERROR;
    }

    private boolean constraintsChanged() {
        return constraints.stream().anyMatch(c -> !c.isInitialised);
    }

    private void initialiseSolver() {
        solver = MPSolver.createSolver("SAT");
        solver.objective().setMinimization();
        parameters = new MPSolverParameters();
        parameters.setIntegerParam(PRESOLVE, PRESOLVE_ON.swigValue());
        parameters.setIntegerParam(INCREMENTALITY, INCREMENTALITY_ON.swigValue());
        variables.forEach(var -> var.initialise(solver));
        constraints.forEach(constraint -> constraint.initialise(solver));
        applyObjective();
        applyInitialisation();
        hasSolver = true;
    }

    private void releaseSolver() {
        objectiveValueCache = solver.objective().value();
        constraints.forEach(OptimiserConstraint::release);
        variables.forEach(OptimiserVariable::release);
        parameters.delete();
        solver.delete();
        hasSolver = false;
    }

    private void applyObjective() {
        objectiveCoefficients.forEach((var, coeff) -> solver.objective().setCoefficient(var.mpVariable(), coeff));
    }

    public double objectiveValue() {
        if (hasSolver) return solver.objective().value();
        else return objectiveValueCache;
    }

    private void applyInitialisation() {
        assert iterate(variables).allMatch(OptimiserVariable::hasInitial);
        MPVariable[] mpVariables = new MPVariable[variables.size()];
        double[] initialisations = new double[variables.size()];
        for (int i = 0; i < variables.size(); i++) {
            mpVariables[i] = variables.get(i).mpVariable();
            initialisations[i] = variables.get(i).hint();
        }
        solver.setHint(mpVariables, initialisations);
    }

    private void clearInitialisation() {
        solver.setHint(new MPVariable[0], new double[0]);
    }

    public void setObjectiveCoefficient(OptimiserVariable<?> var, double coeff) {
        objectiveCoefficients.put(var, coeff);
        if (hasSolver) solver.objective().setCoefficient(var.mpVariable(), coeff);
        else if (isOptimal()) status = Status.FEASIBLE;
    }

    public OptimiserConstraint constraint(double lowerBound, double upperBound, String name) {
        assert status == Status.NOT_SOLVED;
        OptimiserConstraint constraint = new OptimiserConstraint(lowerBound, upperBound, name);
        constraints.add(constraint);
        return constraint;
    }

    public OptimiserVariable.Integer intVar(double lowerBound, double upperBound, String name) {
        assert status == Status.NOT_SOLVED;
        OptimiserVariable.Integer var = new OptimiserVariable.Integer(lowerBound, upperBound, name);
        variables.add(var);
        return var;
    }

    public OptimiserVariable.Boolean booleanVar(String name) {
        assert status == Status.NOT_SOLVED;
        OptimiserVariable.Boolean var = new OptimiserVariable.Boolean(name);
        variables.add(var);
        return var;
    }

    public Status status() {
        return status;
    }

    public enum Status {
        NOT_SOLVED, OPTIMAL, FEASIBLE, ERROR;

        static Status of(MPSolver.ResultStatus mpStatus) {
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
        return "Optimiser[" + "hasSolver=" + hasSolver + ", variables=" + variables.size() +
                ", constraints=" + constraints.size() + "]" + (hasSolver ? solver.exportModelAsLpFormat() : "");
    }
}
