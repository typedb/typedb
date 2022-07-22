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
import java.util.Objects;
import java.util.Set;

import static com.google.ortools.linearsolver.MPSolverParameters.IncrementalityValues.INCREMENTALITY_ON;
import static com.google.ortools.linearsolver.MPSolverParameters.IntegerParam.INCREMENTALITY;
import static com.google.ortools.linearsolver.MPSolverParameters.IntegerParam.PRESOLVE;
import static com.google.ortools.linearsolver.MPSolverParameters.PresolveValues.PRESOLVE_ON;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

/**
 * A wrapper around an OR-Tools solver, which supports an incremental solving mode on top of non-incremental solvers.
 * This implementation expects that a value is always set on each OptimiserVariable that satisfies the entire model.
 *
 * Optimiser guarantees solutions are better than the last whenever the objective has not changed.
 * Optimiser models can be modified dynamically by updating coefficients on the objective or on constraints.
 */
public class Optimiser {

    private final List<OptimiserVariable<?>> variables;
    private final Set<OptimiserConstraint> constraints;
    private final Map<OptimiserVariable<?>, Double> objectiveCoefficients;
    private MPSolver solver;
    private MPSolverParameters parameters;
    private Status status;
    private boolean isConstraintsUpToDate;
    private Double objectiveValue;

    public Optimiser() {
        variables = new ArrayList<>();
        constraints = new HashSet<>();
        objectiveCoefficients = new HashMap<>();
        status = Status.NOT_SOLVED;
        objectiveValue = null;
        isConstraintsUpToDate = false;
    }

    public synchronized Status optimise(long timeLimitMillis) {
        if (isOptimal()) return status;
        assert solutionSatisfiesModel();
        maySetSolver();
        setSolutionAsHints();
        solver.setTimeLimit(timeLimitMillis);
        status = Status.of(solver.solve(parameters));
        recordSolverValues();
        if (isOptimal()) releaseSolver();
        assert solutionSatisfiesModel();
        return status;
    }

    private void maySetSolver() {
        if (solver == null) createSolver();
        else if (isConstraintsUpToDate) {
            solver.reset();
            setConstraintCoefficients();
        }
    }

    private void recordSolverValues() {
        if ((isFeasible() || isOptimal()) && solver.objective().value() < objectiveValue()) {
            objectiveValue = solver.objective().value();
            variables.forEach(OptimiserVariable::recordSolutionValue);
        }
    }

    private boolean solutionSatisfiesModel() {
        return iterate(variables).allMatch(OptimiserVariable::isSatisfied) &&
                iterate(constraints).allMatch(OptimiserConstraint::isSatisfied);
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

    public void setConstraintsChanged() {
        isConstraintsUpToDate = true;
    }

    private void createSolver() {
        solver = MPSolver.createSolver("SAT");
        solver.objective().setMinimization();
        parameters = new MPSolverParameters();
        parameters.setIntegerParam(PRESOLVE, PRESOLVE_ON.swigValue());
        parameters.setIntegerParam(INCREMENTALITY, INCREMENTALITY_ON.swigValue());
        variables.forEach(var -> var.initialise(solver));
        constraints.forEach(constraint -> constraint.initialise(solver));
        setConstraintCoefficients();
        setObjective();
    }

    private void setConstraintCoefficients() {
        constraints.forEach(OptimiserConstraint::setCoefficients);
        isConstraintsUpToDate = false;
    }

    private void releaseSolver() {
        constraints.forEach(OptimiserConstraint::release);
        variables.forEach(OptimiserVariable::release);
        parameters.delete();
        solver.delete();
        solver = null;
    }

    private void setSolutionAsHints() {
        assert iterate(variables).allMatch(OptimiserVariable::hasValue);
        MPVariable[] mpVariables = new MPVariable[variables.size()];
        double[] hints = new double[variables.size()];
        for (int i = 0; i < variables.size(); i++) {
            mpVariables[i] = variables.get(i).mpVariable();
            hints[i] = variables.get(i).valueAsDouble();
        }
        solver.setHint(mpVariables, hints);
    }

    private void setObjective() {
        objectiveCoefficients.forEach((var, coeff) -> solver.objective().setCoefficient(var.mpVariable(), coeff));
    }

    public double objectiveValue() {
        assert !objectiveCoefficients.isEmpty();
        if (objectiveValue == null) objectiveValue = evaluateObjective();
        return objectiveValue;
    }

    private double evaluateObjective() {
        double value = 0.0;
        for (Map.Entry<OptimiserVariable<?>, Double> term : objectiveCoefficients.entrySet()) {
            value += term.getKey().valueAsDouble() * term.getValue();
        }
        return value;
    }

    public void setObjectiveCoefficient(OptimiserVariable<?> var, double coeff) {
        Double prevCoeff = objectiveCoefficients.put(var, coeff);
        if (!Objects.equals(coeff, prevCoeff)) {
            if (solver != null) solver.objective().setCoefficient(var.mpVariable(), coeff);
            else if (isOptimal()) status = Status.FEASIBLE;
            objectiveValue = null;
        }
    }

    public OptimiserConstraint constraint(double lowerBound, double upperBound, String name) {
        assert status == Status.NOT_SOLVED;
        OptimiserConstraint constraint = new OptimiserConstraint(this, lowerBound, upperBound, name);
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
        return "Optimiser[" + "hasSolver=" + (solver == null) + ", variables=" + variables.size() +
                ", constraints=" + constraints.size() + "]" + (solver != null ? solver.exportModelAsLpFormat() : "");
    }
}
