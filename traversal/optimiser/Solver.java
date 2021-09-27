package com.vaticle.typedb.core.traversal.optimiser;

import com.google.ortools.linearsolver.MPSolver;
import com.google.ortools.linearsolver.MPSolverParameters;
import com.google.ortools.linearsolver.MPVariable;
import com.google.ortools.sat.IntVar;
import com.vaticle.typedb.core.common.exception.TypeDBException;

import java.util.HashMap;
import java.util.Map;

import static com.google.ortools.linearsolver.MPSolverParameters.IncrementalityValues.INCREMENTALITY_ON;
import static com.google.ortools.linearsolver.MPSolverParameters.IntegerParam.INCREMENTALITY;
import static com.google.ortools.linearsolver.MPSolverParameters.IntegerParam.PRESOLVE;
import static com.google.ortools.linearsolver.MPSolverParameters.PresolveValues.PRESOLVE_ON;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;

public class Solver {

    private final Map<IntVariable, Double> objectiveCoefficients;
    private SolverStatus status;
    private MPSolver solver;
    private MPSolverParameters parameters;

    private enum SolverStatus {
        INACTIVE, ACTIVE
    }

    Solver() {
        objectiveCoefficients = new HashMap<>();
        status = SolverStatus.INACTIVE;
    }

    public ResultStatus solve(long timeLimitMillis) {
        if (status == SolverStatus.ACTIVE) {
            solver.setTimeLimit(timeLimitMillis);
            return ResultStatus.of(solver.solve(parameters));
        } else {
            solver = MPSolver.createSolver("SCIP");
            solver.objective().setMinimization();
            activate();
            parameters = new MPSolverParameters();
            parameters.setIntegerParam(PRESOLVE, PRESOLVE_ON.swigValue());
            parameters.setIntegerParam(INCREMENTALITY, INCREMENTALITY_ON.swigValue());
            solver.setTimeLimit(timeLimitMillis);
            return ResultStatus.of(solver.solve(parameters));
        }
    }

    public void deactivate() {
        // TODO think about threading
        // TODO delete all MP objects
        // TODO set to INACTIVE
    }

    private void activate() {
        // TODO pass the solver into all constraints and variables to make them active
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

    public void setObjectiveCoefficient(IntVariable var, double coeff) {
        objectiveCoefficients.put(var, coeff);
    }

    public Constraint makeConstraint(double lowerBound, double upperBound, String name) {
        return new Constraint(lowerBound, upperBound, name);
    }

    public IntVariable makeIntVar(double lowerBound, double upperBound, String name) {
        return new IntVariable(lowerBound, upperBound, name);
    }

    public void resetHints() {
        // TODO
//        solver.setHint(new MPVariable[]{}, new double[]{});
    }

    public void setHints(IntVariable[] variables, double[] initialValues) {
        // TODO
    }
}
