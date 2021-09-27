package com.vaticle.typedb.core.traversal.optimiser;

import com.google.ortools.linearsolver.MPSolverParameters;

import static com.google.ortools.linearsolver.MPSolverParameters.IncrementalityValues.INCREMENTALITY_ON;
import static com.google.ortools.linearsolver.MPSolverParameters.IntegerParam.INCREMENTALITY;
import static com.google.ortools.linearsolver.MPSolverParameters.IntegerParam.PRESOLVE;
import static com.google.ortools.linearsolver.MPSolverParameters.PresolveValues.PRESOLVE_ON;

public class Parameters {

    private final MPSolverParameters parameters;

    Parameters() {
        this.parameters = new MPSolverParameters();
    }

   void enablePresolve() {

   }

    void enableIncremental() {
   }

}

