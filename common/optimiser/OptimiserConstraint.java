/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.common.optimiser;

import com.google.ortools.linearsolver.MPConstraint;
import com.google.ortools.linearsolver.MPSolver;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class OptimiserConstraint {

    private final Optimiser optimiser;
    private final double lowerBound;
    private final double upperBound;
    private final String name;
    private final Map<OptimiserVariable<?>, Double> coefficients;
    private MPConstraint mpConstraint;

    public OptimiserConstraint(Optimiser optimiser, double lowerBound, double upperBound, String name) {
        this.optimiser = optimiser;
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
        this.coefficients = new HashMap<>();
        this.name = name;
    }

    public void setCoefficient(OptimiserVariable<?> variable, double coeff) {
        assert mpConstraint == null || coefficients.containsKey(variable);
        Double prevCoeff = coefficients.put(variable, coeff);
        if (!Objects.equals(prevCoeff, coeff)) optimiser.setConstraintsChanged();
    }

    synchronized void initialise(MPSolver solver) {
        this.mpConstraint = solver.makeConstraint(lowerBound, upperBound, name);
    }

    synchronized void setCoefficients() {
        coefficients.forEach((var, coeff) -> mpConstraint.setCoefficient(var.mpVariable(), coeff));
    }

    synchronized void release() {
        mpConstraint.delete();
        mpConstraint = null;
    }

    boolean isSatisfied() {
        double total = 0.0;
        for (Map.Entry<OptimiserVariable<?>, Double> entry : coefficients.entrySet()) {
            total += entry.getKey().valueAsDouble() * entry.getValue();
        }
        return lowerBound <= total && total <= upperBound;
    }
}
