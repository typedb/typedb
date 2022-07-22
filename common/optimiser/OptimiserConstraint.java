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
