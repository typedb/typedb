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

public class OptimiserConstraint {

    private final double lowerBound;
    private final double upperBound;
    private final String name;
    private final Map<OptimiserVariable<?>, Double> coefficients;
    private MPConstraint mpConstraint;
    boolean isInitialised;

    public OptimiserConstraint(double lowerBound, double upperBound, String name) {
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
        this.coefficients = new HashMap<>();
        this.name = name;
        this.isInitialised = false;
    }

    public void setCoefficient(OptimiserVariable<?> variable, double coeff) {
        if (isInitialised)
            if (coefficients.containsKey(variable)) {
                isInitialised = coefficients.get(variable) == coeff;
            } else {
                isInitialised = false;
            }
        coefficients.put(variable, coeff);
    }

    synchronized void initialise(MPSolver solver) {
        if (this.mpConstraint == null) this.mpConstraint = solver.makeConstraint(lowerBound, upperBound, name);
        coefficients.forEach((var, coeff) -> mpConstraint.setCoefficient(var.mpVariable(), coeff));
        isInitialised = true;
    }

    synchronized void release() {
        this.mpConstraint.delete();
        this.mpConstraint = null;
    }
}
