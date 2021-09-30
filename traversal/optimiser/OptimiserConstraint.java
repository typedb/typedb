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
    private State state;

    private enum State {INACTIVE, ACTIVE}

    public OptimiserConstraint(double lowerBound, double upperBound, String name) {
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
        this.coefficients = new HashMap<>();
        this.name = name;
        this.state = State.INACTIVE;
    }

    public void setCoefficient(OptimiserVariable<?> variable, double coeff) {
        assert state == State.INACTIVE;
        coefficients.put(variable, coeff);
    }

    synchronized void activate(MPSolver solver) {
        assert state == State.INACTIVE;
        this.mpConstraint = solver.makeConstraint(lowerBound, upperBound, name);
        coefficients.forEach((var, coeff) -> mpConstraint.setCoefficient(var.mpVariable(), coeff));
        this.state = State.ACTIVE;
    }

    synchronized void deactivate() {
        assert state == State.ACTIVE;
        this.mpConstraint.delete();
        this.state = State.INACTIVE;
    }
}
