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

public class Constraint {

    private final double lowerBound;
    private final double upperBound;
    private final String name;
    private final Map<Variable, Double> coefficients;
    private MPConstraint mpConstraint;
    private ActivationStatus status;

    public Constraint(double lowerBound, double upperBound, String name) {
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
        this.coefficients = new HashMap<>();
        this.name = name;
        this.status = ActivationStatus.INACTIVE;
    }

    public void setCoefficient(Variable variable, double coeff) {
        assert status == ActivationStatus.INACTIVE;
        coefficients.put(variable, coeff);
    }

    synchronized void activate(MPSolver solver) {
        assert status == ActivationStatus.INACTIVE;
        this.mpConstraint = solver.makeConstraint(lowerBound, upperBound, name);
        coefficients.forEach((var, coeff) -> mpConstraint.setCoefficient(var.mpVariable(), coeff));
        this.status = ActivationStatus.ACTIVE;
    }

    synchronized void deactivate() {
        assert status == ActivationStatus.ACTIVE;
        this.mpConstraint.delete();
        this.status = ActivationStatus.INACTIVE;
    }
}
