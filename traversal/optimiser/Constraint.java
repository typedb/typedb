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
    private final Map<IntVariable, Double> coefficients;
    private MPConstraint mpConstraint;
    private Status status;

    private enum Status { INACTIVE, ACTIVE; }

    public Constraint(double lowerBound, double upperBound, String name) {
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
        this.coefficients = new HashMap<>();
        this.name = name;
        this.status = Status.INACTIVE;
    }

    public void setCoefficient(IntVariable variable, double coeff) {
        coefficients.put(variable, coeff);
    }

    public void activate(MPSolver solver) {
        // TODO think about threading, idempotency
        this.mpConstraint = solver.makeConstraint(lowerBound, upperBound, name);
        coefficients.forEach((var, coeff) -> mpConstraint.setCoefficient(var.mpVariable, coeff));
        this.status = Status.ACTIVE;
    }

    public void deactivate() {
        this.mpConstraint.delete();
        this.status = Status.INACTIVE;
    }
}
