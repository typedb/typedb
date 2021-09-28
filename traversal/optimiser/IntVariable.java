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

import com.google.ortools.linearsolver.MPSolver;
import com.google.ortools.linearsolver.MPVariable;

public class IntVariable {

    private final double lowerBound;
    private final double upperBound;
    private final String name;
    private Double hint;
    private Status status;
    MPVariable mpVariable;

    private enum Status {INACTIVE, ACTIVE;}

    public IntVariable(double lowerBound, double upperBound, String name) {
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
        this.name = name;
        this.status = Status.INACTIVE;
    }

    public int solutionValue() {
        assert status == Status.ACTIVE;
        return (int) Math.round(mpVariable.solutionValue());
    }

    public void activate(MPSolver mpSolver) {
        assert status == Status.INACTIVE;
        // TODO think about threading, idempotency
        this.mpVariable = mpSolver.makeIntVar(lowerBound, upperBound, name);
        this.status = Status.ACTIVE;
    }

    public void deactivate() {
        this.mpVariable.delete();
        this.status = Status.INACTIVE;
    }

    public boolean hasHint() {
        return hint != null;
    }

    public void setHint(double hint) {
        this.hint = hint;
    }

    public void clearHint() {
        hint = null;
    }

    public double getHint() {
        assert hasHint();
        return hint;
    }

    @Override
    public String toString() {
        return name;
    }
}
