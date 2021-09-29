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
    private Double initial;
    private Status status;
    MPVariable mpVariable;
    private Integer solution;

    private enum Status {INACTIVE, ACTIVE;}

    public IntVariable(double lowerBound, double upperBound, String name) {
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
        this.name = name;
        this.status = Status.INACTIVE;
    }

    public void recordValue() {
        assert status == Status.ACTIVE;
        solution = (int) Math.round(mpVariable.solutionValue());
    }

    public int solutionValue() {
        assert solution != null;
        return solution;
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

    public boolean hasInitial() {
        return initial != null;
    }

    public void setInitial(double initial) {
        this.initial = initial;
    }

    public void clearInitial() {
        initial = null;
    }

    public double getInitial() {
        assert hasInitial();
        return initial;
    }

    @Override
    public String toString() {
        return name;
    }
}
