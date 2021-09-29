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

public class IntVariable extends Variable {

    private final double lowerBound;
    private final double upperBound;
    private ActivationStatus status;
    private Integer initial;
    private Integer solution;
    private MPVariable mpVariable;

    public IntVariable(double lowerBound, double upperBound, String name) {
        super(name);
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
        this.status = ActivationStatus.INACTIVE;
    }

    @Override
    public Integer solutionValue() {
        assert solution != null;
        return solution;
    }

    @Override
    MPVariable mpVariable() {
        assert status == ActivationStatus.ACTIVE;
        return mpVariable;
    }

    @Override
    public void recordValue() {
        assert status == ActivationStatus.ACTIVE;
        solution = (int) Math.round(mpVariable.solutionValue());
    }

    @Override
    synchronized void activate(MPSolver mpSolver) {
        assert status == ActivationStatus.INACTIVE;
        this.mpVariable = mpSolver.makeIntVar(lowerBound, upperBound, name);
        this.status = ActivationStatus.ACTIVE;
    }

    @Override
    synchronized void deactivate() {
        assert status == ActivationStatus.ACTIVE;
        this.mpVariable.delete();
        this.status = ActivationStatus.INACTIVE;
    }

    @Override
    boolean hasInitial() {
        return initial != null;
    }

    public void setInitial(int initial) {
        this.initial = initial;
    }

    @Override
    public void clearInitial() {
        initial = null;
    }

    @Override
    public double getInitial() {
        assert hasInitial();
        return initial;
    }

    @Override
    public String toString() {
        return name + "[Int][status=" + status + "]";
    }
}
