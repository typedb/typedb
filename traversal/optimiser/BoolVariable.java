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
import com.vaticle.typedb.core.common.exception.TypeDBException;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.UNEXPECTED_OPTIMISER_VALUE;

public class BoolVariable extends Variable {

    private Status status;
    private Boolean initial;
    private Boolean solution;
    MPVariable mpVariable;

    public BoolVariable(String name) {
        super(name);
        this.status = Status.INACTIVE;
    }

    @Override
    MPVariable mpVariable() {
        assert status == Status.ACTIVE;
        return mpVariable;
    }

    @Override
    public void recordValue() {
        assert status == Status.ACTIVE;
        if (mpVariable.solutionValue() == 0.0) solution = false;
        else if (mpVariable.solutionValue() == 1.0) solution = true;
        else throw TypeDBException.of(UNEXPECTED_OPTIMISER_VALUE);
    }

    public boolean solutionValue() {
        assert solution != null;
        return solution;
    }

    @Override
    public void activate(MPSolver mpSolver) {
        assert status == Status.INACTIVE;
        // TODO think about threading, idempotency
        this.mpVariable = mpSolver.makeBoolVar(name);
        this.status = Status.ACTIVE;
    }

    @Override
    public void deactivate() {
        this.mpVariable.delete();
        this.status = Status.INACTIVE;
    }

    public void setInitial(boolean initial) {
        this.initial = initial;
    }

    @Override
    public boolean hasInitial() {
        return initial != null;
    }

    @Override
    public void clearInitial() {
        initial = null;
    }

    @Override
    public double getInitial() {
        assert hasInitial();
        if (initial) return 1.0;
        else return 0.0;
    }

}
