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

import com.google.ortools.linearsolver.MPSolver;
import com.google.ortools.linearsolver.MPVariable;
import com.vaticle.typedb.core.common.exception.TypeDBException;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.UNEXPECTED_OPTIMISER_VALUE;

public abstract class OptimiserVariable<T> {

    final String name;
    MPVariable mpVariable;
    T value;

    OptimiserVariable(String name) {
        this.name = name;
    }

    public T value() {
        return value;
    }

    public boolean hasValue() {
        return value != null;
    }

    public void setValue(T value) {
        this.value = value;
    }

    public abstract double valueAsDouble();

    MPVariable mpVariable() {
        return mpVariable;
    }

    abstract void recordSolutionValue();

    abstract void initialise(MPSolver solver);

    public abstract boolean isSatisfied();

    synchronized void release() {
        this.mpVariable.delete();
    }

    @Override
    public String toString() {
        return name + "[" + getClass().getSimpleName() + "]";
    }

    public static class Boolean extends OptimiserVariable<java.lang.Boolean> {

        public Boolean(String name) {
            super(name);
        }

        @Override
        void recordSolutionValue() {
            if (mpVariable.solutionValue() == 0.0) value = false;
            else if (mpVariable.solutionValue() == 1.0) value = true;
            else throw TypeDBException.of(UNEXPECTED_OPTIMISER_VALUE);
        }

        @Override
        synchronized void initialise(MPSolver mpSolver) {
            this.mpVariable = mpSolver.makeBoolVar(name);
        }

        @Override
        public double valueAsDouble() {
            assert hasValue();
            if (value) return 1.0;
            else return 0.0;
        }

        @Override
        public boolean isSatisfied() {
            return hasValue();
        }
    }

    public static class Integer extends OptimiserVariable<java.lang.Integer> {

        private final double lowerBound;
        private final double upperBound;

        public Integer(double lowerBound, double upperBound, String name) {
            super(name);
            this.lowerBound = lowerBound;
            this.upperBound = upperBound;
        }

        @Override
        void recordSolutionValue() {
            value = (int) Math.round(mpVariable.solutionValue());
        }

        @Override
        synchronized void initialise(MPSolver mpSolver) {
            this.mpVariable = mpSolver.makeIntVar(lowerBound, upperBound, name);
        }

        @Override
        public double valueAsDouble() {
            assert hasValue();
            return value;
        }

        @Override
        public boolean isSatisfied() {
            return hasValue() && lowerBound <= valueAsDouble() && valueAsDouble() <= upperBound;
        }
    }
}
