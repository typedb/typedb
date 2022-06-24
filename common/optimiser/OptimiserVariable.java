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

    OptimiserVariable(String name) {
        this.name = name;
    }

    public abstract T solutionValue();

    abstract MPVariable mpVariable();

    abstract void recordValue();

    public abstract void clearInitial();

    abstract boolean hasInitial();

    public abstract double getInitial();

    abstract void initialise(MPSolver solver);

    abstract void release();

    @Override
    public String toString() {
        return name + "[" + getClass().getSimpleName() + "]";
    }

    public static class Boolean extends OptimiserVariable<java.lang.Boolean> {

        private java.lang.Boolean initial;
        private java.lang.Boolean solution;
        private MPVariable mpVariable;

        public Boolean(String name) {
            super(name);
        }

        @Override
        public java.lang.Boolean solutionValue() {
            assert solution != null;
            return solution;
        }

        @Override
        MPVariable mpVariable() {
            return mpVariable;
        }

        @Override
        void recordValue() {
            if (mpVariable.solutionValue() == 0.0) solution = false;
            else if (mpVariable.solutionValue() == 1.0) solution = true;
            else throw TypeDBException.of(UNEXPECTED_OPTIMISER_VALUE);
        }

        @Override
        synchronized void initialise(MPSolver mpSolver) {
            this.mpVariable = mpSolver.makeBoolVar(name);
        }

        @Override
        synchronized void release() {
            this.mpVariable.delete();
        }

        public void setInitial(boolean initial) {
            this.initial = initial;
        }

        @Override
        boolean hasInitial() {
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

    public static class Integer extends OptimiserVariable<java.lang.Integer> {

        private final double lowerBound;
        private final double upperBound;
        private java.lang.Integer initial;
        private java.lang.Integer solution;
        private MPVariable mpVariable;

        public Integer(double lowerBound, double upperBound, String name) {
            super(name);
            this.lowerBound = lowerBound;
            this.upperBound = upperBound;
        }

        @Override
        public java.lang.Integer solutionValue() {
            assert solution != null;
            return solution;
        }

        @Override
        MPVariable mpVariable() {
            return mpVariable;
        }

        @Override
        void recordValue() {
            solution = (int) Math.round(mpVariable.solutionValue());
        }

        @Override
        synchronized void initialise(MPSolver mpSolver) {
            this.mpVariable = mpSolver.makeIntVar(lowerBound, upperBound, name);
        }

        @Override
        synchronized void release() {
            this.mpVariable.delete();
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
    }
}
