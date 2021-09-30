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

public abstract class OptimiserVariable<T> {

    private enum State {INACTIVE, ACTIVE}

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

    abstract void activate(MPSolver solver);

    abstract void deactivate();

    public static class Boolean extends OptimiserVariable<java.lang.Boolean> {

        private State state;
        private java.lang.Boolean initial;
        private java.lang.Boolean solution;
        private MPVariable mpVariable;

        public Boolean(String name) {
            super(name);
            this.state = State.INACTIVE;
        }

        @Override
        public java.lang.Boolean solutionValue() {
            assert solution != null;
            return solution;
        }

        @Override
        MPVariable mpVariable() {
            assert state == State.ACTIVE;
            return mpVariable;
        }

        @Override
        void recordValue() {
            assert state == State.ACTIVE;
            if (mpVariable.solutionValue() == 0.0) solution = false;
            else if (mpVariable.solutionValue() == 1.0) solution = true;
            else throw TypeDBException.of(UNEXPECTED_OPTIMISER_VALUE);
        }

        @Override
        synchronized void activate(MPSolver mpSolver) {
            assert state == State.INACTIVE;
            this.mpVariable = mpSolver.makeBoolVar(name);
            this.state = State.ACTIVE;
        }

        @Override
        synchronized void deactivate() {
            assert state == State.ACTIVE;
            this.mpVariable.delete();
            this.state = State.INACTIVE;
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

        @Override
        public String toString() {
            return name + "[Bool][status=" + state + "]";
        }
    }

    public static class Integer extends OptimiserVariable<java.lang.Integer> {

        private final double lowerBound;
        private final double upperBound;
        private State state;
        private java.lang.Integer initial;
        private java.lang.Integer solution;
        private MPVariable mpVariable;

        public Integer(double lowerBound, double upperBound, String name) {
            super(name);
            this.lowerBound = lowerBound;
            this.upperBound = upperBound;
            this.state = State.INACTIVE;
        }

        @Override
        public java.lang.Integer solutionValue() {
            assert solution != null;
            return solution;
        }

        @Override
        MPVariable mpVariable() {
            assert state == State.ACTIVE;
            return mpVariable;
        }

        @Override
        void recordValue() {
            assert state == State.ACTIVE;
            solution = (int) Math.round(mpVariable.solutionValue());
        }

        @Override
        synchronized void activate(MPSolver mpSolver) {
            assert state == State.INACTIVE;
            this.mpVariable = mpSolver.makeIntVar(lowerBound, upperBound, name);
            this.state = State.ACTIVE;
        }

        @Override
        synchronized void deactivate() {
            assert state == State.ACTIVE;
            this.mpVariable.delete();
            this.state = State.INACTIVE;
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
            return name + "[Int][status=" + state + "]";
        }
    }
}
