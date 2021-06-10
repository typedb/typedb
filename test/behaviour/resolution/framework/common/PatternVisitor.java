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

package com.vaticle.typedb.core.test.behaviour.resolution.framework.common;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.pattern.Disjunction;
import com.vaticle.typedb.core.pattern.Negation;
import com.vaticle.typedb.core.pattern.variable.Variable;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public abstract class PatternVisitor {

    private Disjunction visitDisjunction(Disjunction disjunction) {
        return new Disjunction(iterate(disjunction.conjunctions()).map(this::visitConjunction).toList());
    }

    public Conjunction visitConjunction(Conjunction conjunction) {
        return new Conjunction(visitVariables(conjunction.variables()), visitNegations(conjunction.negations()));
    }

    protected Set<Negation> visitNegations(Set<Negation> negations) {
        return iterate(negations).map(this::visitNegation).toSet();
    }

    Negation visitNegation(Negation negation) {
        return new Negation(visitDisjunction(negation.disjunction()));
    }

    protected Set<Variable> visitVariables(Set<Variable> variables) {
        return iterate(variables).map(this::visitVariable).toSet();
    }

    protected Variable visitVariable(Variable pattern) {
        return pattern;
    }

    public static class VariableVisitor extends PatternVisitor {

        private final Function<Variable, Variable> function;

        public VariableVisitor(Function<Variable, Variable> function) {
            this.function = function;
        }

        @Override
        protected Variable visitVariable(Variable variable) {
            return function.apply(variable);
        }
    }

    public static class NegationRemovalVisitor extends PatternVisitor {

        @Override
        protected Set<Negation> visitNegations(Set<Negation> negations) {
            return new HashSet<>();
        }

        @Override
        Negation visitNegation(Negation negation) {
            throw TypeDBException.of(ILLEGAL_STATE);
        }
    }
}
