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
import com.vaticle.typeql.lang.pattern.Conjunctable;
import com.vaticle.typeql.lang.pattern.Conjunction;
import com.vaticle.typeql.lang.pattern.Disjunction;
import com.vaticle.typeql.lang.pattern.Negation;
import com.vaticle.typeql.lang.pattern.Pattern;
import com.vaticle.typeql.lang.pattern.variable.BoundVariable;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public abstract class PatternVisitor {

    public Disjunction<Conjunction<Conjunctable>> visitDisjunction(Disjunction<Conjunction<Conjunctable>> disjunction) {
        return new Disjunction<>(iterate(disjunction.patterns()).map(this::visitConjunction).toList());
    }

    public Conjunction<Conjunctable> visitConjunction(Conjunction<Conjunctable> conjunction) {
        List<Conjunctable> conjunctables = new ArrayList<>();
        conjunction.patterns().forEach(pattern -> {
            if (pattern.isVariable()) {
                conjunctables.add(visitVariable(pattern.asVariable()));
            } else if (pattern.isNegation()) {
                conjunctables.add(visitNegation(pattern.asNegation()));
            } else throw TypeDBException.of(ILLEGAL_STATE);
        });
        return new Conjunction<>(conjunctables);
    }

    Negation<Disjunction<Conjunction<Conjunctable>>> visitNegation(Negation<? extends Pattern> negation) {
        return new Negation<>(visitDisjunction(negation.normalise().pattern()));
    }

    protected BoundVariable visitVariable(BoundVariable variable) {
        return variable;
    }

    public static class VariableVisitor extends PatternVisitor {

        private final Function<BoundVariable, BoundVariable> function;

        public VariableVisitor(Function<BoundVariable, BoundVariable> function) {
            this.function = function;
        }

        @Override
        public BoundVariable visitVariable(BoundVariable variable) {
            return function.apply(variable);
        }
    }

    public static class NegationRemovalVisitor extends PatternVisitor {

        // TODO: Would like to return explicitly BoundVariables and not Negations
        @Override
        public Conjunction<BoundVariable> visitConjunction(Conjunction<Conjunctable> conjunction) {
            List<BoundVariable> variables = new ArrayList<>();
            conjunction.patterns().forEach(pattern -> {
                if (pattern.isVariable()) {
                    variables.add(visitVariable(pattern.asVariable()));
                } else {
                    if (!pattern.isNegation()) throw TypeDBException.of(ILLEGAL_STATE);
                }
            });
            return new Conjunction<BoundVariable>(variables);
        }
    }
}
