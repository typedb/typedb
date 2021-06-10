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

import com.vaticle.typeql.lang.pattern.Conjunction;
import com.vaticle.typeql.lang.pattern.Disjunction;
import com.vaticle.typeql.lang.pattern.Negation;
import com.vaticle.typeql.lang.pattern.Pattern;
import com.vaticle.typeql.lang.pattern.variable.BoundVariable;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.collect.Iterables.getOnlyElement;

public abstract class PatternVisitor {
    public Pattern visitPattern(Pattern pattern) {
        if (pattern.isVariable()) {
            return visitVariable(pattern.asVariable());
        } else if (pattern.isConjunction()) {
            return visitConjunction((Conjunction<? extends Pattern>) pattern);
        } else if (pattern.isNegation()) {
            return visitNegation((Negation<? extends Pattern>) pattern);
        } else if (pattern.isDisjunction()) {
            return visitDisjunction((Disjunction<? extends Pattern>) pattern);
        }
        throw new UnsupportedOperationException();
    }

    Pattern visitVariable(BoundVariable pattern) {
        return pattern;
    }

    Pattern visitConjunction(Conjunction<? extends Pattern> pattern) {
        List<? extends Pattern> patterns = pattern.patterns();
        Set<Pattern> newPatterns = new HashSet<>();
        patterns.forEach(p -> {
            Pattern childPattern = visitPattern(p);
            if (childPattern != null) {
                newPatterns.add(childPattern);
            }
        });
        return new Conjunction<>(newPatterns);
    }

    Pattern visitNegation(Negation<? extends Pattern> pattern) {
        return new Negation<>(visitPattern(pattern.asNegation().pattern()));
    }

    private Pattern visitDisjunction(Disjunction<? extends Pattern> pattern) {
        List<? extends Pattern> patterns = pattern.patterns();
        Set<Pattern> newPatterns = new HashSet<>();
        patterns.forEach(p -> {
            Pattern childPattern = visitPattern(p);
            if (childPattern != null) {
                newPatterns.add(childPattern);
            }
        });
        return new Disjunction<>(newPatterns);
    }

    public static class VariableVisitor extends PatternVisitor {

        private final Function<BoundVariable, Pattern> function;

        public VariableVisitor(Function<BoundVariable, Pattern> function) {
            this.function = function;
        }

        @Override
        Pattern visitVariable(BoundVariable pattern) {
            return function.apply(pattern);
        }
    }

    public static class ConjunctionFlatteningVisitor extends PatternVisitor {
        @Override
        Pattern visitConjunction(Conjunction<? extends Pattern> pattern) {
            List<? extends Pattern> patterns = pattern.patterns();
            Set<Pattern> newPatterns = new HashSet<>();
            patterns.forEach(p -> {
                Pattern childPattern = visitPattern(p);
                if (childPattern.isConjunction()) {
                    newPatterns.addAll(((Conjunction<? extends Pattern>) childPattern).patterns());
                } else if (childPattern != null) {
                    newPatterns.add(visitPattern(childPattern));
                }
            });
            if (newPatterns.size() == 0) {
                return null;
            } else if (newPatterns.size() == 1) {
                return getOnlyElement(newPatterns);
            }
            return new Conjunction<>(newPatterns);
        }
    }

    public static class NegationRemovalVisitor extends PatternVisitor {
        @Override
        Pattern visitNegation(Negation<? extends Pattern> pattern) {
            return null;
        }
    }
}
