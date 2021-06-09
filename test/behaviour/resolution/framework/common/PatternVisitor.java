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
}
