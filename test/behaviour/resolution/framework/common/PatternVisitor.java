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

import java.util.HashSet;
import java.util.Set;

public abstract class PatternVisitor {
    public Pattern visitPattern(Pattern pattern) {
        if (pattern instanceof Statement) {
            return visitStatement((Statement) pattern);
        } else if (pattern instanceof Conjunction) {
            return visitConjunction((Conjunction<? extends Pattern>) pattern);
        } else if (pattern instanceof Negation) {
            return visitNegation((Negation<? extends Pattern>) pattern);
        } else if (pattern instanceof Disjunction) {
            return visitDisjunction((Disjunction<? extends Pattern>) pattern);
        }
        throw new UnsupportedOperationException();
    }

    Pattern visitStatement(Statement pattern) {
        return pattern;
    }

    Pattern visitConjunction(Conjunction<? extends Pattern> pattern) {
        Set<? extends Pattern> patterns = pattern.getPatterns();
        HashSet<Pattern> newPatterns = new HashSet<>();
        patterns.forEach(p -> {
            Pattern childPattern = visitPattern(p);
            if (childPattern != null) {
                newPatterns.add(childPattern);
            }
        });
        return new Conjunction<>(newPatterns);
    }

    Pattern visitNegation(Negation<? extends Pattern> pattern) {
        return new Negation<>(visitPattern(pattern.getPattern()));
    }

    private Pattern visitDisjunction(Disjunction<? extends Pattern> pattern) {
        Set<? extends Pattern> patterns = pattern.getPatterns();
        HashSet<Pattern> newPatterns = new HashSet<>();
        patterns.forEach(p -> {
            Pattern childPattern = visitPattern(p);
            if (childPattern != null) {
                newPatterns.add(childPattern);
            }
        });
        return new Disjunction<>(newPatterns);
    }
}
