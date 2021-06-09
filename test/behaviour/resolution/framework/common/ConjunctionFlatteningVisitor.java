/*
 * Copyright (C) 2020 Grakn Labs
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

package grakn.core.test.behaviour.resolution.framework.common;

import graql.lang.pattern.Conjunction;
import graql.lang.pattern.Pattern;

import java.util.HashSet;
import java.util.Set;

import static com.google.common.collect.Iterables.getOnlyElement;

public class ConjunctionFlatteningVisitor extends PatternVisitor {
    Pattern visitConjunction(Conjunction<? extends Pattern> pattern) {
        Set<? extends Pattern> patterns = pattern.getPatterns();
        HashSet<Pattern> newPatterns = new HashSet<>();
        patterns.forEach(p -> {
            Pattern childPattern = visitPattern(p);
            if (childPattern instanceof Conjunction) {
                newPatterns.addAll(((Conjunction<? extends Pattern>) childPattern).getPatterns());
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
