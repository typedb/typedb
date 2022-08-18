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

package com.vaticle.typedb.core.logic.resolvable;

import com.vaticle.typedb.core.pattern.Disjunction;


import java.util.Set;

import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public class ResolvableDisjunction {
    private final Disjunction disjunctionPattern;
    private final Set<ResolvableConjunction> resolvableConjunctions;

    private ResolvableDisjunction(Disjunction disjunctionPattern, Set<ResolvableConjunction> conjunctions) {
        this.disjunctionPattern = disjunctionPattern;
        this.resolvableConjunctions = conjunctions;
    }

    public static ResolvableDisjunction of(Disjunction disjunctionPattern) {
        Set<ResolvableConjunction> resolvableConjunctions = iterate(disjunctionPattern.conjunctions())
                .map(c -> ResolvableConjunction.of(c)).toSet();
        return new ResolvableDisjunction(disjunctionPattern, resolvableConjunctions);
    }

    public Disjunction pattern() {
        return disjunctionPattern;
    }

    public Set<ResolvableConjunction> conjunctions() {
        return resolvableConjunctions;
    }
}
