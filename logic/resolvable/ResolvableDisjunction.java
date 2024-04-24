/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
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
