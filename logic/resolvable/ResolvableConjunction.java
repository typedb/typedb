/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
package com.vaticle.typedb.core.logic.resolvable;

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.pattern.Conjunction;

import java.util.Set;

import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.common.iterator.Iterators.link;

public class ResolvableConjunction {
    private final Conjunction conjunctionPattern;
    private final Set<Negated> negations;
    private final Set<Concludable> concludables;

    private ResolvableConjunction(Conjunction conjunctionPattern, Set<Concludable> concludables, Set<Negated> negations) {
        this.conjunctionPattern = conjunctionPattern;
        this.negations = negations;
        this.concludables = concludables;
    }

    public static ResolvableConjunction of(Conjunction conjunction) {
        Set<Concludable> concludables = Concludable.create(conjunction);
        Set<Negated> negations = iterate(conjunction.negations())
                .map(negation -> new Negated(negation))
                .toSet();

        return new ResolvableConjunction(conjunction, concludables, negations);
    }

    public Conjunction pattern() {
        return conjunctionPattern;
    }

    public Set<Negated> negations() {
        return negations;
    }

    public Set<Concludable> positiveConcludables() {
        return concludables;
    }

    public FunctionalIterator<Concludable> allConcludables() {
        return link(iterate(concludables),
                iterate(negations()).flatMap(negated -> iterate(negated.disjunction().conjunctions()))
                        .flatMap(conj -> conj.allConcludables()));
    }

    @Override
    public String toString() {
        return conjunctionPattern.toString();
    }
}
