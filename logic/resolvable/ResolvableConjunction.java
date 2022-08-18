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

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.pattern.Conjunction;

import java.util.Set;

import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.common.iterator.Iterators.link;

public class ResolvableConjunction {
    private final Conjunction conjunctionPattern;
    private final Set<Negated> negations;
    private final Set<Concludable> concludables;

    private ResolvableConjunction(Conjunction conjunction, Set<Concludable> concludables, Set<Negated> negations) {
        this.conjunctionPattern = conjunction;
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
}
