/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.logic.resolvable;

import com.vaticle.typedb.core.pattern.Disjunction;
import com.vaticle.typedb.core.pattern.Negation;
import com.vaticle.typedb.core.pattern.variable.Variable;
import com.vaticle.typedb.core.traversal.common.Identifier.Variable.Retrievable;

import java.util.HashSet;
import java.util.Set;

import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static java.util.Collections.emptySet;

public class Negated extends Resolvable<Disjunction> {
    // note: we always guarantee unique anonymous IDs within one query
    private final Set<Retrievable> identifiers;
    private final ResolvableDisjunction disjunction;

    public Negated(Negation negation) {
        super(negation.disjunction());
        this.disjunction = ResolvableDisjunction.of(negation.disjunction());
        this.identifiers = new HashSet<>();
        pattern().conjunctions().forEach(c -> iterate(c.retrieves()).forEachRemaining(identifiers::add));
    }

    public ResolvableDisjunction disjunction() {
        return disjunction;
    }

    @Override
    public Set<Variable> generating() {
        return emptySet();
    }

    @Override
    public Set<Retrievable> retrieves() {
        return this.identifiers;
    }

    @Override
    public Set<Variable> variables() {
        return iterate(pattern().conjunctions()).flatMap(conj -> iterate(conj.variables())).toSet();
    }

    @Override
    public boolean isNegated() {
        return true;
    }

    @Override
    public Negated asNegated() {
        return this;
    }
}
