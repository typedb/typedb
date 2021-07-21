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
 */

package com.vaticle.typedb.core.logic.resolvable;

import com.vaticle.typedb.core.pattern.Disjunction;
import com.vaticle.typedb.core.pattern.Negation;
import com.vaticle.typedb.core.pattern.variable.ThingVariable;
import com.vaticle.typedb.core.traversal.common.Identifier.Variable.Retrievable;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public class Negated extends Resolvable<Disjunction> {

    // note: we always guarantee unique anonymous IDs within one query
    private final Set<Retrievable> identifiers;

    public Negated(Negation negation) {
        super(negation.disjunction());
        this.identifiers = new HashSet<>();
        pattern().conjunctions().forEach(c -> iterate(c.retrieves()).forEachRemaining(identifiers::add));
    }

    @Override
    public Optional<ThingVariable> generating() {
        return Optional.empty();
    }

    @Override
    public Set<Retrievable> retrieves() {
        return this.identifiers;
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
