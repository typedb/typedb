/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.pattern;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.pattern.variable.Variable;
import com.vaticle.typedb.core.pattern.variable.VariableRegistry;

import java.util.Objects;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Pattern.UNBOUNDED_NEGATION;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typeql.lang.common.TypeQLToken.Char.SPACE;
import static com.vaticle.typeql.lang.common.TypeQLToken.Operator.NOT;

public class Negation implements Pattern, Cloneable {

    private final Disjunction disjunction;

    public Negation(Disjunction disjunction) {
        this.disjunction = disjunction;
    }

    public static Negation create(com.vaticle.typeql.lang.pattern.Negation<?> typeql, VariableRegistry bounds) {
        Disjunction disjunction = Disjunction.create(typeql.normalise().pattern(), bounds);
        disjunction.conjunctions().forEach(conjunction -> {
            if (iterate(conjunction.variables()).map(Variable::reference)
                    .filter(com.vaticle.typeql.lang.common.Reference::isName).noneMatch(bounds::isBound)) {
                throw TypeDBException.of(UNBOUNDED_NEGATION);
            }
        });
        return new Negation(disjunction);
    }

    public Disjunction disjunction() { return disjunction; }

    @Override
    public Negation clone() {
        return new Negation(disjunction.clone());
    }

    @Override
    public String toString() {
        return "" + NOT + SPACE + disjunction.toString();
    }

    public boolean isCoherent() {
        return disjunction.isCoherent();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Negation negation = (Negation) o;
        return disjunction.equals(negation.disjunction);
    }

    @Override
    public int hashCode() {
        return Objects.hash(disjunction);
    }
}
