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

package com.vaticle.typedb.core.pattern;

import com.vaticle.factory.tracing.client.FactoryTracingThreadStatic.ThreadTrace;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.pattern.variable.Variable;
import com.vaticle.typedb.core.pattern.variable.VariableRegistry;
import com.vaticle.typeql.lang.pattern.variable.Reference;

import java.util.Objects;

import static com.vaticle.factory.tracing.client.FactoryTracingThreadStatic.traceOnThread;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Pattern.UNBOUNDED_NEGATION;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typeql.lang.common.TypeQLToken.Char.SPACE;
import static com.vaticle.typeql.lang.common.TypeQLToken.Operator.NOT;

public class Negation implements Pattern, Cloneable {

    private static final String TRACE_PREFIX = "negation.";
    private final Disjunction disjunction;

    public Negation(Disjunction disjunction) {
        this.disjunction = disjunction;
    }

    public static Negation create(com.vaticle.typeql.lang.pattern.Negation<?> typeql, VariableRegistry bounds) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "create")) {
            Disjunction disjunction = Disjunction.create(typeql.normalise().pattern(), bounds);
            disjunction.conjunctions().forEach(conjunction -> {
                if (iterate(conjunction.variables()).map(Variable::reference)
                        .filter(Reference::isName).noneMatch(bounds::contains)) {
                    throw TypeDBException.of(UNBOUNDED_NEGATION);
                }
            });
            return new Negation(disjunction);
        }
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
