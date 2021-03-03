/*
 * Copyright (C) 2021 Grakn Labs
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

package grakn.core.pattern;

import grabl.tracing.client.GrablTracingThreadStatic.ThreadTrace;
import grakn.core.common.exception.GraknException;
import grakn.core.pattern.variable.Variable;
import grakn.core.pattern.variable.VariableRegistry;
import graql.lang.pattern.variable.Reference;

import static grabl.tracing.client.GrablTracingThreadStatic.traceOnThread;
import static grakn.core.common.exception.ErrorMessage.Pattern.UNBOUNDED_NEGATION;
import static grakn.core.common.iterator.Iterators.iterate;
import static graql.lang.common.GraqlToken.Char.SPACE;
import static graql.lang.common.GraqlToken.Operator.NOT;

public class Negation implements Pattern, Cloneable {

    private static final String TRACE_PREFIX = "negation.";
    private final Disjunction disjunction;

    public Negation(Disjunction disjunction) {
        this.disjunction = disjunction;
    }

    public static Negation create(graql.lang.pattern.Negation<?> graql, VariableRegistry bounds) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "create")) {
            Disjunction disjunction = Disjunction.create(graql.normalise().pattern(), bounds);
            disjunction.conjunctions().forEach(conjunction -> {
                if (iterate(conjunction.variables()).map(Variable::reference)
                        .filter(Reference::isName).noneMatch(bounds::contains)) {
                    throw GraknException.of(UNBOUNDED_NEGATION);
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
}
