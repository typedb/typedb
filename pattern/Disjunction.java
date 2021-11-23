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
 *
 */

package com.vaticle.typedb.core.pattern;

import com.vaticle.factory.tracing.client.FactoryTracingThreadStatic.ThreadTrace;
import com.vaticle.typedb.core.pattern.variable.VariableRegistry;
import com.vaticle.typeql.lang.pattern.Conjunctable;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

import static com.vaticle.factory.tracing.client.FactoryTracingThreadStatic.traceOnThread;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typeql.lang.common.TypeQLToken.Char.CURLY_CLOSE;
import static com.vaticle.typeql.lang.common.TypeQLToken.Char.CURLY_OPEN;
import static com.vaticle.typeql.lang.common.TypeQLToken.Char.NEW_LINE;
import static com.vaticle.typeql.lang.common.TypeQLToken.Operator.OR;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

public class Disjunction implements Pattern, Cloneable {

    private static final String TRACE_PREFIX = "disjunction.";
    private final List<Conjunction> conjunctions;
    private final int hash;

    public Disjunction(List<Conjunction> conjunctions) {
        this.conjunctions = conjunctions;
        this.hash = Objects.hash(conjunctions);
    }

    public static Disjunction create(
            com.vaticle.typeql.lang.pattern.Disjunction<com.vaticle.typeql.lang.pattern.Conjunction<Conjunctable>> typeql) {
        return create(typeql, null);
    }

    public static Disjunction create(
            com.vaticle.typeql.lang.pattern.Disjunction<com.vaticle.typeql.lang.pattern.Conjunction<Conjunctable>> typeql,
            @Nullable VariableRegistry bounds) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "create")) {
            return new Disjunction(typeql.patterns().stream().map(
                    conjunction -> Conjunction.create(conjunction, bounds)
            ).collect(toList()));
        }
    }

    public List<Conjunction> conjunctions() {
        return conjunctions;
    }

    public boolean isCoherent() {
        return iterate(conjunctions).allMatch(Conjunction::isCoherent);
    }

    @Override
    public Disjunction clone() {
        return new Disjunction(iterate(conjunctions).map(Conjunction::clone).toList());
    }

    @Override
    public String toString() {
        return conjunctions.stream().map(Conjunction::toString)
                .collect(joining("" + CURLY_CLOSE + NEW_LINE + OR + NEW_LINE + CURLY_OPEN,
                                 "" + CURLY_OPEN, "" + CURLY_CLOSE));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Disjunction that = (Disjunction) o;
        // TODO this isn't right because it is positional-equality too!
        return this.conjunctions.equals(that.conjunctions);
    }

    @Override
    public int hashCode() {
        return hash;
    }
}
