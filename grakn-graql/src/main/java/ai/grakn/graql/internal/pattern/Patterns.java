/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.graql.internal.pattern;

import ai.grakn.graql.Var;
import ai.grakn.graql.VarPattern;
import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.Disjunction;
import ai.grakn.graql.admin.PatternAdmin;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.admin.VarProperty;

import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Factory for instances of {@link ai.grakn.graql.Pattern}.
 *
 * Also includes helper methods to operate on a {@link ai.grakn.graql.Pattern} or {@link VarPattern}.
 *
 * @author Felix Chapman
 */
public class Patterns {

    private static final AtomicLong counter = new AtomicLong(System.currentTimeMillis() * 1000);

    public static final Var RELATION_EDGE = reservedVar("RELATION_EDGE");
    public static final Var RELATION_DIRECTION = reservedVar("RELATION_DIRECTION");

    private Patterns() {}

    public static <T extends PatternAdmin> Conjunction<T> conjunction(Set<T> patterns) {
        return new AutoValue_ConjunctionImpl<>(patterns);
    }

    public static <T extends PatternAdmin> Disjunction<T> disjunction(Set<T> patterns) {
        return new AutoValue_DisjunctionImpl<>(patterns);
    }

    public static Var var() {
        return VarImpl.of(Long.toString(counter.getAndIncrement()), Var.Kind.Generated);
    }

    public static Var var(String value) {
        return VarImpl.of(value, Var.Kind.UserDefined);
    }

    public static VarPatternAdmin varPattern(Var name, Set<VarProperty> properties) {
        if (properties.isEmpty()) {
            return name.admin();
        } else {
            return new AutoValue_VarPatternImpl(name, properties);
        }
    }

    private static Var reservedVar(String value) {
        return VarImpl.of(value, Var.Kind.Reserved);
    }
}
