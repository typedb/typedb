/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.graql.internal.pattern;

import ai.grakn.graql.Var;
import ai.grakn.graql.VarPattern;
import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.Disjunction;
import ai.grakn.graql.admin.PatternAdmin;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.admin.VarProperty;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.Set;
import java.util.UUID;

/**
 * Factory for instances of {@link ai.grakn.graql.Pattern}.
 *
 * Also includes helper methods to operate on a {@link ai.grakn.graql.Pattern} or {@link VarPattern}.
 *
 * @author Felix Chapman
 */
public class Patterns {

    private Patterns() {}

    public static <T extends PatternAdmin> Conjunction<T> conjunction(Set<T> patterns) {
        return new AutoValue_ConjunctionImpl<>(patterns);
    }

    public static <T extends PatternAdmin> Disjunction<T> disjunction(Set<T> patterns) {
        return new AutoValue_DisjunctionImpl<>(patterns);
    }

    public static VarPatternAdmin mergeVars(Collection<VarPatternAdmin> vars) {
        VarPatternAdmin first = vars.iterator().next();
        Var name = first.getVarName();
        ImmutableSet.Builder<VarProperty> properties = ImmutableSet.builder();

        for (VarPatternAdmin var : vars) {
            if (var.getVarName().isUserDefinedName()) {
                name = var.getVarName();
            }

            properties.addAll(var.getProperties().iterator());
        }

        return varPattern(name, properties.build());
    }

    public static Var var() {
        return new AutoValue_VarImpl(UUID.randomUUID().toString(), false);
    }

    public static Var var(String value) {
        return new AutoValue_VarImpl(value, true);
    }

    public static VarPatternAdmin varPattern(Var name, Set<VarProperty> properties) {
        if (properties.isEmpty()) {
            return name.admin();
        } else {
            return new AutoValue_VarPatternImpl(name, properties);
        }
    }
}
