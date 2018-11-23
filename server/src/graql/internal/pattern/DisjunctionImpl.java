/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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

package grakn.core.graql.internal.pattern;

import grakn.core.graql.query.Var;
import grakn.core.graql.admin.Conjunction;
import grakn.core.graql.admin.Disjunction;
import grakn.core.graql.admin.PatternAdmin;
import grakn.core.graql.admin.VarPatternAdmin;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.util.Set;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toSet;

@AutoValue
abstract class DisjunctionImpl<T extends PatternAdmin> extends AbstractPattern implements Disjunction<T> {

    public static <T extends PatternAdmin> Disjunction<T> of(Set<T> patterns) {
        return new AutoValue_DisjunctionImpl<>(patterns);
    }

    @Override
    public abstract Set<T> getPatterns();

    @Override
    public Disjunction<Conjunction<VarPatternAdmin>> getDisjunctiveNormalForm() {
        // Concatenate all disjunctions into one big disjunction
        Set<Conjunction<VarPatternAdmin>> dnf = getPatterns().stream()
                .flatMap(p -> p.getDisjunctiveNormalForm().getPatterns().stream())
                .collect(toSet());

        return Patterns.disjunction(dnf);
    }

    @Override
    public Set<Var> commonVars() {
        return getPatterns().stream().map(PatternAdmin::commonVars).reduce(Sets::intersection).orElse(ImmutableSet.of());
    }

    @Override
    public boolean isDisjunction() {
        return true;
    }

    @Override
    public Disjunction<?> asDisjunction() {
        return this;
    }

    @Override
    public String toString() {
        return getPatterns().stream().map(Object::toString).collect(Collectors.joining(" or "));
    }

    @Override
    public PatternAdmin admin() {
        return this;
    }
}
