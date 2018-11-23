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

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import grakn.core.graql.admin.Conjunction;
import grakn.core.graql.admin.Disjunction;
import grakn.core.graql.admin.Negation;
import grakn.core.graql.admin.PatternAdmin;
import grakn.core.graql.admin.VarPatternAdmin;
import grakn.core.graql.query.Var;
import java.util.Set;

import static java.util.stream.Collectors.joining;

@AutoValue
abstract class NegationImpl<T extends PatternAdmin> extends AbstractPattern implements Negation<T> {

    @Override
    public abstract Set<T> getPatterns();

    @Override
    public Disjunction<Conjunction<VarPatternAdmin>> getDisjunctiveNormalForm() {
        return this.negate().getDisjunctiveNormalForm();
    }

    @Override
    public PatternAdmin negate() {
        return Iterables.getOnlyElement(getPatterns()).negate();
    }

    @Override
    public Set<Var> commonVars() {
        return getPatterns().stream().map(PatternAdmin::commonVars).reduce(ImmutableSet.of(), Sets::union);
    }

    @Override
    public boolean isNegation() { return true; }

    @Override
    public Negation<?> asNegation() {
        return this;
    }


    @Override
    public String toString() {
        return "NOT {" + getPatterns().stream().map(s -> s + ";").collect(joining(" ")) + "}";
    }

    @Override
    public PatternAdmin admin() {
        return this;
    }
}
