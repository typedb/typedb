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

package grakn.core.graql.query.pattern;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.CheckReturnValue;

import static java.util.stream.Collectors.joining;

public class Negation<T extends Pattern> implements Pattern {

    private final Set<T> patterns;

    public Negation(Set<T> patterns) {
        if (patterns == null) {
            throw new NullPointerException("Null patterns");
        }
        this.patterns = patterns.stream().map(Objects::requireNonNull).collect(Collectors.toSet());
    }

    @CheckReturnValue
    public Set<T> getPatterns(){ return patterns;}

    @Override
    public Disjunction<Conjunction<Statement>> getDisjunctiveNormalForm() {
        return Iterables.getOnlyElement(getPatterns()).negate().getDisjunctiveNormalForm();
    }

    @Override
    public Pattern negate() {
        return Iterables.getOnlyElement(getPatterns());
    }

    @Override
    public Set<Variable> variables() {
        return getPatterns().stream().map(Pattern::variables).reduce(ImmutableSet.of(), Sets::union);
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

}

