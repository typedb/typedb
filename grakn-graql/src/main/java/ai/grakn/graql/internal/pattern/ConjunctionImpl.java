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

import ai.grakn.graql.VarName;
import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.Disjunction;
import ai.grakn.graql.admin.PatternAdmin;
import ai.grakn.graql.admin.VarAdmin;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Set;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

class ConjunctionImpl<T extends PatternAdmin> implements Conjunction<T> {

    private final Set<T> patterns;

    ConjunctionImpl(Set<T> patterns) {
        this.patterns = patterns;
    }

    @Override
    public Set<T> getPatterns() {
        return patterns;
    }

    @Override
    public Disjunction<Conjunction<VarAdmin>> getDisjunctiveNormalForm() {
        // Get all disjunctions in query
        List<Set<Conjunction<VarAdmin>>> disjunctionsOfConjunctions = patterns.stream()
                .map(p -> p.getDisjunctiveNormalForm().getPatterns())
                .collect(toList());

        // Get the cartesian product.
        // in other words, this puts the 'ands' on the inside and the 'ors' on the outside
        // e.g. (A or B) and (C or D)  <=>  (A and C) or (A and D) or (B and C) or (B and D)
        Set<Conjunction<VarAdmin>> dnf = Sets.cartesianProduct(disjunctionsOfConjunctions).stream()
                .map(ConjunctionImpl::fromConjunctions)
                .collect(toSet());

        return Patterns.disjunction(dnf);

        // Wasn't that a horrible function? Here it is in Haskell:
        //     dnf = map fromConjunctions . sequence . map getDisjunctiveNormalForm . patterns
    }

    @Override
    public Set<VarName> commonVarNames() {
        return patterns.stream().map(PatternAdmin::commonVarNames).reduce(ImmutableSet.of(), Sets::union);
    }

    @Override
    public boolean isConjunction() {
        return true;
    }

    @Override
    public Conjunction<?> asConjunction() {
        return this;
    }

    private static <U extends PatternAdmin> Conjunction<U> fromConjunctions(List<Conjunction<U>> conjunctions) {
        Set<U> patterns = conjunctions.stream().flatMap(p -> p.getPatterns().stream()).collect(toSet());
        return Patterns.conjunction(patterns);
    }

    @Override
    public boolean equals(Object obj) {
        return (obj instanceof ConjunctionImpl) && patterns.equals(((ConjunctionImpl) obj).patterns);
    }

    @Override
    public int hashCode() {
        return patterns.hashCode();
    }

    @Override
    public String toString() {
        return "{" + patterns.stream().map(s -> s + ";").collect(joining(" ")) + "}";
    }

    @Override
    public PatternAdmin admin() {
        return this;
    }
}
