/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 *
 */

package io.mindmaps.graql.internal.query;

import com.google.common.collect.Sets;
import io.mindmaps.graql.internal.admin.PatternAdmin;
import io.mindmaps.graql.internal.admin.VarAdmin;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

/**
 * A class representing a conjunction (and) of patterns. All inner patterns must match in a query
 */
public class Conjunction<T extends PatternAdmin> implements PatternAdmin {

    private final Set<T> patterns;

    public Conjunction(Set<T> patterns) {
        this.patterns = patterns;
    }

    /**
     * @return the patterns within this conjunction
     */
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
                .map(Conjunction::fromConjunctions)
                .collect(toSet());

        return PatternAdmin.disjunction(dnf);

        // Wasn't that a horrible function? Here it is in Haskell:
        //     dnf = map fromConjunctions . sequence . map getDisjunctiveNormalForm . patterns
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
        return new Conjunction<>(patterns);
    }

    @Override
    public boolean equals(Object obj) {
        return (obj instanceof Conjunction) && patterns.equals(((Conjunction) obj).patterns);
    }

    @Override
    public int hashCode() {
        return patterns.hashCode();
    }

    @Override
    public String toString() {
        return patterns.stream().map(Object::toString).collect(Collectors.joining("; "));
    }

    @Override
    public PatternAdmin admin() {
        return this;
    }
}
