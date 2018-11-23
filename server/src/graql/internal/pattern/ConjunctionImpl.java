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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import grakn.core.graql.admin.Conjunction;
import grakn.core.graql.admin.Disjunction;
import grakn.core.graql.admin.PatternAdmin;
import grakn.core.graql.admin.ReasonerQuery;
import grakn.core.graql.admin.VarPatternAdmin;
import grakn.core.graql.internal.reasoner.query.ReasonerQueries;
import grakn.core.graql.query.Var;
import grakn.core.server.Transaction;
import grakn.core.server.session.TransactionImpl;

import java.util.List;
import java.util.Set;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

class ConjunctionImpl<T extends PatternAdmin> extends AbstractPattern implements Conjunction<T> {

    private final Set<T> patterns;

    ConjunctionImpl(
            Set<T> patterns) {
        if (patterns == null) {
            throw new NullPointerException("Null patterns");
        }
        this.patterns = patterns;
    }

    @Override
    public Set<T> getPatterns() {
        return patterns;
    }

    @Override
    public Disjunction<Conjunction<VarPatternAdmin>> getDisjunctiveNormalForm() {
        // Get all disjunctions in query
        List<Set<Conjunction<VarPatternAdmin>>> disjunctionsOfConjunctions = getPatterns().stream()
                .map(p -> p.getDisjunctiveNormalForm().getPatterns())
                .collect(toList());

        // Get the cartesian product.
        // in other words, this puts the 'ands' on the inside and the 'ors' on the outside
        // e.g. (A or B) and (C or D)  <=>  (A and C) or (A and D) or (B and C) or (B and D)
        Set<Conjunction<VarPatternAdmin>> dnf = Sets.cartesianProduct(disjunctionsOfConjunctions).stream()
                .map(ConjunctionImpl::fromConjunctions)
                .collect(toSet());

        return Patterns.disjunction(dnf);

        // Wasn't that a horrible function? Here it is in Haskell:
        //     dnf = map fromConjunctions . sequence . map getDisjunctiveNormalForm . patterns
    }

    @Override
    public Set<Var> commonVars() {
        return getPatterns().stream().map(PatternAdmin::commonVars).reduce(ImmutableSet.of(), Sets::union);
    }

    @Override
    public boolean isConjunction() {
        return true;
    }

    @Override
    public Conjunction<?> asConjunction() {
        return this;
    }

    @Override
    public ReasonerQuery toReasonerQuery(Transaction tx) {
        Conjunction<VarPatternAdmin> pattern = Iterables.getOnlyElement(getDisjunctiveNormalForm().getPatterns());
        // TODO: This cast is unsafe - this method should accept an `TransactionImpl`
        return ReasonerQueries.create(pattern, (TransactionImpl<?>) tx);
    }

    private static <U extends PatternAdmin> Conjunction<U> fromConjunctions(List<Conjunction<U>> conjunctions) {
        Set<U> patterns = conjunctions.stream().flatMap(p -> p.getPatterns().stream()).collect(toSet());
        return Patterns.conjunction(patterns);
    }

    @Override
    public String toString() {
        return "{" + getPatterns().stream().map(s -> s + ";").collect(joining(" ")) + "}";
    }

    @Override
    public PatternAdmin admin() {
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o instanceof ConjunctionImpl) {
            ConjunctionImpl<?> that = (ConjunctionImpl<?>) o;
            return (this.patterns.equals(that.getPatterns()));
        }
        return false;
    }

    @Override
    public int hashCode() {
        int h = 1;
        h *= 1000003;
        h ^= this.patterns.hashCode();
        return h;
    }
}
