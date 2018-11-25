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
import grakn.core.graql.admin.ReasonerQuery;
import grakn.core.graql.internal.reasoner.query.ReasonerQueries;
import grakn.core.graql.query.Graql;
import grakn.core.server.Transaction;
import grakn.core.server.session.TransactionImpl;

import javax.annotation.CheckReturnValue;
import java.util.List;
import java.util.Set;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

/**
 * A class representing a conjunction (and) of patterns. All inner patterns must match in a query
 *
 * @param <T> the type of patterns in this conjunction
 */
public class Conjunction<T extends Pattern> implements Pattern {

    private final Set<T> patterns;

    public Conjunction(Set<T> patterns) {
        if (patterns == null) {
            throw new NullPointerException("Null patterns");
        }
        this.patterns = patterns;
    }

    /**
     * @return the patterns within this conjunction
     */
    @CheckReturnValue
    public Set<T> getPatterns() {
        return patterns;
    }

    @Override
    public Disjunction<Conjunction<Statement>> getDisjunctiveNormalForm() {
        // Get all disjunctions in query
        List<Set<Conjunction<Statement>>> disjunctionsOfConjunctions = getPatterns().stream()
                .map(p -> p.getDisjunctiveNormalForm().getPatterns())
                .collect(toList());

        // Get the cartesian product.
        // in other words, this puts the 'ands' on the inside and the 'ors' on the outside
        // e.g. (A or B) and (C or D)  <=>  (A and C) or (A and D) or (B and C) or (B and D)
        Set<Conjunction<Statement>> dnf = Sets.cartesianProduct(disjunctionsOfConjunctions).stream()
                .map(Conjunction::fromConjunctions)
                .collect(toSet());

        return Graql.or(dnf);

        // Wasn't that a horrible function? Here it is in Haskell:
        //     dnf = map fromConjunctions . sequence . map getDisjunctiveNormalForm . patterns
    }

    @Override
    public Set<Variable> commonVars() {
        return getPatterns().stream().map(Pattern::commonVars).reduce(ImmutableSet.of(), Sets::union);
    }

    @Override
    public boolean isConjunction() {
        return true;
    }

    /**
     * @return the corresponding reasoner query
     */
    @CheckReturnValue
    public ReasonerQuery toReasonerQuery(Transaction tx) {
        Conjunction<Statement> pattern = Iterables.getOnlyElement(getDisjunctiveNormalForm().getPatterns());
        // TODO: This cast is unsafe - this method should accept an `TransactionImpl`
        return ReasonerQueries.create(pattern, (TransactionImpl<?>) tx);
    }

    private static <U extends Pattern> Conjunction<U> fromConjunctions(List<Conjunction<U>> conjunctions) {
        Set<U> patterns = conjunctions.stream().flatMap(p -> p.getPatterns().stream()).collect(toSet());
        return Graql.and(patterns);
    }

    @Override
    public String toString() {
        return "{" + getPatterns().stream().map(s -> s + ";").collect(joining(" ")) + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o instanceof Conjunction) {
            Conjunction<?> that = (Conjunction<?>) o;
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
