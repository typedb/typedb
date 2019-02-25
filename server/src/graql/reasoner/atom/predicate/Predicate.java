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

package grakn.core.graql.reasoner.atom.predicate;

import com.google.common.collect.Sets;
import grakn.core.common.exception.ErrorMessage;
import grakn.core.concept.type.Rule;
import grakn.core.graql.reasoner.atom.Atomic;
import grakn.core.graql.reasoner.atom.AtomicBase;
import grakn.core.graql.reasoner.query.ReasonerQuery;
import graql.lang.statement.Statement;
import graql.lang.statement.Variable;

import java.util.Set;

/**
 * {@link AtomicBase} extension serving as base class for predicate implementations.
 *
 * @param <T> the type of the predicate on a concept
 */
public abstract class Predicate<T> extends AtomicBase {

    private final Variable varName;
    private final Statement pattern;
    private final ReasonerQuery parentQuery;
    private final T predicate;

    public Predicate(Variable varName, Statement pattern, T predicate, ReasonerQuery parentQuery) {
        this.varName = varName;
        this.pattern = pattern;
        this.parentQuery = parentQuery;
        this.predicate = predicate;
    }

    public Variable getVarName() {
        return varName;
    }
    public Statement getPattern() {
        return pattern;
    }
    public ReasonerQuery getParentQuery() {
        return parentQuery;
    }
    public T getPredicate() { return predicate; }
    public abstract String getPredicateValue();

    @Override
    public Set<String> validateAsRuleHead(Rule rule) {
        return Sets.newHashSet(ErrorMessage.VALIDATION_RULE_ILLEGAL_ATOMIC_IN_HEAD.getMessage(rule.then(), rule.label()));
    }

    @Override
    public boolean isStructurallyEquivalent(Object obj) {
        return isAlphaEquivalent(obj);
    }

    @Override
    public int structuralEquivalenceHashCode() {
        return alphaEquivalenceHashCode();
    }

    @Override
    public boolean subsumes(Atomic atom) { return this.isAlphaEquivalent(atom); }

    @Override
    public final boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o instanceof Predicate) {
            Predicate that = (Predicate) o;
            return (this.getVarName().equals(that.getVarName()))
                    && (this.getPredicate().equals(that.getPredicate()));
        }
        return false;
    }

    @Override
    public final int hashCode() {
        int h = 1;
        h *= 1000003;
        h ^= this.getVarName().hashCode();
        h *= 1000003;
        h ^= this.getPredicate().hashCode();
        return h;
    }
}