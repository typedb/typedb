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
package grakn.core.graql.internal.reasoner.atom.predicate;

import com.google.common.collect.Sets;
import grakn.core.common.exception.ErrorMessage;
import grakn.core.graql.internal.reasoner.atom.Atomic;
import grakn.core.graql.concept.Rule;
import grakn.core.graql.internal.reasoner.atom.AtomicBase;

import java.util.Set;

/**
 *
 * <p>
 * {@link AtomicBase} extension serving as base class for predicate implementations.
 * </p>
 *
 * @param <T> the type of the predicate on a concept
 *
 *
 */
public abstract class Predicate<T> extends AtomicBase {

    public abstract T getPredicate();
    public abstract String getPredicateValue();

    @Override
    public Set<String> validateAsRuleHead(Rule rule) {
        return Sets.newHashSet(ErrorMessage.VALIDATION_RULE_ILLEGAL_ATOMIC_IN_HEAD.getMessage(rule.then(), rule.label()));
    }
    
    @Override
    public boolean isAlphaEquivalent(Object obj){
        if (obj == null || this.getClass() != obj.getClass()) return false;
        if (obj == this) return true;
        Predicate a2 = (Predicate) obj;
        return this.getPredicateValue().equals(a2.getPredicateValue());
    }

    @Override
    public int alphaEquivalenceHashCode() {
        int hashCode = 1;
        hashCode = hashCode * 37 + this.getPredicateValue().hashCode();
        return hashCode;
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