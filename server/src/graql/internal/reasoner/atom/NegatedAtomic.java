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

package grakn.core.graql.internal.reasoner.atom;

import com.google.auto.value.AutoValue;
import grakn.core.graql.admin.Atomic;
import grakn.core.graql.admin.ReasonerQuery;
import grakn.core.graql.concept.Rule;
import grakn.core.graql.query.pattern.Pattern;
import grakn.core.graql.query.pattern.Statement;
import grakn.core.graql.query.pattern.Variable;
import java.util.HashSet;
import java.util.Set;

/**
 *
 * Corresponds to a negative literal: ¬atomic;
 *
 */
@AutoValue
public abstract class NegatedAtomic implements Atomic {

    public abstract Atomic inner();

    public static NegatedAtomic create(Atomic inner) { return new AutoValue_NegatedAtomic(inner); }

    @Override
    public String toString(){ return "NOT " + inner().toString();}

    @Override
    public boolean isSelectable(){ return true;}

    @Override
    public Atomic copy(ReasonerQuery parent) { return create(inner().copy(parent)); }

    @Override
    public boolean isPositive() { return false; }

    @Override
    public Atomic negate() {
        return inner();
    }

    @Override
    public Set<String> validateAsRuleHead(Rule rule) {
        //TODO throw exception
        return new HashSet<>();
    }

    @Override
    public boolean subsumes(Atomic atom) {
        //TODO throw exception
        return false;
    }

    @Override
    public Variable getVarName() { return inner().getVarName(); }

    @Override
    public Statement getPattern() { return inner().getPattern(); }

    @Override
    public ReasonerQuery getParentQuery() { return inner().getParentQuery(); }

    @Override
    public void checkValid() { inner().checkValid(); }

    @Override
    public boolean equals(Object obj){
        if (obj == this) return true;
        if (obj == null || this.getClass() != obj.getClass()) return false;
        NegatedAtomic that = (NegatedAtomic) obj;
        return this.inner().equals(that.inner());
    }

    @Override
    public boolean isAlphaEquivalent(Object obj) {
        if (obj == this) return true;
        if (obj == null || this.getClass() != obj.getClass()) return false;
        NegatedAtomic that = (NegatedAtomic) obj;
        return this.inner().isAlphaEquivalent(that.inner());
    }

    @Override
    public boolean isStructurallyEquivalent(Object obj) {
        if (obj == this) return true;
        if (obj == null || this.getClass() != obj.getClass()) return false;
        NegatedAtomic that = (NegatedAtomic) obj;
        return this.inner().isStructurallyEquivalent(that.inner());
    }

    @Override
    public int alphaEquivalenceHashCode() {
        return inner().alphaEquivalenceHashCode();
    }

    @Override
    public int structuralEquivalenceHashCode() {
        return inner().structuralEquivalenceHashCode();
    }

    @Override
    public Pattern getCombinedPattern() { return inner().getCombinedPattern(); }

    @Override
    public Set<Variable> getVarNames() { return inner().getVarNames(); }

    @Override
    public Atomic inferTypes() { return create(inner().inferTypes()); }
}
