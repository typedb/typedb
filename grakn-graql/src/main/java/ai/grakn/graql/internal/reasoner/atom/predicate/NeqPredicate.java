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

package ai.grakn.graql.internal.reasoner.atom.predicate;

import ai.grakn.graql.Var;
import ai.grakn.graql.answer.ConceptMap;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.internal.pattern.property.NeqProperty;

import ai.grakn.graql.internal.reasoner.atom.AtomicEquivalence;
import com.google.common.base.Equivalence;
import java.util.Set;

/**
 *
 * <p>
 * Predicate implementation specialising it to be an inequality predicate. Corresponds to graql {@link NeqProperty}.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public abstract class NeqPredicate extends Predicate<Var> {

    private boolean predicateBindingsEquivalent(NeqPredicate that, Equivalence<Atomic> equiv){
        IdPredicate thisPredicate = this.getIdPredicate(this.getVarName());
        IdPredicate thatPredicate = that.getIdPredicate(that.getVarName());
        IdPredicate thisRefPredicate = this.getIdPredicate(this.getPredicate());
        IdPredicate thatRefPredicate = that.getIdPredicate(that.getPredicate());
        return ( (thisPredicate == null) ? (thisPredicate == thatPredicate) : equiv.equivalent(thisPredicate, thatPredicate) )
                && ( (thisRefPredicate == null) ? (thisRefPredicate == thatRefPredicate) : equiv.equivalent(thisRefPredicate, thatRefPredicate) );
    }

    private int bindingHash(Equivalence<Atomic> equiv){
        int hashCode = 1;
        IdPredicate idPredicate = this.getIdPredicate(this.getVarName());
        IdPredicate refIdPredicate = this.getIdPredicate(this.getPredicate());
        hashCode = hashCode * 37 + (idPredicate != null? equiv.hash(idPredicate) : 0);
        hashCode = hashCode * 37 + (refIdPredicate != null? equiv.hash(refIdPredicate) : 0);
        return hashCode;
    }

    @Override
    public boolean isAlphaEquivalent(Object obj){
        if (obj == null || this.getClass() != obj.getClass()) return false;
        if (obj == this) return true;
        NeqPredicate that = (NeqPredicate) obj;
        return predicateBindingsEquivalent(that, AtomicEquivalence.AlphaEquivalence);
    }

    @Override
    public int alphaEquivalenceHashCode() {
        return bindingHash(AtomicEquivalence.AlphaEquivalence);
    }

    @Override
    public boolean isStructurallyEquivalent(Object obj){
        if (obj == null || this.getClass() != obj.getClass()) return false;
        if (obj == this) return true;
        NeqPredicate that = (NeqPredicate) obj;
        return predicateBindingsEquivalent(that, AtomicEquivalence.StructuralEquivalence);
    }

    @Override
    public int structuralEquivalenceHashCode() {
        return bindingHash(AtomicEquivalence.StructuralEquivalence);
    }

    @Override
    public String toString(){
        IdPredicate idPredicate = this.getIdPredicate(this.getVarName());
        IdPredicate refIdPredicate = this.getIdPredicate(this.getPredicate());
        return "[" + getVarName() + "!=" + getPredicate() + "]" +
                (idPredicate != null? idPredicate : "" ) +
                (refIdPredicate != null? refIdPredicate : "");
    }

    @Override
    public String getPredicateValue() {
        return getPredicate().getValue();
    }

    @Override
    public Set<Var> getVarNames(){
        Set<Var> vars = super.getVarNames();
        vars.add(getPredicate());
        return vars;
    }

    /**
     * @param sub substitution to be checked against the predicate
     * @return true if provided subsitution satisfies the predicate
     */
    public abstract boolean isSatisfied(ConceptMap sub);
}
