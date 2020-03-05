/*
 * Copyright (C) 2020 Grakn Labs
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
 *
 */

package grakn.core.graql.reasoner.atom.predicate;

import com.google.common.base.Equivalence;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.graql.reasoner.atom.AtomicEquivalence;
import grakn.core.kb.graql.reasoner.atom.Atomic;
import grakn.core.kb.graql.reasoner.query.ReasonerQuery;
import graql.lang.statement.Statement;
import graql.lang.statement.Variable;
import java.util.Set;

public abstract class VariablePredicate extends Predicate<Variable> {

    VariablePredicate(Variable varName, Variable predicateVar, Statement pattern, ReasonerQuery parentQuery) {
        super(varName, pattern, predicateVar, parentQuery);
    }

    private boolean predicateBindingsEquivalent(VariablePredicate that, Equivalence<Atomic> equiv){
        IdPredicate thisPredicate = this.getIdPredicate(this.getVarName());
        IdPredicate thatPredicate = that.getIdPredicate(that.getVarName());
        IdPredicate thisRefPredicate = this.getIdPredicate(this.getPredicate());
        IdPredicate thatRefPredicate = that.getIdPredicate(that.getPredicate());
        return (
                (thisPredicate == null) ?
                thisPredicate == thatPredicate :
                equiv.equivalent(thisPredicate, thatPredicate)
        ) && (
                (thisRefPredicate == null) ?
                (thisRefPredicate == thatRefPredicate) :
                equiv.equivalent(thisRefPredicate, thatRefPredicate)
        );
    }

    private int bindingHash(AtomicEquivalence equiv){
        int hashCode = 1;
        IdPredicate idPredicate = getIdPredicate(getVarName());
        IdPredicate refIdPredicate = getIdPredicate(getPredicate());
        hashCode = hashCode * 37 + (idPredicate != null? equiv.hash(idPredicate) : 0);
        hashCode = hashCode * 37 + (refIdPredicate != null? equiv.hash(refIdPredicate) : 0);
        return hashCode;
    }

    @Override
    public boolean isAlphaEquivalent(Object obj){
        if (obj == null || this.getClass() != obj.getClass()) return false;
        if (obj == this) return true;
        VariablePredicate that = (VariablePredicate) obj;
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
        VariablePredicate that = (VariablePredicate) obj;
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
        return getPredicate().name();
    }

    @Override
    public Set<Variable> getVarNames(){
        Set<Variable> vars = super.getVarNames();
        vars.add(getPredicate());
        return vars;
    }

    /**
     * @param sub substitution to be checked against the predicate
     * @return true if provided subsitution satisfies the predicate
     */
    public abstract boolean isSatisfied(ConceptMap sub);
}
