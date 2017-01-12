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

package ai.grakn.graql.internal.reasoner.atom.binary;

import ai.grakn.concept.ConceptId;
import ai.grakn.graql.admin.PatternAdmin;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.VarName;
import ai.grakn.graql.internal.pattern.Patterns;
import ai.grakn.graql.internal.reasoner.atom.AtomBase;
import ai.grakn.graql.internal.reasoner.atom.AtomicFactory;
import ai.grakn.graql.internal.reasoner.atom.predicate.Predicate;
import ai.grakn.graql.internal.reasoner.query.Query;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 *
 * <p>
 * Base implementation for binary atoms with multiple predicate.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public abstract class MultiPredicateBinary extends BinaryBase {
    private final  Set<Predicate> multiPredicate = new HashSet<>();

    protected MultiPredicateBinary(VarAdmin pattern, Set<Predicate> preds, Query par) {
        super(pattern, par);
        this.multiPredicate.addAll(preds);
        this.typeId = extractTypeId(atomPattern.asVar());
    }

    protected MultiPredicateBinary(MultiPredicateBinary a) {
        super(a);
        a.getMultiPredicate().forEach(pred -> multiPredicate.add((Predicate) AtomicFactory.create(pred, getParentQuery())));
        this.typeId = a.getTypeId() != null? ConceptId.of(a.getTypeId().getValue()) : null;
    }

    protected abstract ConceptId extractTypeId(VarAdmin var);

    @Override
    public void setParentQuery(Query q) {
        super.setParentQuery(q);
        multiPredicate.forEach(pred -> pred.setParentQuery(q));
    }

    public Set<Predicate> getMultiPredicate() { return multiPredicate;}

    @Override
    public PatternAdmin getCombinedPattern() {
        Set<VarAdmin> vars = getMultiPredicate().stream()
                .map(AtomBase::getPattern)
                .map(PatternAdmin::asVar)
                .collect(Collectors.toSet());
        vars.add(super.getPattern().asVar());
        return Patterns.conjunction(vars);
    }

    @Override
    protected boolean predicatesEquivalent(BinaryBase at) {
        MultiPredicateBinary atom = (MultiPredicateBinary) at;
        boolean predicatesEquivalent = true;
        Iterator<Predicate> it = getMultiPredicate().iterator();
        while(it.hasNext() && predicatesEquivalent){
            Iterator<Predicate> objIt = atom.getMultiPredicate().iterator();
            boolean predicateHasEquivalent = false;
            while(objIt.hasNext() && !predicateHasEquivalent)
                predicateHasEquivalent = it.next().isEquivalent(objIt.next());
            predicatesEquivalent = predicateHasEquivalent;
        }
        return predicatesEquivalent;
    }

    private int multiPredicateEquivalenceHashCode(){
        int hashCode = 0;
        for (Predicate aMultiPredicate : multiPredicate) hashCode += aMultiPredicate.equivalenceHashCode();
        return hashCode;
    }

    @Override
    public int equivalenceHashCode() {
        int hashCode = 1;
        hashCode = hashCode * 37 + (this.typeId != null? this.typeId.hashCode() : 0);
        hashCode = hashCode * 37 + multiPredicateEquivalenceHashCode();
        return hashCode;
    }

    @Override
    public boolean isValueUserDefinedName() { return multiPredicate.isEmpty();}

    @Override
    public void unify (Map<VarName, VarName> unifiers) {
        super.unify(unifiers);
        multiPredicate.forEach(predicate -> predicate.unify(unifiers));
    }
}
