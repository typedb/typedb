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

import ai.grakn.graql.admin.PatternAdmin;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.pattern.Patterns;
import ai.grakn.graql.internal.reasoner.atom.Atomic;
import ai.grakn.graql.internal.reasoner.atom.AtomicFactory;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;
import ai.grakn.graql.internal.reasoner.atom.predicate.Predicate;
import ai.grakn.graql.internal.reasoner.query.Query;
import com.google.common.collect.Sets;
import java.util.Set;

/**
 *
 * <p>
 * Atom implementation defining a has-role atom.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public class HasRole extends TypeAtom {
    private Predicate relationPredicate = null;

    public HasRole(VarAdmin pattern, Predicate relPredicate, IdPredicate predicate, Query par) {
        super(pattern, predicate, par);
        this.relationPredicate = relPredicate;
    }

    private HasRole(HasRole a){
        super(a);
        this.relationPredicate = a.getRelationPredicate() != null ?
                (Predicate) AtomicFactory.create(a.getRelationPredicate(), getParentQuery()) : null;
    }

    private boolean predicatesEquivalent(HasRole atom) {
        Predicate pred = getRelationPredicate();
        Predicate objPredicate = atom.getRelationPredicate();
        return (pred == null && objPredicate == null)
                || ((pred != null && objPredicate != null) && pred.isEquivalent(objPredicate));
    }

    @Override
    public PatternAdmin getCombinedPattern() {
        Set<VarAdmin> vars = Sets.newHashSet(super.getPattern().asVar());
        if (getPredicate() != null) vars.add(getPredicate().getPattern().asVar());
        if (getRelationPredicate() != null) vars.add(getRelationPredicate().getPattern().asVar());
        return Patterns.conjunction(vars);
    }

    @Override
    public boolean isEquivalent(Object obj) {
        return super.isEquivalent(obj)
                && predicatesEquivalent((HasRole) obj);
    }

    @Override
    public int equivalenceHashCode() {
        int hashCode = super.equivalenceHashCode();
        hashCode = hashCode * 37 + (getRelationPredicate() != null ? getRelationPredicate().equivalenceHashCode() : 0);
        return hashCode;
    }

    @Override
    public Atomic clone(){ return new HasRole(this);}
    public Predicate getRelationPredicate(){ return relationPredicate;}
}
