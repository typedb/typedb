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

package ai.grakn.graql.internal.reasoner.atom.binary.type;

import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.reasoner.atom.binary.TypeAtom;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;
import ai.grakn.graql.internal.reasoner.atom.predicate.Predicate;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

/**
 *
 * <p>
 * TypeAtom corresponding to graql a {@link ai.grakn.graql.internal.pattern.property.IsaProperty} property.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public class IsaAtom extends TypeAtom {

    public IsaAtom(VarPatternAdmin pattern, Var predicateVar, IdPredicate p, ReasonerQuery par) {
        super(pattern, predicateVar, p, par);}
    private IsaAtom(Var var, Var predicateVar, IdPredicate p, ReasonerQuery par){
        this(
                var.isa(predicateVar).admin(),
                predicateVar,
                p,
                par
        );
    }
    protected IsaAtom(TypeAtom a) { super(a);}

    @Override
    public boolean isAllowedToFormRuleHead(){
        return getOntologyConcept() != null;
    }

    @Override
    public String toString(){
        String typeString = (getOntologyConcept() != null? getOntologyConcept().getLabel() : "") + "(" + getVarName() + ")";
        return typeString + getPredicates().map(Predicate::toString).collect(Collectors.joining(""));
    }

    @Override
    public Atomic copy(){
        return new IsaAtom(this);
    }

    @Override
    public Set<TypeAtom> unify(Unifier u){
        Collection<Var> vars = u.get(getVarName());
        return vars.isEmpty()?
                Collections.singleton(this) :
                vars.stream().map(v -> new IsaAtom(v, getPredicateVariable(), getPredicate(), this.getParentQuery())).collect(Collectors.toSet());
    }
}
