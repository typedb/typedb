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

import ai.grakn.concept.ConceptId;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.Var;
import ai.grakn.graql.VarPattern;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.atom.binary.TypeAtom;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;
import ai.grakn.graql.internal.reasoner.atom.predicate.Predicate;

import ai.grakn.graql.internal.reasoner.utils.Pair;
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

    public IsaAtom(VarPattern pattern, Var predicateVar, IdPredicate p, ReasonerQuery par) {
        super(pattern, predicateVar, p, par);}
    public IsaAtom(Var var, Var predicateVar, SchemaConcept type, ReasonerQuery par) {
        this(var, predicateVar, new IdPredicate(predicateVar, type.getLabel(), par), par);
    }
    private IsaAtom(Var var, Var predicateVar, IdPredicate p, ReasonerQuery par){
        this(var.isa(predicateVar).admin(), predicateVar, p, par);
    }
    protected IsaAtom(TypeAtom a) { super(a);}

    @Override
    public boolean isAllowedToFormRuleHead(){
        return getSchemaConcept() != null;
    }

    @Override
    public String toString(){
        String typeString = (getSchemaConcept() != null? getSchemaConcept().getLabel() : "") + "(" + getVarName() + ")";
        return typeString + getPredicates().map(Predicate::toString).collect(Collectors.joining(""));
    }

    @Override
    public Atomic copy(){
        return new IsaAtom(this);
    }

    @Override
    public Pattern getCombinedPattern(){
        if (getPredicateVariable().isUserDefinedName()) return super.getCombinedPattern();
        return getSchemaConcept() != null? getVarName().isa(getSchemaConcept().getLabel().getValue()): getVarName().isa(getPredicateVariable());
    }

    protected Pair<VarPattern, IdPredicate> getTypedPair(SchemaConcept type){
        ConceptId typeId = type.getId();
        Var typeVariable = getPredicateVariable().getValue().isEmpty() ? Graql.var().asUserDefined() : getPredicateVariable();

        VarPattern newPattern = getPattern().isa(typeVariable);
        IdPredicate newPredicate = new IdPredicate(typeVariable.id(typeId).admin(), getParentQuery());
        return new Pair<>(newPattern, newPredicate);
    }

    @Override
    public IsaAtom addType(SchemaConcept type) {
        Pair<VarPattern, IdPredicate> typedPair = getTypedPair(type);
        return new IsaAtom(typedPair.getKey(), typedPair.getValue().getVarName(), typedPair.getValue(), this.getParentQuery());
    }

    @Override
    public Set<TypeAtom> unify(Unifier u){
        Collection<Var> vars = u.get(getVarName());
        return vars.isEmpty()?
                Collections.singleton(this) :
                vars.stream().map(v -> new IsaAtom(v, getPredicateVariable(), getTypePredicate(), this.getParentQuery())).collect(Collectors.toSet());
    }

    @Override
    public Atom rewriteWithTypeVariable() {
        return new IsaAtom(getPattern(), getPredicateVariable().asUserDefined(), getTypePredicate(), getParentQuery());
    }

    @Override
    public Atom rewriteToUserDefined(Atom parentAtom) {
        return parentAtom.getPredicateVariable().isUserDefinedName()?
                rewriteWithTypeVariable() :
                this;
    }
}
