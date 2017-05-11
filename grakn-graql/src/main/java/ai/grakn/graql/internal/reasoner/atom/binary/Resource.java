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

import ai.grakn.concept.Type;
import ai.grakn.concept.TypeLabel;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.admin.ValuePredicateAdmin;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.concept.ConceptId;
import ai.grakn.graql.VarName;
import ai.grakn.graql.internal.pattern.property.HasResourceProperty;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.internal.reasoner.atom.AtomicFactory;
import ai.grakn.graql.internal.reasoner.atom.ResolutionStrategy;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;
import ai.grakn.graql.internal.reasoner.atom.predicate.Predicate;
import ai.grakn.graql.internal.reasoner.atom.predicate.ValuePredicate;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueryImpl;
import ai.grakn.graql.internal.reasoner.query.UnifierImpl;
import ai.grakn.graql.internal.reasoner.rule.InferenceRule;

import java.util.Collections;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static ai.grakn.graql.internal.reasoner.ReasonerUtils.checkTypesDisjoint;

/**
 *
 * <p>
 * Atom implementation defining a resource atom corresponding to a {@link HasResourceProperty}.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public class Resource extends MultiPredicateBinary<ValuePredicate>{

    public Resource(VarAdmin pattern, ReasonerQuery par) { this(pattern, Collections.emptySet(), par);}
    public Resource(VarAdmin pattern, Set<ValuePredicate> p, ReasonerQuery par){ super(pattern, p, par);}
    private Resource(Resource a) {
        super(a);
        Set<ValuePredicate> multiPredicate = getMultiPredicate();
        a.getMultiPredicate().stream()
                .map(pred -> (ValuePredicate) AtomicFactory.create(pred, getParentQuery()))
                .forEach(multiPredicate::add);
        this.typeId = a.getTypeId() != null? ConceptId.of(a.getTypeId().getValue()) : null;
    }

    @Override
    public String toString(){
        String multiPredicateString = getMultiPredicate().isEmpty()?
                getValueVariable().toString() :
                getMultiPredicate().stream().map(Predicate::getPredicate).collect(Collectors.toSet()).toString();
        return getVarName() + " has " + getType().getLabel() + " " +
                multiPredicateString +
                getIdPredicates().stream().map(IdPredicate::toString).collect(Collectors.joining(""));
    }

    @Override
    protected boolean hasEquivalentPredicatesWith(BinaryBase at) {
        if (!(at instanceof Resource)) return false;
        Resource atom = (Resource) at;
        if(this.getMultiPredicate().size() != atom.getMultiPredicate().size()) return false;
        for (ValuePredicate predicate : getMultiPredicate()) {
            Iterator<ValuePredicate> objIt = atom.getMultiPredicate().iterator();
            boolean predicateHasEquivalent = false;
            while (objIt.hasNext() && !predicateHasEquivalent) {
                predicateHasEquivalent = predicate.isEquivalent(objIt.next());
            }
            if (!predicateHasEquivalent) return false;
        }
        return true;
    }

    @Override
    public boolean isRuleApplicable(InferenceRule child) {
        Atom ruleAtom = child.getRuleConclusionAtom();
        if(!(ruleAtom.isResource())) return false;

        Resource childAtom = (Resource) ruleAtom;

        //check types
        TypeAtom parentTypeConstraint = this.getTypeConstraints().stream().findFirst().orElse(null);
        TypeAtom childTypeConstraint = childAtom.getTypeConstraints().stream().findFirst().orElse(null);

        Type parentType = parentTypeConstraint != null? parentTypeConstraint.getType() : null;
        Type childType = childTypeConstraint != null? childTypeConstraint.getType() : null;
        if (parentType != null && childType != null && checkTypesDisjoint(parentType, childType)) return false;

        //check predicates
        if (childAtom.getMultiPredicate().isEmpty() || getMultiPredicate().isEmpty()) return true;
        for (ValuePredicate childPredicate : childAtom.getMultiPredicate()) {
            Iterator<ValuePredicate> parentIt = getMultiPredicate().iterator();
            boolean predicateCompatible = false;
            while (parentIt.hasNext() && !predicateCompatible) {
                ValuePredicate parentPredicate = parentIt.next();
                predicateCompatible = parentPredicate.getPredicate().isCompatibleWith(childPredicate.getPredicate());
            }
            if (!predicateCompatible) return false;
        }
        return true;
    }

    @Override
    public Set<VarName> getVarNames() {
        Set<VarName> vars = super.getVarNames();
        getMultiPredicate().stream().flatMap(p -> p.getVarNames().stream()).forEach(vars::add);
        return vars;
    }

    @Override
    protected ConceptId extractTypeId(VarAdmin var) {
        HasResourceProperty resProp = var.getProperties(HasResourceProperty.class).findFirst().orElse(null);
        TypeLabel typeLabel = resProp != null? resProp.getType() : null;
        return typeLabel != null ? getParentQuery().graph().getType(typeLabel).getId() : null;
    }

    @Override
    protected VarName extractValueVariableName(VarAdmin var){
        HasResourceProperty prop = var.getProperties(HasResourceProperty.class).findFirst().orElse(null);
        VarAdmin resVar = prop.getResource();
        return resVar.isUserDefinedName()? resVar.getVarName() : VarName.of("");
    }

    @Override
    protected void setValueVariable(VarName var) {
        super.setValueVariable(var);
        atomPattern = atomPattern.asVar().mapProperty(HasResourceProperty.class, prop -> prop.setResource(prop.getResource().setVarName(var)));
    }

    @Override
    public Atomic copy(){ return new Resource(this);}

    @Override
    public boolean isResource(){ return true;}

    @Override
    public boolean isSelectable(){ return true;}

    @Override
    public boolean isAllowedToFormRuleHead(){
        if (getType() == null || getMultiPredicate().size() > 1) return false;
        if (getMultiPredicate().isEmpty()) return true;

        ValuePredicate predicate = getMultiPredicate().iterator().next();
        return predicate.getPredicate().isSpecific();
    }

    @Override
    public boolean requiresMaterialisation(){ return true;}

    @Override
    public int resolutionPriority(){
        int priority = super.resolutionPriority();
        ReasonerQueryImpl parent = (ReasonerQueryImpl) getParentQuery();
        Set<ValuePredicateAdmin> vps = getValuePredicates().stream().map(ValuePredicate::getPredicate).collect(Collectors.toSet());

        priority += ResolutionStrategy.IS_RESOURCE_ATOM;

        if (vps.isEmpty()){
            if (parent.getIdPredicate(getValueVariable()) != null) priority += ResolutionStrategy.SPECIFIC_VALUE_PREDICATE;
            else priority += ResolutionStrategy.VARIABLE_VALUE_PREDICATE;
        } else {
            for (ValuePredicateAdmin vp : vps) {
                if (vp.isSpecific()) {
                    priority += ResolutionStrategy.SPECIFIC_VALUE_PREDICATE;
                } else if (vp.getInnerVar().isPresent()) {
                    VarAdmin innerVar = vp.getInnerVar().orElse(null);
                    if (parent.getIdPredicate(innerVar.getVarName()) != null) priority += ResolutionStrategy.SPECIFIC_VALUE_PREDICATE;
                    else priority += ResolutionStrategy.VARIABLE_VALUE_PREDICATE;
                } else {
                    priority += ResolutionStrategy.NON_SPECIFIC_VALUE_PREDICATE;
                }
            }
        }

        return priority;
    }

    @Override
    public Unifier getUnifier(Atomic parentAtom) {
        if (!(parentAtom instanceof TypeAtom)) return super.getUnifier(parentAtom);

        Unifier unifier = new UnifierImpl();
        unifier.addMapping(this.getValueVariable(), parentAtom.getVarName());
        if (parentAtom.containsVar(this.getVarName())) unifier.addMapping(this.getVarName(), VarName.anon());
        return unifier;
    }

    @Override
    public Set<ValuePredicate> getValuePredicates(){
        Set<ValuePredicate> valuePredicates = super.getValuePredicates();
        getMultiPredicate().forEach(valuePredicates::add);
        return valuePredicates;
    }

    @Override
    public Set<TypeAtom> getMappedTypeConstraints() {
        return getTypeConstraints().stream()
                .filter(t -> t.getVarName().equals(getVarName()))
                .filter(t -> Objects.nonNull(t.getType()))
                .collect(Collectors.toSet());
    }

}
