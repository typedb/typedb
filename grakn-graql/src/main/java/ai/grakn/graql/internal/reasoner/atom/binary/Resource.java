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

import ai.grakn.concept.TypeLabel;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.concept.ConceptId;
import ai.grakn.graql.VarName;
import ai.grakn.graql.internal.pattern.property.HasResourceProperty;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.internal.reasoner.atom.predicate.Predicate;
import ai.grakn.graql.internal.reasoner.atom.predicate.ValuePredicate;
import ai.grakn.graql.internal.reasoner.query.UnifierImpl;
import ai.grakn.graql.internal.reasoner.rule.InferenceRule;

import java.util.Collections;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 *
 * <p>
 * Atom implementation defining a resource atom.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public class Resource extends MultiPredicateBinary{

    public Resource(VarAdmin pattern, ReasonerQuery par) { this(pattern, Collections.emptySet(), par);}
    public Resource(VarAdmin pattern, Set<Predicate> p, ReasonerQuery par){ super(pattern, p, par);}
    private Resource(Resource a) { super(a);}

    @Override
    protected boolean isRuleApplicable(InferenceRule child) {
        Atom ruleAtom = child.getHead().getAtom();
        if(!(ruleAtom instanceof Resource)) return false;
        Resource childAtom = (Resource) ruleAtom;
        if (childAtom.getMultiPredicate().isEmpty() || getMultiPredicate().isEmpty()) return true;

        for (Predicate childPredicate : childAtom.getMultiPredicate()) {
            Iterator<Predicate> parentIt = getMultiPredicate().iterator();
            boolean predicateCompatible = false;
            while (parentIt.hasNext() && !predicateCompatible) {
                predicateCompatible = childPredicate.getPredicateValue().equals(parentIt.next().getPredicateValue());
            }
            if (predicateCompatible) {
                return true;
            }
        }
        return false;
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
    public boolean requiresMaterialisation(){ return true;}

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
        getMultiPredicate().stream().map(p -> (ValuePredicate) p).forEach(valuePredicates::add);
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
