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

import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.TypeName;
import ai.grakn.graql.VarName;
import ai.grakn.graql.internal.pattern.property.HasResourceProperty;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.internal.reasoner.atom.predicate.Predicate;
import ai.grakn.graql.internal.reasoner.atom.predicate.ValuePredicate;
import ai.grakn.graql.internal.reasoner.rule.InferenceRule;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

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
        boolean ruleApplicable = false;
        Resource childAtom = (Resource) ruleAtom;
        if (childAtom.getMultiPredicate().isEmpty() || getMultiPredicate().isEmpty()) return true;

        Iterator<Predicate> childIt = childAtom.getMultiPredicate().iterator();
        while(childIt.hasNext() && !ruleApplicable){
            Predicate childPredicate = childIt.next();
            Iterator<Predicate> parentIt = getMultiPredicate().iterator();
            boolean predicateCompatible = false;
            while(parentIt.hasNext() && !predicateCompatible) {
                predicateCompatible = childPredicate.getPredicateValue().equals(parentIt.next().getPredicateValue());
            }
            ruleApplicable = predicateCompatible;
        }
        return ruleApplicable;
    }

    @Override
    protected ConceptId extractTypeId(VarAdmin var) {
        HasResourceProperty resProp = var.getProperties(HasResourceProperty.class).findFirst().orElse(null);
        TypeName typeName = resProp != null? resProp.getType().orElse(null) : null;
        return typeName != null ? getParentQuery().graph().getType(typeName).getId() : null;
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
        atomPattern.asVar().getProperties(HasResourceProperty.class).forEach(prop -> prop.getResource().setVarName(var));
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
    public Set<ValuePredicate> getValuePredicates(){
        Set<ValuePredicate> valuePredicates = super.getValuePredicates();
        getMultiPredicate().stream().map(p -> (ValuePredicate) p).forEach(valuePredicates::add);
        return valuePredicates;
    }
}
