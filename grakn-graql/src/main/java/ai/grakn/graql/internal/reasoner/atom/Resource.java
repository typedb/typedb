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
package ai.grakn.graql.internal.reasoner.atom;

import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.pattern.property.HasResourceProperty;
import ai.grakn.graql.internal.reasoner.query.Query;
import ai.grakn.graql.internal.reasoner.rule.InferenceRule;
import java.util.Set;
import java.util.stream.Collectors;

public class Resource extends Binary{

    public Resource(VarAdmin pattern) { this(pattern, null);}
    public Resource(VarAdmin pattern, Query par) { this(pattern, null, par);}
    public Resource(VarAdmin pattern, Predicate p, Query par){ super(pattern, p, par);}
    private Resource(Resource a) { super(a);}

    @Override
    protected boolean isRuleApplicable(InferenceRule child) {
        Atom ruleAtom = child.getHead().getAtom();
        if(!(ruleAtom instanceof Resource)) return false;
        Resource childAtom = (Resource) ruleAtom;
        Predicate childPredicate = childAtom.getPredicate();
        Predicate parentPredicate = getPredicate();
        return childPredicate == null || parentPredicate == null
                || parentPredicate.getPredicateValue().equals(childPredicate.getPredicateValue());
    }

    @Override
    protected String extractTypeId(VarAdmin var) {
        HasResourceProperty resProp = var.getProperties(HasResourceProperty.class).findFirst().orElse(null);
        return resProp != null? resProp.getType().orElse("") : "";
    }


    @Override
    protected String extractValueVariableName(VarAdmin var){
        HasResourceProperty prop = var.getProperties(HasResourceProperty.class).findFirst().orElse(null);
        VarAdmin resVar = prop.getResource();
        return resVar.isUserDefinedName()? resVar.getVarName() : "";
    }

    @Override
    protected void setValueVariable(String var) {
        super.setValueVariable(var);
        atomPattern.asVar().getProperties(HasResourceProperty.class).forEach(prop -> prop.getResource().setVarName(var));
    }

    @Override
    public Atomic clone(){
        return new Resource(this);
    }

    @Override
    public boolean isResource(){ return true;}
    @Override
    public boolean isSelectable(){ return true;}

    //TODO fix the single predicate
    @Override
    public Set<Predicate> getValuePredicates(){
        return getParentQuery().getValuePredicates().stream()
                .filter(atom -> atom.getVarName().equals(getValueVariable()))
                .collect(Collectors.toSet());
    }
}
