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

public class Resource extends Binary{

    public Resource(VarAdmin pattern) { super(pattern);}
    public Resource(VarAdmin pattern, Query par) { super(pattern, par);}
    private Resource(Resource a) { super(a);}

    @Override
    protected boolean isRuleApplicable(InferenceRule child) {
        Atom ruleAtom = child.getRuleConclusionAtom();
        if(!(ruleAtom instanceof Resource)) return false;
        Resource childAtom = (Resource) ruleAtom;
        Query parent = getParentQuery();
        String childVal = child.getHead().getValuePredicate(childAtom.getValueVariable());
        String parentVal = parent.getValuePredicate(getValueVariable());
        return parentVal.isEmpty() || childVal.isEmpty() || parentVal.equals(childVal);
    }

    @Override
    protected String extractValueVariableName(VarAdmin var){
        HasResourceProperty prop = var.getProperties(HasResourceProperty.class).findFirst().orElse(null);
        VarAdmin resVar = prop.getResource();
        return resVar.isUserDefinedName()? resVar.getName() : "";
    }

    @Override
    protected void setValueVariable(String var) {
        valueVariable = var;
        atomPattern.asVar().getProperties(HasResourceProperty.class).forEach(prop -> prop.getResource().setName(var));
    }

    @Override
    public Atomic clone(){
        return new Resource(this);
    }

    @Override
    public boolean isResource(){ return true;}
}
