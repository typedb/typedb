/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */
package io.mindmaps.graql.internal.reasoner.predicate;

import com.google.common.collect.Sets;
import io.mindmaps.graql.admin.VarAdmin;
import io.mindmaps.graql.internal.pattern.property.HasResourceProperty;
import io.mindmaps.graql.internal.reasoner.query.Query;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class Resource extends AtomBase{

    private String valueVariable;

    public Resource(VarAdmin pattern) {
        super(pattern);
        this.valueVariable = extractName(pattern);
    }

    public Resource(VarAdmin pattern, Query par) {
        super(pattern, par);
        this.valueVariable = extractName(pattern);
    }

    public Resource(Resource a) {
        super(a);
        this.valueVariable = extractName(a.getPattern().asVar());
    }

    @Override
    public Atomic clone(){
        return new Resource(this);
    }

    @Override
    public boolean isResource(){ return true;}

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Resource)) return false;
        Resource a2 = (Resource) obj;
        return this.typeId.equals(a2.getTypeId()) && this.varName.equals(a2.getVarName())
                && this.valueVariable.equals(a2.getVal());
    }

    @Override
    public int hashCode() {
        int hashCode = 1;
        hashCode = hashCode * 37 + this.typeId.hashCode();
        hashCode = hashCode * 37 + this.valueVariable.hashCode();
        hashCode = hashCode * 37 + this.varName.hashCode();
        return hashCode;
    }

    @Override
    public boolean isEquivalent(Object obj) {
        if (!(obj instanceof Resource)) return false;
        Resource a2 = (Resource) obj;
        Query parent = getParentQuery();
        return this.typeId.equals(a2.getTypeId())
                && parent.getSubstitution(valueVariable).equals(a2.getParentQuery().getSubstitution(a2.valueVariable));
    }

    @Override
    public int equivalenceHashCode(){
        int hashCode = 1;
        hashCode = hashCode * 37 + this.typeId.hashCode();
        hashCode = hashCode * 37 + getParentQuery().getSubstitution(this.valueVariable).hashCode();
        return hashCode;
    }

    @Override
    public String getVal(){ return valueVariable;}

    private void setValueVariable(String var){
        valueVariable = var;
        atomPattern.asVar().getProperties(HasResourceProperty.class).forEach(prop -> prop.getResource().setName(var));
    }

    @Override
    public Set<String> getVarNames() {
        Set<String> varNames = Sets.newHashSet(getVarName());
        String valueVariable = extractName(getPattern().asVar());
        if (!valueVariable.isEmpty()) varNames.add(valueVariable);
        return varNames;
    }

    @Override
    public Set<Atomic> getValuePredicates() {
        return getParentQuery().getAtoms().stream()
            .filter(Atomic::isValuePredicate)
            .filter(atom -> atom.containsVar(getVal()))
            .collect(Collectors.toSet());
    }

    @Override
    public void unify(String from, String to) {
        super.unify(from, to);
        String var = valueVariable;
        if (var.equals(from)) {
            setValueVariable(to);
        } else if (var.equals(to)) {
            setValueVariable("captured->" + var);
        }
    }

    @Override
    public void unify (Map<String, String> unifiers) {
        super.unify(unifiers);
        String var = valueVariable;
        if (unifiers.containsKey(var)) {
            setValueVariable(unifiers.get(var));
        }
        else if (unifiers.containsValue(var)) {
            setValueVariable("captured->" + var);
        }
    }

    private String extractName(VarAdmin var){
        String name = "";
        Set<HasResourceProperty> props = var.getProperties(HasResourceProperty.class).collect(Collectors.toSet());
        if(props.size() == 1){
            VarAdmin resVar = props.iterator().next().getResource();
            if (resVar.getValuePredicates().isEmpty() && resVar.isUserDefinedName())
                name = resVar.getName();
        }
        return name;
    }

    @Override
    public Map<String, String> getUnifiers(Atomic parentAtom) {
        Map<String, String> unifiers = new HashMap<>();
        String childVarName = this.getVarName();
        String parentVarName = parentAtom.getVarName();
        String childValVarName = this.getVal();
        String parentValVarName = parentAtom.getVal();

        if (!childVarName.equals(parentVarName)) unifiers.put(childVarName, parentVarName);
        if (!childValVarName.equals(parentValVarName)) unifiers.put(childValVarName, parentValVarName);
        return unifiers;
    }
}
