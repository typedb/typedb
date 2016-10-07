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
import io.mindmaps.graql.admin.ValuePredicateAdmin;
import io.mindmaps.graql.admin.VarAdmin;
import io.mindmaps.graql.internal.pattern.property.HasResourceProperty;
import io.mindmaps.graql.internal.reasoner.query.Query;
import io.mindmaps.util.ErrorMessage;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class Resource extends AtomBase{

    //TODO change to ValuePredicate
    private final String value;

    public Resource(VarAdmin pattern) {
        super(pattern);
        this.value = extractValue(pattern);
    }

    public Resource(VarAdmin pattern, Query par) {
        super(pattern, par);
        this.value = extractValue(pattern);
    }

    public Resource(Resource a) {
        super(a);
        this.value = extractValue(a.getPattern().asVar());
    }

    @Override
    public Atomic clone(){
        return new Resource(this);
    }

    @Override
    public boolean isUnary(){ return true;}

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Resource)) return false;
        Resource a2 = (Resource) obj;
        return this.typeId.equals(a2.getTypeId()) && this.varName.equals(a2.getVarName())
                && this.value.equals(a2.getVal());
    }

    @Override
    public int hashCode() {
        int hashCode = 1;
        hashCode = hashCode * 37 + this.typeId.hashCode();
        hashCode = hashCode * 37 + this.value.hashCode();
        hashCode = hashCode * 37 + this.varName.hashCode();
        return hashCode;
    }

    @Override
    public boolean isEquivalent(Object obj) {
        if (!(obj instanceof Resource)) return false;
        Resource a2 = (Resource) obj;
        return this.typeId.equals(a2.getTypeId()) && this.value.equals(a2.getVal());
    }

    @Override
    public int equivalenceHashCode(){
        int hashCode = 1;
        hashCode = hashCode * 37 + this.typeId.hashCode();
        hashCode = hashCode * 37 + this.value.hashCode();
        return hashCode;
    }

    //TODO extract from ValuePredicate
    @Override
    public String getVal(){ return value;}

    @Override
    public Set<String> getVarNames() {
        Set<String> varNames = Sets.newHashSet(getVarName());
        String valueVariable = extractName(getPattern().asVar());
        if (!valueVariable.isEmpty()) varNames.add(valueVariable);
        return varNames;
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

    private String extractValue(VarAdmin var) {
        String value = "";
        Map<VarAdmin, Set<ValuePredicateAdmin>> resourceMap = var.getResourcePredicates();
        if (resourceMap.size() != 0) {
            if (resourceMap.size() > 1)
                throw new IllegalArgumentException(ErrorMessage.PATTERN_NOT_VAR.getMessage(this.toString()));
            Map.Entry<VarAdmin, Set<ValuePredicateAdmin>> entry = resourceMap.entrySet().iterator().next();
            value = entry.getValue().iterator().hasNext()? entry.getValue().iterator().next().getPredicate().getValue().toString() : "";
        }
        String valueVariable = extractName(getPattern().asVar());
        if (!valueVariable.isEmpty()) value = "$" + valueVariable;
        return value;
    }
}
