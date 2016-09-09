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

import io.mindmaps.util.ErrorMessage;
import io.mindmaps.graql.admin.ValuePredicateAdmin;
import io.mindmaps.graql.admin.VarAdmin;
import io.mindmaps.graql.internal.reasoner.container.Query;

import java.util.*;
import java.util.stream.Collectors;

public class Atom extends AtomBase{

    private final String val;

    public Atom(VarAdmin pattern) {
        super(pattern);
        this.val = extractValue(pattern);
    }

    public Atom(VarAdmin pattern, Query par) {
        super(pattern, par);
        this.val = extractValue(pattern);
    }

    public Atom(Atom a) {
        super(a);
        this.val = extractValue(a.getPattern().asVar());
    }
    @Override
    public boolean isUnary(){ return true;}

        @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Atom)) return false;
        Atom a2 = (Atom) obj;
        return this.typeId.equals(a2.getTypeId()) && this.varName.equals(a2.getVarName())
                && this.val.equals(a2.getVal());
    }

    @Override
    public boolean isEquivalent(Object obj) {
        if (!(obj instanceof Atom)) return false;
        Atom a2 = (Atom) obj;
        return this.typeId.equals(a2.getTypeId()) && this.val.equals(a2.getVal());
    }

    @Override
    public int hashCode() {
        int hashCode = 1;
        hashCode = hashCode * 37 + this.typeId.hashCode();
        hashCode = hashCode * 37 + this.val.hashCode();
        hashCode = hashCode * 37 + this.varName.hashCode();
        return hashCode;
    }

    @Override
    public void print() {
        System.out.println("atom: \npattern: " + toString());
        System.out.println("varName: " + varName + " typeId: " + typeId + " val: " + val);
        if (isValuePredicate()) System.out.println("isValuePredicate");
        System.out.println();
    }

    @Override
    public String getVal(){ return val;}

    @Override
    public Set<Atomic> getTypeConstraints(){
        if (isResource()) {
            Set<Atomic> typeConstraints = getParentQuery().getAtoms();
            return typeConstraints.stream().filter(atom -> atom.isType() && !atom.isResource() && containsVar(atom.getVarName()))
                    .collect(Collectors.toSet());
        }
        else
            return new HashSet<>();
    }

    private String extractValue(VarAdmin var) {
        String value = "";
        Map<VarAdmin, Set<ValuePredicateAdmin>> resourceMap = var.getResourcePredicates();
        if (resourceMap.size() != 0) {
            if (resourceMap.size() != 1)
                throw new IllegalArgumentException(ErrorMessage.PATTERN_NOT_VAR.getMessage(this.toString()));

            Map.Entry<VarAdmin, Set<ValuePredicateAdmin>> entry = resourceMap.entrySet().iterator().next();
            value = entry.getValue().iterator().hasNext()? entry.getValue().iterator().next().getPredicate().getValue().toString() : "";
        }
        return value;
    }
}

