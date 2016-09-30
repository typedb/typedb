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
import io.mindmaps.MindmapsGraph;
import io.mindmaps.concept.Concept;
import io.mindmaps.concept.Rule;
import io.mindmaps.util.ErrorMessage;
import io.mindmaps.concept.RoleType;
import io.mindmaps.concept.Type;
import io.mindmaps.graql.*;
import io.mindmaps.graql.admin.PatternAdmin;
import io.mindmaps.graql.admin.ValuePredicateAdmin;
import io.mindmaps.graql.admin.VarAdmin;
import io.mindmaps.graql.internal.pattern.Patterns;
import io.mindmaps.graql.internal.reasoner.query.Query;
import javafx.util.Pair;

import java.util.*;

public abstract class AtomBase implements Atomic{

    protected String varName;
    protected final String typeId;

    protected final PatternAdmin atomPattern;

    private Query parent = null;

    public AtomBase() {
        this.varName = null;
        this.typeId = null;
        this.atomPattern = null;
    }

    public AtomBase(VarAdmin pattern) {
        this.atomPattern = pattern;
        Pair<String, String> varData = extractDataFromVar(atomPattern.asVar());
        this.varName = varData.getKey();
        this.typeId = varData.getValue();
    }

    public AtomBase(VarAdmin pattern, Query par) {
        this.atomPattern = pattern;
        Pair<String, String> varData = extractDataFromVar(atomPattern.asVar());
        this.varName = varData.getKey();
        this.typeId = varData.getValue();
        this.parent = par;
    }

    public AtomBase(AtomBase a) {
        if (a.getParentQuery() != null)
            this.parent = a.getParentQuery();

        this.atomPattern = Patterns.mergeVars(Sets.newHashSet(a.atomPattern.asVar()));
        Pair<String, String> varData = extractDataFromVar(atomPattern.asVar());
        varName = varData.getKey();
        typeId = varData.getValue();
    }

    public abstract Atomic clone();

    private Pair<String, String> extractDataFromVar(VarAdmin var) {
        String vTypeId;
        String vName = var.getName();

        Map<VarAdmin, Set<ValuePredicateAdmin>> resourceMap = var.getResourcePredicates();
        if (resourceMap.size() != 0) {
            if (resourceMap.size() != 1)
                throw new IllegalArgumentException(ErrorMessage.MULTIPLE_RESOURCES.getMessage(var.toString()));

            Map.Entry<VarAdmin, Set<ValuePredicateAdmin>> entry = resourceMap.entrySet().iterator().next();
            vTypeId = entry.getKey().getId().orElse("");
        }
        else
            vTypeId = var.getType().flatMap(VarAdmin::getId).orElse("");

        return new Pair<>(vName, vTypeId);
    }

    @Override
    public String toString(){ return atomPattern.toString(); }

    @Override
    public void print() {
        System.out.println("atom: \npattern: " + toString());
        System.out.println("varName: " + varName + " typeId: " + typeId);
        System.out.println();
    }

    @Override
    public boolean isResource(){ return !atomPattern.asVar().getResourcePredicates().isEmpty();}
    @Override
    public boolean isType(){ return !typeId.isEmpty();}
    @Override
    public boolean isRuleResolvable(){
        Type type = getParentQuery().getGraph().orElse(null).getType(getTypeId());
        return !type.getRulesOfConclusion().isEmpty();
    }

    @Override
    public boolean isRecursive(){
        if (isResource()) return false;
        boolean atomRecursive = false;

        String typeId = getTypeId();
        if (typeId.isEmpty()) return false;
        Type type = getParentQuery().getGraph().orElse(null).getType(typeId);
        Collection<Rule> presentInConclusion = type.getRulesOfConclusion();
        Collection<Rule> presentInHypothesis = type.getRulesOfHypothesis();

        for(Rule rule : presentInConclusion)
            atomRecursive |= presentInHypothesis.contains(rule);

        return atomRecursive;
    }
    @Override
    public boolean containsVar(String name){ return varName.equals(name);}

    @Override
    public PatternAdmin getPattern(){ return atomPattern;}

    private MatchQuery getBaseMatchQuery(MindmapsGraph graph) {
        QueryBuilder qb = Graql.withGraph(graph);
        MatchQuery matchQuery = qb.match(getPattern());

        //add substitutions
        Map<String, Atomic> varSubMap = getVarSubMap();
        Set<String> selectVars = getVarNames();
        //form a disjunction of each set of subs for a given variable and add to query
        varSubMap.forEach( (var, sub) -> {
            Set<PatternAdmin> patterns = new HashSet<>();
            patterns.add(sub.getPattern());
            matchQuery.admin().getPattern().getPatterns().add(Patterns.conjunction(patterns));
        });
        return matchQuery.admin().select(selectVars);
    }

    @Override
    public MatchQuery getMatchQuery(MindmapsGraph graph) {
        return getBaseMatchQuery(graph);
    }

    @Override
    public Query getParentQuery(){
        if(parent == null)
            throw new IllegalStateException(ErrorMessage.PARENT_MISSING.getMessage());
        return parent;
    }

    @Override
    public void setParentQuery(Query q){ parent = q;}

    private void setVarName(String var){
        varName = var;
        atomPattern.asVar().setName(var);
    }

    @Override
    public void changeEachVarName(String from, String to) {
        String var = getVarName();
        if (var.equals(from)) {
            setVarName(to);
        } else if (var.equals(to)) {
            setVarName("captured->" + var);
        }
    }

    @Override
    public void changeEachVarName(Map<String, String> mappings){
        String var = getVarName();
        if (mappings.containsKey(var)) {
            setVarName(mappings.get(var));
        }
        else if (mappings.containsValue(var)) {
            setVarName("captured->" + var);
        }
    }

    @Override
    public String getVarName(){ return varName;}
    @Override
    public Set<String> getVarNames(){
        return Sets.newHashSet(varName);
    }
    @Override
    public String getTypeId(){ return typeId;}
    @Override
    public String getVal(){ return null;}

    @Override
    public Map<String, String> getUnifiers(Atomic parentAtom) {
        Set<String> varsToAllocate = parentAtom.getVarNames();

        Set<String> childBVs = getVarNames();

        Map<String, String> unifiers = new HashMap<>();
        Map<String, Pair<Type, RoleType>> childMap = getVarTypeRoleMap();
        Map<RoleType, Pair<String, Type>> parentMap = parentAtom.getRoleVarTypeMap();

        for (String chVar : childBVs) {
            RoleType role = childMap.containsKey(chVar) ? childMap.get(chVar).getValue() : null;
            String pVar = role != null && parentMap.containsKey(role) ? parentMap.get(role).getKey() : "";
            if (pVar.isEmpty())
                pVar = varsToAllocate.iterator().next();

            if (!chVar.equals(pVar))
                unifiers.put(chVar, pVar);

            varsToAllocate.remove(pVar);
        }

        return unifiers;
    }

    @Override
    public Set<Atomic> getSubstitutions() {
        Set<Atomic> subs = new HashSet<>();
        getParentQuery().getAtoms().forEach( atom ->{
            if(atom.isSubstitution() && containsVar(atom.getVarName()) )
                subs.add(atom);
        });
        return subs;
    }

    @Override
    public Set<Atomic> getTypeConstraints(){
        throw new IllegalArgumentException(ErrorMessage.NO_TYPE_CONSTRAINTS.getMessage());
    }

    @Override
    public Map<String, Atomic> getVarSubMap() {
        Map<String, Atomic> map = new HashMap<>();
        getSubstitutions().forEach( sub -> {
            String var = sub.getVarName();
            map.put(var, sub);
        });
        return map;
    }

    @Override
    public Map<RoleType, String> getRoleConceptIdMap(){
        Map<RoleType, String> roleConceptMap = new HashMap<>();
        Map<String, Atomic> varSubMap = getVarSubMap();
        Map<RoleType, Pair<String, Type>> roleVarMap = getRoleVarTypeMap();

        roleVarMap.forEach( (role, varTypePair) -> {
            String var = varTypePair.getKey();
            roleConceptMap.put(role, varSubMap.containsKey(var) ? varSubMap.get(var).getVal() : "");
        });

        return roleConceptMap;
    }

    public Map<String, javafx.util.Pair<Type, RoleType>> getVarTypeRoleMap() {
        Map<String, javafx.util.Pair<Type, RoleType>> roleVarTypeMap = new HashMap<>();
        if (getParentQuery() == null) return roleVarTypeMap;

        Set<String> vars = getVarNames();
        Map<String, Type> varTypeMap = getParentQuery().getVarTypeMap();

        vars.forEach(var -> {
            Type type = varTypeMap.get(var);
            roleVarTypeMap.put(var, new Pair<>(type, null));
        });
        return roleVarTypeMap;
    }

    public Map<RoleType, Pair<String, Type>> getRoleVarTypeMap() {
        return new HashMap<>();
    }

}

