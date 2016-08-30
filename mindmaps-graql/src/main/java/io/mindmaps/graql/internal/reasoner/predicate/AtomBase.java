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
import io.mindmaps.MindmapsTransaction;
import io.mindmaps.constants.ErrorMessage;
import io.mindmaps.graql.*;
import io.mindmaps.graql.admin.PatternAdmin;
import io.mindmaps.graql.admin.ValuePredicateAdmin;
import io.mindmaps.graql.admin.VarAdmin;
import io.mindmaps.graql.internal.query.ConjunctionImpl;
import io.mindmaps.graql.internal.query.DisjunctionImpl;
import io.mindmaps.graql.internal.reasoner.container.Query;
import org.javatuples.Pair;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public abstract class AtomBase implements Atomic{

    protected String varName;
    protected final String typeId;

    protected final PatternAdmin atomPattern;
    protected final Set<Query> expansions = new HashSet<>();

    private Query parent = null;

    public AtomBase() {
        this.varName = null;
        this.typeId = null;
        this.atomPattern = null;
    }

    public AtomBase(VarAdmin pattern) {
        this.atomPattern = pattern;
        Pair<String, String> varData = extractDataFromVar(atomPattern.asVar());
        this.varName = varData.getValue0();
        this.typeId = varData.getValue1();
    }

    public AtomBase(VarAdmin pattern, Query par) {
        this.atomPattern = pattern;
        Pair<String, String> varData = extractDataFromVar(atomPattern.asVar());
        this.varName = varData.getValue0();
        this.typeId = varData.getValue1();
        this.parent = par;
    }

    public AtomBase(AtomBase a) {
        this.atomPattern = a.atomPattern;
        Pair<String, String> varData = extractDataFromVar(atomPattern.asVar());
        varName = varData.getValue0();
        typeId = varData.getValue1();
        a.expansions.forEach(exp -> expansions.add(new Query(exp)));
    }

    @Override
    public String toString(){ return atomPattern.toString(); }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof AtomBase)) return false;
        AtomBase a2 = (AtomBase) obj;
        return this.getTypeId().equals(a2.getTypeId()) && this.getVarName().equals(a2.getVarName());
    }

    @Override
    public int hashCode() {
        int hashCode = 1;
        hashCode = hashCode * 37 + this.typeId.hashCode();
        hashCode = hashCode * 37 + this.varName.hashCode();
        return hashCode;
    }

    @Override
    public void print() {
        System.out.println("atom: \npattern: " + toString());
        System.out.println("varName: " + varName + " typeId: " + typeId);
        if (isValuePredicate()) System.out.println("isValuePredicate");
        System.out.println();
    }

    @Override
    public void addExpansion(Query query){
        query.setParentAtom(this);
        expansions.add(query);
    }

    @Override
    public void removeExpansion(Query query){
        if(expansions.contains(query)) {
            query.setParentAtom(null);
            expansions.remove(query);
        }
    }

    @Override
    public boolean isResource(){ return !atomPattern.asVar().getResourcePredicates().isEmpty();}
    @Override
    public boolean isType(){ return !typeId.isEmpty();}
    @Override
    public boolean containsVar(String name){ return varName.equals(name);}

    @Override
    public PatternAdmin getPattern(){ return atomPattern;}
    @Override
    public PatternAdmin getExpandedPattern() {
        Set<PatternAdmin> expandedPattern = new HashSet<>();
        expandedPattern.add(atomPattern);
        expansions.forEach(q -> expandedPattern.add(q.getExpandedPattern()));
        return new DisjunctionImpl<>(expandedPattern);
    }

    protected MatchQueryDefault getBaseMatchQuery(MindmapsTransaction graph) {
        QueryBuilder qb = Graql.withTransaction(graph);
        MatchQueryDefault matchQuery = qb.match(getPattern());

        //add substitutions
        Map<String, Set<Atomic>> varSubMap = getVarSubMap();
        Set<String> selectVars = getVarNames();
        //form a disjunction of each set of subs for a given variable and add to query
        varSubMap.forEach( (key, val) -> {
            Set<PatternAdmin> patterns = new HashSet<>();
            val.forEach(sub -> patterns.add(sub.getPattern()));
            matchQuery.admin().getPattern().getPatterns().add(new ConjunctionImpl<>(patterns));
        });
        return matchQuery.admin().select(selectVars);
    }

    @Override
    public MatchQueryDefault getMatchQuery(MindmapsTransaction graph) {
        return getBaseMatchQuery(graph);
    }

    @Override
    public MatchQueryDefault getExpandedMatchQuery(MindmapsTransaction graph) {
        QueryBuilder qb = Graql.withTransaction(graph);
        Set<String> selectVars = Sets.newHashSet(varName);
        return qb.match(getExpandedPattern()).select(selectVars);
    }

    @Override
    public Query getParentQuery(){return parent;}
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
    public Set<Query> getExpansions(){ return expansions;}

    @Override
    public Set<Atomic> getSubstitutions() {
        Set<Atomic> subs = new HashSet<>();
        getParentQuery().getAtoms().forEach( atom ->{
            if(atom.isValuePredicate() && containsVar(atom.getVarName()) )
                subs.add(atom);
        });
        return subs;
    }

    public Set<Atomic> getTypeConstraints(){
        throw new IllegalArgumentException(ErrorMessage.NO_TYPE_CONSTRAINTS.getMessage());
    }

    public Set<Atomic> getNeighbours(){
        Set<Atomic> neighbours = new HashSet<>();
        getParentQuery().getAtoms().forEach(atom ->{
            //TODO allow unary predicates
            if (!atom.equals(this) && !atom.isValuePredicate() && !atom.isUnary()) {
                Set<String> intersection = new HashSet<>(getVarNames());
                intersection.retainAll(atom.getVarNames());
                if (!intersection.isEmpty()) neighbours.add(atom);
            }
        });
        return neighbours;
    }

    @Override
    public Map<String, Set<Atomic>> getVarSubMap() {
        Map<String, Set<Atomic>> map = new HashMap<>();
        getSubstitutions().forEach( sub -> {
            String var = sub.getVarName();
            if (map.containsKey(var))
                map.get(var).add(sub);
            else
                map.put(var, Sets.newHashSet(sub));
        });
        return map;
    }

    private Pair<String, String> extractDataFromVar(VarAdmin var) {
        String vTypeId;
        String vName = var.getName();

        Map<VarAdmin, Set<ValuePredicateAdmin>> resourceMap = var.getResourcePredicates();
        if (resourceMap.size() != 0) {
            if (resourceMap.size() != 1)
                throw new IllegalArgumentException(ErrorMessage.MULTIPLE_RESOURCES.getMessage(var.toString()));

            Map.Entry<VarAdmin, Set<ValuePredicateAdmin>> entry = resourceMap.entrySet().iterator().next();
            vTypeId = entry.getKey().getId().get();
        }
        else
            vTypeId = var.getType().flatMap(VarAdmin::getId).orElse("");

        return new Pair<>(vName, vTypeId);
    }

}

