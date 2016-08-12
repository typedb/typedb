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
import io.mindmaps.core.MindmapsTransaction;
import io.mindmaps.graql.*;
import io.mindmaps.graql.internal.reasoner.container.Query;
import org.javatuples.Pair;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public abstract class AtomBase implements Atomic{

    protected String varName;
    protected final String typeId;

    protected final Pattern.Admin atomPattern;
    protected final Set<Query> expansions = new HashSet<>();

    private Query parent = null;

    public AtomBase()
    {
        this.varName = null;
        this.typeId = null;
        this.atomPattern = null;
    }

    public AtomBase(Var.Admin pattern)
    {
        this.atomPattern = pattern;
        Pair<String, String> varData = getDataFromVar(atomPattern.asVar());
        this.varName = varData.getValue0();
        this.typeId = varData.getValue1();
    }

    public AtomBase(Var.Admin pattern, Query par)
    {
        this.atomPattern = pattern;
        Pair<String, String> varData = getDataFromVar(atomPattern.asVar());
        this.varName = varData.getValue0();
        this.typeId = varData.getValue1();
        this.parent = par;
    }

    public AtomBase(AtomBase a)
    {
        this.atomPattern = a.atomPattern;
        Pair<String, String> varData = getDataFromVar(atomPattern.asVar());
        varName = varData.getValue0();
        typeId = varData.getValue1();
        a.expansions.forEach(exp -> expansions.add(new Query(exp)));
    }

    @Override
    public String toString(){ return atomPattern.toString(); }

    @Override
    public boolean equals(Object obj)
    {
        if (!(obj instanceof AtomBase)) return false;
        AtomBase a2 = (AtomBase) obj;
        return this.getTypeId().equals(a2.getTypeId()) && this.getVarName().equals(a2.getVarName());
    }

    @Override
    public int hashCode()
    {
        int hashCode = 1;
        hashCode = hashCode * 37 + this.typeId.hashCode();
        hashCode = hashCode * 37 + this.varName.hashCode();
        return hashCode;
    }

    @Override
    public void print()
    {
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
        if(expansions.contains(query))
        {
            query.setParentAtom(null);
            expansions.remove(query);
        }
    }

    @Override
    public boolean isValuePredicate(){ return !atomPattern.asVar().getValuePredicates().isEmpty();}
    @Override
    public boolean isResource(){ return !atomPattern.asVar().getResourcePredicates().isEmpty();}
    @Override
    public boolean isType(){ return !typeId.isEmpty();}
    @Override
    public boolean containsVar(String name){ return varName.equals(name);}

    @Override
    public Pattern.Admin getPattern(){ return atomPattern;}
    @Override
    public Pattern.Admin getExpandedPattern()
    {
        Set<Pattern.Admin> expandedPattern = new HashSet<>();
        expandedPattern.add(atomPattern);
        expansions.forEach(q -> expandedPattern.add(q.getExpandedPattern()));
        return Pattern.Admin.disjunction(expandedPattern);
    }

    @Override
    public MatchQueryMap getExpandedMatchQuery(MindmapsTransaction graph)
    {
        QueryBuilder qb = QueryBuilder.build(graph);
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

    private Pair<String, String> getDataFromVar(Var.Admin var) {

        String vTypeId;
        String vName = var.getName();

        Map<Var.Admin, Set<ValuePredicate.Admin>> resourceMap = var.getResourcePredicates();

        if (resourceMap.size() != 0) {
            if (resourceMap.size() != 1)
                throw new IllegalArgumentException("Multiple resource types in extractData");

            Map.Entry<Var.Admin, Set<ValuePredicate.Admin>> entry = resourceMap.entrySet().iterator().next();
            vTypeId = entry.getKey().getId().get();
        }
        else
            vTypeId = var.getType().flatMap(Var.Admin::getId).orElse("");

        return new Pair<>(vName, vTypeId);

    }

}

