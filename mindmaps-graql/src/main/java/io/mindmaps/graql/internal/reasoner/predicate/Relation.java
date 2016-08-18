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

import io.mindmaps.MindmapsTransaction;
import io.mindmaps.core.model.RoleType;
import io.mindmaps.core.model.Type;
import io.mindmaps.graql.Graql;
import io.mindmaps.graql.MatchQueryDefault;
import io.mindmaps.graql.QueryBuilder;
import io.mindmaps.graql.Var;
import io.mindmaps.graql.admin.PatternAdmin;
import io.mindmaps.graql.admin.VarAdmin;
import io.mindmaps.graql.internal.query.ConjunctionImpl;
import io.mindmaps.graql.internal.query.DisjunctionImpl;
import io.mindmaps.graql.internal.reasoner.container.Query;
import org.javatuples.Pair;

import java.util.*;

import static io.mindmaps.graql.internal.reasoner.Utility.getCompatibleRoleTypes;

public class Relation extends AtomBase{

    private final Set<Var.Casting> castings = new HashSet<>();


    public Relation(VarAdmin pattern){
        super(pattern);
        castings.addAll(pattern.getCastings());
    }


    public Relation(VarAdmin pattern, Query par)
    {
        super(pattern, par);
        castings.addAll(pattern.getCastings());
    }

    public Relation(Relation a)
    {
        super(a);
        castings.addAll(a.getPattern().asVar().getCastings());

        expansions.forEach(this::removeExpansion);
        a.expansions.forEach(exp -> expansions.add(new Query(exp)));
    }

    @Override
    public boolean equals(Object obj)
    {
        if (!(obj instanceof Relation)) return false;
        Relation a2 = (Relation) obj;
        return this.getTypeId().equals(a2.getTypeId()) && this.getVarNames().equals(a2.getVarNames());
    }

    @Override
    public boolean isEquivalent(Object obj)
    {
        if (!(obj instanceof Relation)) return false;
        Relation a2 = (Relation) obj;
        return this.getTypeId().equals(a2.getTypeId());
    }

    @Override
    public int hashCode()
    {
        int hashCode = 1;
        hashCode = hashCode * 37 + this.typeId.hashCode();
        hashCode = hashCode * 37 + getVarNames().hashCode();
        return hashCode;
    }

    @Override
    public void print()
    {
        System.out.println("atom: \npattern: " + toString());
        System.out.println("varName: " + varName + " typeId: " + typeId);
        System.out.print("Castings: ");
        castings.forEach(c -> System.out.print(c.getRolePlayer().getPrintableName() + " "));
        System.out.println();
    }

    @Override
    public boolean isRelation(){ return true;}
    @Override
    public boolean isValuePredicate(){ return false;}
    @Override
    public boolean isResource(){ return false;}
    @Override
    public boolean isType(){ return true;}
    @Override
    public boolean containsVar(String name)
    {
        boolean varFound = false;

        Iterator<Var.Casting> it = castings.iterator();
        while(it.hasNext() && !varFound)
            varFound = it.next().getRolePlayer().getName().equals(name);

        return varFound;
    }

    @Override
    public MatchQueryDefault getMatchQuery(MindmapsTransaction graph)
    {
        QueryBuilder qb = Graql.withTransaction(graph);
        MatchQueryDefault matchQuery = qb.match(getPattern());

        //add substitutions
        Map<String, Set<Atomic>> varSubMap = getVarSubMap();
        Set<String> selectVars = getVarNames();
        //form a disjunction of each set of subs for a given variable and add to query
        varSubMap.forEach( (key, val) -> {
            //selectVars.remove(key);
            Set<PatternAdmin> patterns = new HashSet<>();
            val.forEach(sub -> patterns.add(sub.getPattern()));
            matchQuery.admin().getPattern().getPatterns().add(new DisjunctionImpl<>(patterns));
        });

        //add constraints
        Set<PatternAdmin> patterns = new HashSet<>();
        getTypeConstraints().forEach(c -> patterns.add(c.getPattern()));
        matchQuery.admin().getPattern().getPatterns().add(new ConjunctionImpl<>(patterns));

        return matchQuery.select(selectVars);
    }

    @Override
    public MatchQueryDefault getExpandedMatchQuery(MindmapsTransaction graph)
    {
        QueryBuilder qb = Graql.withTransaction(graph);
        Set<String> selectVars = getVarNames();
        return qb.match(getExpandedPattern()).select(selectVars);
    }

    @Override
    public void changeEachVarName(String from, String to)
    {
        castings.forEach(c -> {
            String var = c.getRolePlayer().getName();
            if (var.equals(from)) {
                c.getRolePlayer().setName(to);
            }
            //mark captured variable
            else if (var.equals(to)) {
                c.getRolePlayer().setName("captured->" + var);
            }
        });
    }

    @Override
    public void changeEachVarName(Map<String, String> mappings)
    {
        castings.forEach(c -> {
            String var = c.getRolePlayer().getName();
            if (mappings.containsKey(var) ) {
                String target = mappings.get(var);
                c.getRolePlayer().setName(target);
            }
            //mark captured variable
            else if (mappings.containsValue(var)) {
                c.getRolePlayer().setName("captured->" + var);
            }
            });
    }

    @Override
    public Set<String> getVarNames(){
        Set<String> vars = new HashSet<>();
        castings.forEach(c -> vars.add(c.getRolePlayer().getName()));
        return vars;
    }

    @Override
    public String getVal(){
        throw new IllegalAccessError("getVal() on a relation atom!");
    }

    /**
     * Attempts to infer the implicit roleTypes of vars in a relAtom
     * @return map containing a varName - varType, varRoleType triple
     */
    public Map<String, Pair<Type, RoleType>> getVarTypeRoleMap()
    {
        Map<String, Pair<Type, RoleType>> roleVarTypeMap = new HashMap<>();

        if (getParentQuery() == null) return roleVarTypeMap;

        MindmapsTransaction graph =  getParentQuery().getTransaction();
        String relTypeId = getTypeId();
        Set<String> vars = getVarNames();
        Map<String, Type> varTypeMap = getParentQuery().getVarTypeMap();

        for (String var : vars)
        {
            Type type = varTypeMap.get(var);
            String roleTypeId = "";
            for(Var.Casting c : castings) {
                if (c.getRolePlayer().getName().equals(var))
                    roleTypeId = c.getRoleType().flatMap(VarAdmin::getId).orElse("");
            }
            /**roletype explicit*/
            if (!roleTypeId.isEmpty())
                roleVarTypeMap.put(var, new Pair<>(type, graph.getRoleType(roleTypeId)));
            else {
                if (type != null) {
                    Set<RoleType> cRoles = getCompatibleRoleTypes(type.getId(), relTypeId, getParentQuery().getTransaction());

                    /**if roleType is unambigous*/
                    if (cRoles.size() == 1)
                        roleVarTypeMap.put(var, new Pair<>(type, cRoles.iterator().next()));
                    else
                        roleVarTypeMap.put(var, new Pair<>(type, null));

                }
            }
        }
        return roleVarTypeMap;
    }

    /**
     * Attempts to infer the implicit roleTypes and matching types
     * @return map containing a RoleType-Type pair
     */
    public Map<RoleType, Pair<String, Type>> getRoleVarTypeMap()
    {
        Map<RoleType, Pair<String, Type>> roleVarTypeMap = new HashMap<>();

        if (getParentQuery() == null) return roleVarTypeMap;

        MindmapsTransaction graph =  getParentQuery().getTransaction();
        String relTypeId = getTypeId();
        Set<String> relVars = getVarNames();
        Map<String, Type> varTypeMap = getParentQuery().getVarTypeMap();

        for (String var : relVars)
        {
            Type type = varTypeMap.get(var);
            String roleTypeId = "";
            for(Var.Casting c : castings) {
                if (c.getRolePlayer().getName().equals(var))
                    roleTypeId = c.getRoleType().flatMap(VarAdmin::getId).orElse("");
            }

            /**roletype explicit*/
            if (!roleTypeId.isEmpty())
                roleVarTypeMap.put(graph.getRoleType(roleTypeId), new Pair<>(var, type));
            else {

                if (type != null) {
                    Set<RoleType> cRoles = getCompatibleRoleTypes(type.getId(), relTypeId, graph);

                    /**if roleType is unambigous*/
                    if (cRoles.size() == 1)
                        roleVarTypeMap.put(cRoles.iterator().next(), new Pair<>(var, type));

                }
            }
        }
        return roleVarTypeMap;
    }

    public Set<Atomic> getTypeConstraints(){
        Set<Atomic> typeConstraints = new HashSet<>();
        getParentQuery().getAtoms().forEach(atom ->{
            if (atom.isType() && containsVar(atom.getVarName())) typeConstraints.add(atom);
        });
        return typeConstraints;
    }

    public Set<Atomic> getRelatedAtoms(){
        Set<Atomic> relatedAtoms = new HashSet<>();
        getParentQuery().getAtoms().forEach(atom ->{
            if(atom.isRelation()){
                Set<String> intersection = new HashSet<>(getVarNames());
                intersection.retainAll(atom.getVarNames());
                if (!intersection.isEmpty()) relatedAtoms.add(atom);
            }
        });
        return relatedAtoms;
    }

}
