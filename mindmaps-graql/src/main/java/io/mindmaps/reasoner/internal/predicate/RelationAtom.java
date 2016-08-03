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

package io.mindmaps.reasoner.internal.predicate;

import io.mindmaps.core.dao.MindmapsTransaction;
import io.mindmaps.core.model.RoleType;
import io.mindmaps.core.model.Type;
import io.mindmaps.graql.api.query.*;
import io.mindmaps.reasoner.internal.container.Query;
import org.javatuples.Pair;

import java.util.*;

import static io.mindmaps.reasoner.internal.Utility.getCompatibleRoleTypes;

public class RelationAtom extends AtomBase{

    private final Set<Var.Casting> castings = new HashSet<>();

    public RelationAtom(Var.Admin pattern)
    {
        super(pattern);
        castings.addAll(pattern.getCastings());
    }

    public RelationAtom(Var.Admin pattern, Query par)
    {
        super(pattern, par);
        castings.addAll(pattern.getCastings());
    }

    public RelationAtom(RelationAtom a)
    {
        super(a);
        castings.addAll(a.getPattern().asVar().getCastings());

        for(Query exp : expansions)
            removeExpansion(exp);
        a.expansions.forEach(exp -> expansions.add(new Query(exp)));
    }

    @Override
    public boolean equals(Object obj)
    {
        if (!(obj instanceof RelationAtom)) return false;
        RelationAtom a2 = (RelationAtom) obj;
        return this.getTypeId().equals(a2.getTypeId()) && this.getVarNames().equals(a2.getVarNames());
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
    public MatchQuery getExpandedMatchQuery(MindmapsTransaction graph)
    {
        QueryBuilder qb = QueryBuilder.build(graph);
        Set<String> selectVars = getVarNames();
        return qb.match(getExpandedPattern()).select(selectVars);
    }

    @Override
    public void setVarName(String var){
        throw new IllegalAccessError("setVarName attempted on a relation atom!");
    }

    @Override
    public void changeRelVarName(String from, String to)
    {
        castings.stream().filter(c -> c.getRolePlayer().getName().equals(from))
                .forEach(c -> c.getRolePlayer().setName(to));
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

        String relTypeId = getTypeId();
        Set<String> vars = getVarNames();
        Map<String, Type> varTypeMap = getParentQuery().getVarTypeMap();

        for (String var : vars)
        {
            Type type = varTypeMap.get(var);
            if (type != null)
            {
                Set<RoleType> cRoles = getCompatibleRoleTypes(type.getId(), relTypeId, getParentQuery().getTransaction());

                /**if roleType is unambigous*/
                if(cRoles.size() == 1)
                    roleVarTypeMap.put(var, new Pair<>(type, cRoles.iterator().next()));
                else
                    roleVarTypeMap.put(var, new Pair<>(type, null));

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
                    roleTypeId = c.getRoleType().flatMap(Var.Admin::getId).orElse("");
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
}
