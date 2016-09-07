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

import io.mindmaps.MindmapsGraph;
import io.mindmaps.constants.ErrorMessage;
import io.mindmaps.core.model.RoleType;
import io.mindmaps.core.model.Type;
import io.mindmaps.graql.Graql;
import io.mindmaps.graql.MatchQuery;
import io.mindmaps.graql.QueryBuilder;
import io.mindmaps.graql.Var;
import io.mindmaps.graql.admin.VarAdmin;
import io.mindmaps.graql.internal.reasoner.container.Query;
import javafx.util.Pair;

import java.util.*;
import java.util.stream.Collectors;

import static io.mindmaps.graql.internal.reasoner.Utility.getCompatibleRoleTypes;

public class Relation extends AtomBase {

    private final Set<Var.Casting> castings = new HashSet<>();

    public Relation(VarAdmin pattern) {
        super(pattern);
        castings.addAll(pattern.getCastings());
    }

    public Relation(VarAdmin pattern, Query par) {
        super(pattern, par);
        castings.addAll(pattern.getCastings());
    }

    public Relation(String id, Map<String, String> roleMap){
        super(constructRelPattern(id, roleMap));
        castings.addAll(getPattern().asVar().getCastings());
    }

    public Relation(Relation a) {
        super(a);
        castings.addAll(a.getPattern().asVar().getCastings());

        expansions.forEach(this::removeExpansion);
        a.expansions.forEach(exp -> expansions.add(new Query(exp)));
    }

    //rolePlayer-roleType
    static private VarAdmin constructRelPattern(String id, Map<String, String> roleMap) {
        Var var = Graql.var().isa(id);
        roleMap.forEach( (player, role) -> var.rel(role, player));
        return var.admin().asVar();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Relation)) return false;
        Relation a2 = (Relation) obj;

        //TODO need to compare roles and roleplayers
        return this.getTypeId().equals(a2.getTypeId()) && this.getVarNames().equals(a2.getVarNames());
    }

    @Override
    public boolean isEquivalent(Object obj) {
        if (!(obj instanceof Relation)) return false;
        Relation a2 = (Relation) obj;
        return this.getTypeId().equals(a2.getTypeId()) && a2.getRoleVarTypeMap().size() == this.getRoleVarTypeMap().size();
    }

    @Override
    public int hashCode() {
        int hashCode = 1;
        hashCode = hashCode * 37 + this.typeId.hashCode();
        hashCode = hashCode * 37 + getVarNames().hashCode();
        return hashCode;
    }

    @Override
    public void print() {
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
    public boolean hasExplicitRoleTypes(){
        boolean rolesDefined = true;
        Iterator<Var.Casting> it = castings.iterator();
        while (it.hasNext() && rolesDefined)
            rolesDefined = it.next().getRoleType().isPresent();
        return rolesDefined;
    }
    @Override
    public boolean containsVar(String name) {
        boolean varFound = false;
        Iterator<Var.Casting> it = castings.iterator();
        while(it.hasNext() && !varFound)
            varFound = it.next().getRolePlayer().getName().equals(name);
        return varFound;
    }

    @Override
    public MatchQuery getExpandedMatchQuery(MindmapsGraph graph) {
        QueryBuilder qb = Graql.withGraph(graph);
        Set<String> selectVars = getVarNames();
        return qb.match(getExpandedPattern()).select(selectVars);
    }

    @Override
    public void changeEachVarName(String from, String to) {
        castings.forEach(c -> {
            String var = c.getRolePlayer().getName();
            if (var.equals(from)) {
                c.getRolePlayer().setName(to);
            }
            else if (var.equals(to)) {
                c.getRolePlayer().setName("captured->" + var);
            }
        });
    }

    @Override
    public void changeEachVarName(Map<String, String> mappings) {
        castings.forEach(c -> {
            String var = c.getRolePlayer().getName();
            if (mappings.containsKey(var) ) {
                String target = mappings.get(var);
                c.getRolePlayer().setName(target);
            }
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
        throw new IllegalAccessError(ErrorMessage.NO_VAL_IN_RELATION.getMessage());
    }

    /**
     * Attempts to infer the implicit roleTypes of vars in a relAtom
     * @return map containing a varName - varType, varRoleType triple
     */
    public Map<String, Pair<Type, RoleType>> getVarTypeRoleMap() {
        Map<String, Pair<Type, RoleType>> roleVarTypeMap = new HashMap<>();
        if (getParentQuery() == null) return roleVarTypeMap;

        MindmapsGraph graph =  getParentQuery().getGraph();
        String relTypeId = getTypeId();
        Set<String> vars = getVarNames();
        Map<String, Type> varTypeMap = getParentQuery().getVarTypeMap();

        for (String var : vars) {
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
                    Set<RoleType> cRoles = getCompatibleRoleTypes(type.getId(), relTypeId, getParentQuery().getGraph());

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
    public Map<RoleType, Pair<String, Type>> getRoleVarTypeMap() {
        Map<RoleType, Pair<String, Type>> roleVarTypeMap = new HashMap<>();

        if (getParentQuery() == null) return roleVarTypeMap;

        MindmapsGraph graph =  getParentQuery().getGraph();
        String relTypeId = getTypeId();
        Set<String> relVars = getVarNames();
        Map<String, Type> varTypeMap = getParentQuery().getVarTypeMap();

        for (String var : relVars) {
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
        Set<Atomic> typeConstraints = getParentQuery().getAtoms();
        return typeConstraints.stream().filter(atom -> atom.isType() && !atom.isResource() && containsVar(atom.getVarName()))
                        .collect(Collectors.toSet());
    }


}
