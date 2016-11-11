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

import ai.grakn.GraknGraph;
import ai.grakn.concept.RelationType;
import ai.grakn.graql.Reasoner;
import ai.grakn.graql.internal.reasoner.Utility;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.Rule;
import ai.grakn.concept.Type;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.reasoner.query.Query;
import javafx.util.Pair;

import java.util.*;

public class Relation extends Atom {

    private final Set<VarAdmin.Casting> castings = new HashSet<>();
    private Map<RoleType, Pair<String, Type>> roleVarTypeMap = null;
    private Map<String, Pair<Type, RoleType>> varTypeRoleMap = null;

    public Relation(VarAdmin pattern) {
        super(pattern);
        castings.addAll(pattern.getCastings());
        inferTypeFromRoles();
    }

    public Relation(VarAdmin pattern, Query par) {
        super(pattern, par);
        castings.addAll(pattern.getCastings());
        inferTypeFromRoles();
    }

    public Relation(String id, Map<String, String> roleMap, Query par){
        super(constructRelation(id, roleMap), par);
        castings.addAll(getPattern().asVar().getCastings());
        inferTypeFromRoles();
    }

    private Relation(Relation a) {
        super(a);
        castings.addAll(a.getPattern().asVar().getCastings());
        inferTypeFromRoles();
    }

    @Override
    public Atomic clone(){
        return new Relation(this);
    }

    //rolePlayer-roleType
    public static VarAdmin constructRelation(String id, Map<String, String> roleMap) {
        Var var = Graql.var().isa(id);
        roleMap.forEach( (player, role) -> {
            if (role == null)
                var.rel(player);
            else
                var.rel(role, player);
        });
        return var.admin().asVar();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || this.getClass() != obj.getClass()) return false;
        if (obj == this) return true;
        Relation a2 = (Relation) obj;
        //TODO need to compare roles and roleplayers
        return this.getTypeId().equals(a2.getTypeId()) && this.getVarNames().equals(a2.getVarNames());
    }

    @Override
    public int hashCode() {
        int hashCode = 1;
        hashCode = hashCode * 37 + this.typeId.hashCode();
        hashCode = hashCode * 37 + getVarNames().hashCode();
        return hashCode;
    }

    @Override
    public boolean isEquivalent(Object obj) {
        if (obj == null || this.getClass() != obj.getClass()) return false;
        if (obj == this) return true;
        Relation a2 = (Relation) obj;
        boolean isEquivalent = this.getTypeId().equals(a2.getTypeId());

        //check whether subs correspond to same role players
        Map<RoleType, String> roleConceptMap = getRoleConceptIdMap();
        Map<RoleType, String> childRoleConceptMap = a2.getRoleConceptIdMap();
        Iterator<RoleType> it = roleConceptMap.keySet().iterator();
        while(it.hasNext() && isEquivalent){
            RoleType role = it.next();
            isEquivalent = childRoleConceptMap.containsKey(role) &&
                    childRoleConceptMap.get(role).equals(roleConceptMap.get(role));
        }
        return isEquivalent;
    }

    @Override
    public int equivalenceHashCode(){
        int hashCode = 1;
        hashCode = hashCode * 37 + this.typeId.hashCode();
        hashCode = hashCode * 37 + this.getRoleConceptIdMap().hashCode();
        return hashCode;
    }

    @Override
    public boolean isRelation(){ return true;}

    @Override
    public boolean isRuleResolvable() {
        Type t = getType();
        if (t != null)
            return !t.getRulesOfConclusion().isEmpty();
        else{
            GraknGraph graph = getParentQuery().getGraph().orElse(null);
            Set<Rule> rules = Reasoner.getRules(graph);
            return rules.stream()
                    .flatMap(rule -> rule.getConclusionTypes().stream())
                    .filter(Type::isRelationType).count() != 0;
        }
    }


    public boolean hasExplicitRoleTypes(){
        boolean rolesDefined = false;
        Iterator<VarAdmin.Casting> it = castings.iterator();
        while (it.hasNext() && !rolesDefined)
            rolesDefined = it.next().getRoleType().isPresent();
        return rolesDefined;
    }

    private Set<RoleType> getExplicitRoleTypes(){
        Set<RoleType> roleTypes = new HashSet<>();
        GraknGraph graph = getParentQuery().getGraph().orElse(null);
        castings.stream()
                .filter(c -> c.getRoleType().isPresent())
                .filter(c -> c.getRoleType().get().getId().isPresent())
                .map( c -> graph.getRoleType(c.getRoleType().orElse(null).getId().orElse("")))
                .forEach(roleTypes::add);
        return roleTypes;
    }

    private void inferTypeFromRoles() {
        if (getParentQuery() != null && typeId.isEmpty() && hasExplicitRoleTypes()){
            type = getExplicitRoleTypes().iterator().next().relationType();
            typeId = type.getId();
            atomPattern.admin().asVar().isa(typeId);
        }
    }

    @Override
    public boolean containsVar(String name) {
        boolean varFound = false;
        Iterator<VarAdmin.Casting> it = castings.iterator();
        while(it.hasNext() && !varFound)
            varFound = it.next().getRolePlayer().getName().equals(name);
        return varFound;
    }

    @Override
    public void unify(String from, String to) {
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
    public void unify (Map<String, String> mappings) {
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

    /**
     * Attempts to infer the implicit roleTypes of vars in a relAtom
     * @return map containing a varName - varType, varRoleType triple
     */
    private Map<String, Pair<Type, RoleType>> computeVarTypeRoleMap() {
        Map<String, Pair<Type, RoleType>> roleVarTypeMap = new HashMap<>();
        if (getParentQuery() == null) return roleVarTypeMap;

        GraknGraph graph =  getParentQuery().getGraph().orElse(null);
        Type relType = getType();
        Set<String> vars = getVarNames();
        Map<String, Type> varTypeMap = getParentQuery().getVarTypeMap();

        for (String var : vars) {
            Type type = varTypeMap.get(var);
            String roleTypeId = "";
            for(VarAdmin.Casting c : castings) {
                if (c.getRolePlayer().getName().equals(var))
                    roleTypeId = c.getRoleType().flatMap(VarAdmin::getId).orElse("");
            }
            //roletype explicit
            if (!roleTypeId.isEmpty())
                roleVarTypeMap.put(var, new Pair<>(type, graph.getRoleType(roleTypeId)));
            else {
                if (type != null && relType != null) {
                    Set<RoleType> cRoles = Utility.getCompatibleRoleTypes(type, relType);

                    //if roleType is unambigous
                    if (cRoles.size() == 1)
                        roleVarTypeMap.put(var, new Pair<>(type, cRoles.iterator().next()));
                    else
                        roleVarTypeMap.put(var, new Pair<>(type, null));
                }
            }
        }
        return roleVarTypeMap;
    }

    @Override
    public Map<String, Pair<Type, RoleType>> getVarTypeRoleMap() {
        if (varTypeRoleMap == null)
            varTypeRoleMap = computeVarTypeRoleMap();
        return varTypeRoleMap;
    }

    /**
     * Attempts to infer the implicit roleTypes and matching types based on contents of the parent query
     * @return map containing a RoleType-Type pair
     */
    private Map<RoleType, Pair<String, Type>> computeRoleVarTypeMap() {
        Map<RoleType, Pair<String, Type>> roleVarTypeMap = new HashMap<>();
        if (getParentQuery() == null || getType() == null) return roleVarTypeMap;
        GraknGraph graph =  getParentQuery().getGraph().orElse(null);
        Map<String, Type> varTypeMap = getParentQuery().getVarTypeMap();
        Set<String> allocatedVars = new HashSet<>();
        Set<RoleType> allocatedRoles = new HashSet<>();

        castings.forEach( c -> {
            String var = c.getRolePlayer().getName();
            String roleTypeId = c.getRoleType().flatMap(VarAdmin::getId).orElse("");
            Type type = varTypeMap.get(var);
            if (!roleTypeId.isEmpty()) {
                RoleType role = graph.getRoleType(roleTypeId);
                roleVarTypeMap.put(role, new Pair<>(var, type));
                allocatedVars.add(var);
                allocatedRoles.add(role);
            }
        });

        RelationType relType = (RelationType) getType();
        Set<String> varsToAllocate = getVarNames();
        varsToAllocate.removeAll(allocatedVars);
        for (String var : varsToAllocate) {
            Type type = varTypeMap.get(var);

            if (type != null && relType != null) {
                Set<RoleType> cRoles = Utility.getCompatibleRoleTypes(type, relType);
                //if roleType is unambigous
                if (cRoles.size() == 1) {
                    RoleType role = cRoles.iterator().next();
                    roleVarTypeMap.put(role, new Pair<>(var, type));
                    allocatedVars.add(var);
                    allocatedRoles.add(role);
                }
            }
        }
        Collection<RoleType> rolesToAllocate = relType.hasRoles();
        rolesToAllocate.removeAll(allocatedRoles);
        varsToAllocate.removeAll(allocatedVars);
        if (rolesToAllocate.size() == 1 && varsToAllocate.size() == 1) {
            RoleType role = rolesToAllocate.iterator().next();
            String var = varsToAllocate.iterator().next();
            Type type = varTypeMap.get(var);
            roleVarTypeMap.put(role, new Pair<>(var, type));
        }

        //update pattern and castings
        Map<String, String> roleMap = new HashMap<>();
        roleVarTypeMap.forEach( (r, tp) -> roleMap.put(tp.getKey(), r.getId()));
        getVarNames().stream()
                    .filter(var -> !roleMap.containsKey(var))
                    .forEach( var -> roleMap.put(var, null));
        atomPattern = constructRelation(typeId, roleMap);
        castings.addAll(getPattern().asVar().getCastings());

        return roleVarTypeMap;
    }

    @Override
    public Map<RoleType, Pair<String, Type>> getRoleVarTypeMap() {
        if (roleVarTypeMap == null) {
            if (varTypeRoleMap != null) {
                roleVarTypeMap = new HashMap<>();
                varTypeRoleMap.forEach((var, tpair) -> {
                    RoleType rt = tpair.getValue();
                    if (rt != null)
                        roleVarTypeMap.put(rt, new Pair<>(var, tpair.getKey()));
                });
            } else
                roleVarTypeMap = computeRoleVarTypeMap();
        }
        return roleVarTypeMap;
    }
}
