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
import ai.grakn.concept.RoleType;
import ai.grakn.concept.Rule;
import ai.grakn.concept.Type;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Reasoner;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.RelationPlayer;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.pattern.property.IsaProperty;
import ai.grakn.graql.internal.pattern.property.RelationProperty;
import ai.grakn.graql.internal.reasoner.Utility;
import ai.grakn.graql.internal.reasoner.query.Query;
import ai.grakn.graql.internal.reasoner.rule.InferenceRule;
import ai.grakn.util.ErrorMessage;
import java.util.UUID;
import javafx.util.Pair;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static ai.grakn.graql.internal.reasoner.Utility.checkTypesCompatible;

public class Relation extends TypeAtom{

    private Set<RelationPlayer> relationPlayers;
    private Map<RoleType, Pair<String, Type>> roleVarTypeMap = null;
    private Map<String, Pair<Type, RoleType>> varTypeRoleMap = null;

    public Relation(VarAdmin pattern) { this(pattern, null, null);}
    public Relation(VarAdmin pattern, Query par) { this(pattern, null, par);}
    public Relation(VarAdmin pattern, Predicate predicate, Query par) {
        super(pattern, predicate, par);
        relationPlayers = getRelationPlayers(pattern);
        inferTypeFromRoles();
    }

    public Relation(String name, String typeVariable, Map<String, String> roleMap, Predicate pred, Query par){
        super(constructRelationVar(name, typeVariable, roleMap), pred, par);
        relationPlayers = getRelationPlayers(getPattern().asVar());
        inferTypeFromRoles();
    }

    private Relation(Relation a) {
        super(a);
        relationPlayers = getRelationPlayers(a.getPattern().asVar());
        inferTypeFromRoles();
    }

    private Set<RelationPlayer> getRelationPlayers(){return getRelationPlayers(this.atomPattern.asVar());}
    private Set<RelationPlayer> getRelationPlayers(VarAdmin pattern) {
        Set<RelationPlayer> rps = new HashSet<>();
        pattern.getProperty(RelationProperty.class)
                .ifPresent(prop -> prop.getRelationPlayers().forEach(rps::add));
        return rps;
    }

    @Override
    protected String extractValueVariableName(VarAdmin var) {
        IsaProperty isaProp = var.getProperty(IsaProperty.class).orElse(null);
        return isaProp != null? isaProp.getType().getName() : "";
    }

    @Override
    protected void setValueVariable(String var) {
        IsaProperty isaProp = atomPattern.asVar().getProperty(IsaProperty.class).orElse(null);
        if (isaProp != null) {
            super.setValueVariable(var);
            atomPattern.asVar().getProperties(IsaProperty.class).forEach(prop -> prop.getType().setName(var));
        }
    }

    @Override
    public Atomic clone(){ return new Relation(this);}

    //rolePlayer-roleType
    public static VarAdmin constructRelationVar(String name, String typeVariable, Map<String, String> roleMap) {
        Var var;
        if (name != null && !name.isEmpty())
            var = Graql.var(name);
        else
            var = Graql.var();
        var.isa(Graql.var(typeVariable));
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
        return this.getTypeId().equals(a2.getTypeId())
                && this.getVarNames().equals(a2.getVarNames())
                && relationPlayers.equals(a2.relationPlayers);
    }

    @Override
    public int hashCode() {
        int hashCode = 1;
        hashCode = hashCode * 37 + getTypeId().hashCode();
        hashCode = hashCode * 37 + getVarNames().hashCode();
        return hashCode;
    }

    @Override
    public boolean isEquivalent(Object obj) {
        if (obj == null || this.getClass() != obj.getClass()) return false;
        if (obj == this) return true;
        Relation a2 = (Relation) obj;
        Map<RoleType, String> map = getRoleConceptIdMap();
        Map<RoleType, String> map2 = a2.getRoleConceptIdMap();
        return this.getTypeId().equals(a2.getTypeId()) && map.equals(map2);
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
    public boolean isSelectable(){ return true;}

    @Override
    protected boolean isRuleApplicable(InferenceRule child) {
        Atom ruleAtom = child.getRuleConclusionAtom();
        if(!(ruleAtom instanceof Relation)) return false;

        Relation childAtom = (Relation) ruleAtom;
        //discard if child has less rolePlayers
        if (childAtom.getRelationPlayers().size() < this.getRelationPlayers().size()) return false;

        boolean ruleRelevant = true;
        Query parent = getParentQuery();
        Map<RoleType, Pair<String, Type>> childRoleVarTypeMap = childAtom.getRoleVarTypeMap();
        Map<RoleType, Pair<String, Type>> parentRoleVarTypeMap = getRoleVarTypeMap();

        Iterator<Map.Entry<RoleType, Pair<String, Type>>> it = parentRoleVarTypeMap.entrySet().iterator();
        while(it.hasNext() && ruleRelevant){
            Map.Entry<RoleType, Pair<String, Type>> entry = it.next();
            RoleType parentRole = entry.getKey();

            //check roletypes compatible
            Iterator<RoleType> childRolesIt = childRoleVarTypeMap.keySet().iterator();
            //if child roles are unspecified then compatible
            boolean roleCompatible = !childRolesIt.hasNext();
            while(childRolesIt.hasNext() && !roleCompatible){
                roleCompatible = checkTypesCompatible(parentRole, childRolesIt.next());
            }
            ruleRelevant = roleCompatible;

            //check type compatibility
            Type pType = entry.getValue().getValue();
            if (pType != null && ruleRelevant) {
                //vars can be matched by role types
                if (childRoleVarTypeMap.containsKey(parentRole)) {
                    Type chType = childRoleVarTypeMap.get(parentRole).getValue();
                    //check type compatibility
                    if (chType != null) {
                        ruleRelevant = checkTypesCompatible(pType, chType);

                        //Check for any constraints on the variables
                        String chVar = childRoleVarTypeMap.get(parentRole).getKey();
                        String pVar = entry.getValue().getKey();
                        Predicate childPredicate = child.getBody().getIdPredicate(chVar);
                        Predicate parentPredicate = parent.getIdPredicate(pVar);
                        if (childPredicate != null && parentPredicate != null)
                            ruleRelevant &= childPredicate.getPredicateValue().equals(parentPredicate.getPredicateValue());
                    }
                }
            }
        }
        return ruleRelevant;
    }

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
        Iterator<RelationPlayer> it = relationPlayers.iterator();
        while (it.hasNext() && !rolesDefined)
            rolesDefined = it.next().getRoleType().isPresent();
        return rolesDefined;
    }

    private Set<RoleType> getExplicitRoleTypes(){
        Set<RoleType> roleTypes = new HashSet<>();
        GraknGraph graph = getParentQuery().getGraph().orElse(null);
        relationPlayers.stream()
                .filter(c -> c.getRoleType().isPresent())
                .filter(c -> c.getRoleType().get().getId().isPresent())
                .map( c -> graph.getRoleType(c.getRoleType().orElse(null).getId().orElse("")))
                .forEach(roleTypes::add);
        return roleTypes;
    }

    private void addPredicate(Predicate pred){
        if (getParentQuery() == null) throw new IllegalStateException("No parent in addPredicate");
        Query parent = getParentQuery();
        pred.setParentQuery(parent);
        setPredicate(pred);
        getParentQuery().addAtom(pred);
    }

    private void inferTypeFromRoles() {
        if (getParentQuery() != null && getTypeId().isEmpty() && hasExplicitRoleTypes()){
            type = getExplicitRoleTypes().iterator().next().relationType();
            typeId = type.getId();
            String typeVariable = "rel-" + UUID.randomUUID().toString();
            addPredicate(new IdPredicate(Graql.var(typeVariable).id(typeId).admin()));
            atomPattern = atomPattern.asVar().isa(Graql.var(typeVariable)).admin();
        }
    }

    @Override
    public boolean containsVar(String name) {
        boolean varFound = false;
        Iterator<RelationPlayer> it = relationPlayers.iterator();
        while(it.hasNext() && !varFound)
            varFound = it.next().getRolePlayer().getVarName().equals(name);
        return varFound;
    }

    @Override
    public Set<Predicate> getIdPredicates() {
        Set<Predicate> idPredicates = super.getIdPredicates();
        //from types
        getTypeConstraints()
                .forEach(atom -> {
                    Predicate predicate = getParentQuery().getIdPredicate(atom.getValueVariable());
                    if (predicate != null) idPredicates.add(predicate);
                });
        return idPredicates;
    }

    @Override
    public void unify(String from, String to) {
        super.unify(from, to);
        relationPlayers.forEach(c -> {
            String var = c.getRolePlayer().getVarName();
            if (var.equals(from)) {
                c.getRolePlayer().setVarName(to);
            }
            else if (var.equals(to)) {
                c.getRolePlayer().setVarName("captured->" + var);
            }
        });
    }

    @Override
    public void unify (Map<String, String> mappings) {
        super.unify(mappings);
        relationPlayers.forEach(c -> {
            String var = c.getRolePlayer().getVarName();
            if (mappings.containsKey(var) ) {
                String target = mappings.get(var);
                c.getRolePlayer().setVarName(target);
            }
            else if (mappings.containsValue(var)) {
                c.getRolePlayer().setVarName("captured->" + var);
            }
        });
    }

    @Override
    public Set<String> getVarNames(){
        Set<String> vars = super.getVarNames();
        vars.addAll(getRolePlayers());
        return vars;
    }

    @Override
    public Set<String> getSelectedNames(){
        Set<String> vars = super.getSelectedNames();
        vars.addAll(getRolePlayers());
        return vars;
    }
    public Set<String> getRolePlayers(){
        Set<String> vars = new HashSet<>();
        relationPlayers.forEach(c -> vars.add(c.getRolePlayer().getVarName()));
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
        Set<String> vars = getRolePlayers();
        Map<String, Type> varTypeMap = getParentQuery().getVarTypeMap();

        for (String var : vars) {
            Type type = varTypeMap.get(var);
            String roleTypeId = "";
            for(RelationPlayer c : relationPlayers) {
                if (c.getRolePlayer().getVarName().equals(var))
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

        //explicit role types from castings
        relationPlayers.forEach(c -> {
            String var = c.getRolePlayer().getVarName();
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
        Set<String> varsToAllocate = getRolePlayers();
        varsToAllocate.removeAll(allocatedVars);
        varsToAllocate.forEach(var -> {
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
        });

        Collection<RoleType> rolesToAllocate = relType.hasRoles();
        //remove sub and super roles of allocated roles
        allocatedRoles.forEach(role -> {
            rolesToAllocate.removeAll(role.subTypes());
            if (role.superType() != null) rolesToAllocate.remove(role.superType());
            rolesToAllocate.remove(role);
        });
        varsToAllocate.removeAll(allocatedVars);
        if (varsToAllocate.size() == 1) {
            RoleType role = rolesToAllocate.iterator().next();
            RoleType allocatedRole = role.superType() != null? role.superType() : role;
            String var = varsToAllocate.iterator().next();
            Type type = varTypeMap.get(var);
            roleVarTypeMap.put(allocatedRole, new Pair<>(var, type));
        }

        //update pattern and castings
        Map<String, String> roleMap = new HashMap<>();
        roleVarTypeMap.forEach( (r, tp) -> roleMap.put(tp.getKey(), r.getId()));
        getRolePlayers().stream()
                .filter(var -> !var.equals(getVarName()))
                .filter(var -> !roleMap.containsKey(var))
                .forEach( var -> roleMap.put(var, null));

        //pattern mutation!
        atomPattern = constructRelationVar(isUserDefinedName()? varName : "", getValueVariable(), roleMap);
        relationPlayers = getRelationPlayers(getPattern().asVar());

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

    @Override
    public Map<String, String> getUnifiers(Atomic pat) {
        if (pat.getClass() != this.getClass()) {
            System.out.println("this: " + this.toString());
            throw new IllegalArgumentException(ErrorMessage.UNIFICATION_ATOM_INCOMPATIBILITY.getMessage());
        }

        Map<String, String> unifiers = super.getUnifiers(pat);
        Relation parentAtom = (Relation) pat;
        Set<String> varsToAllocate = parentAtom.getRolePlayers();
        Set<String> childBVs = getRolePlayers();
        Map<String, Pair<Type, RoleType>> childMap = getVarTypeRoleMap();
        Map<RoleType, Pair<String, Type>> parentMap = parentAtom.getRoleVarTypeMap();

        //find child->parent var mappings based on roles
        childBVs.forEach(chVar -> {
            if(!varsToAllocate.isEmpty()) {
                RoleType role = childMap.containsKey(chVar) ? childMap.get(chVar).getValue() : null;
                //map to empty if no var matching
                String pVar = role != null && parentMap.containsKey(role) ? parentMap.get(role).getKey() : "";
                if (pVar.isEmpty())
                    pVar = varsToAllocate.iterator().next();
                if (!chVar.equals(pVar)) unifiers.put(chVar, pVar);
                varsToAllocate.remove(pVar);
            }
        });

        return unifiers;
    }

    private Map<String, Predicate> getVarSubMap() {
        Map<String, Predicate> map = new HashMap<>();
        getPredicates().stream()
            .filter(Predicate::isIdPredicate)
            .forEach( sub -> {
                String var = sub.getVarName();
                map.put(var, sub);
        });
        return map;
    }

    private Map<RoleType, String> getRoleConceptIdMap(){
        Map<RoleType, String> roleConceptMap = new HashMap<>();
        Map<String, Predicate> varSubMap = getVarSubMap();
        Map<RoleType, Pair<String, Type>> roleVarMap = getRoleVarTypeMap();

        roleVarMap.forEach( (role, varTypePair) -> {
            String var = varTypePair.getKey();
            roleConceptMap.put(role, varSubMap.containsKey(var) ? varSubMap.get(var).getPredicateValue() : "");
        });
        return roleConceptMap;
    }
}
