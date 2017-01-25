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
package ai.grakn.graql.internal.reasoner.atom.binary;

import ai.grakn.GraknGraph;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.Rule;
import ai.grakn.concept.Type;
import ai.grakn.concept.TypeName;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Var;
import ai.grakn.graql.VarName;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.RelationPlayer;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.pattern.property.IsaProperty;
import ai.grakn.graql.internal.pattern.property.RelationProperty;
import ai.grakn.graql.internal.reasoner.Reasoner;
import ai.grakn.graql.internal.reasoner.Utility;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;
import ai.grakn.graql.internal.reasoner.atom.predicate.Predicate;
import ai.grakn.graql.internal.reasoner.query.ReasonerAtomicQuery;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueryImpl;
import ai.grakn.graql.internal.reasoner.rule.InferenceRule;
import ai.grakn.graql.internal.util.CommonUtil;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.Schema;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.List;
import javafx.util.Pair;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static ai.grakn.graql.internal.reasoner.Utility.capture;
import static ai.grakn.graql.internal.reasoner.Utility.checkTypesCompatible;
import static ai.grakn.graql.internal.reasoner.Utility.getCompatibleRelationTypes;
import static ai.grakn.graql.internal.reasoner.Utility.getNonMetaTopRole;
import static ai.grakn.graql.internal.reasoner.Utility.roleToRelationTypes;
import static ai.grakn.graql.internal.reasoner.Utility.typeToRelationTypes;


/**
 *
 * <p>
 * Atom implementation defining a relation atom.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public class Relation extends TypeAtom {

    private Map<RoleType, Pair<VarName, Type>> roleVarTypeMap = null;

    public Relation(VarAdmin pattern, IdPredicate predicate, ReasonerQuery par) {
        super(pattern, predicate, par);
    }

    public Relation(VarName name, VarName typeVariable, Map<VarName, Var> roleMap, IdPredicate pred, ReasonerQuery par) {
        super(constructRelationVar(name, typeVariable, roleMap), pred, par);
    }

    private Relation(Relation a) {
        super(a);
    }

    private Set<RelationPlayer> getRelationPlayers() {
        Set<RelationPlayer> rps = new HashSet<>();
        this.atomPattern.asVar().getProperty(RelationProperty.class)
                .ifPresent(prop -> prop.getRelationPlayers().forEach(rps::add));
        return rps;
    }

    @Override
    protected VarName extractValueVariableName(VarAdmin var) {
        IsaProperty isaProp = var.getProperty(IsaProperty.class).orElse(null);
        return isaProp != null ? isaProp.getType().getVarName() : VarName.of("");
    }

    @Override
    protected void setValueVariable(VarName var) {
        IsaProperty isaProp = atomPattern.asVar().getProperty(IsaProperty.class).orElse(null);
        if (isaProp != null) {
            super.setValueVariable(var);
            atomPattern.asVar().getProperties(IsaProperty.class).forEach(prop -> prop.getType().setVarName(var));
        }
    }

    @Override
    public Atomic copy() {
        return new Relation(this);
    }


    private static VarAdmin constructRelationVar(VarName varName, VarName typeVariable, Map<VarName, Var> roleMap) {
        return constructRelationVar(varName, typeVariable, roleMap.entrySet().stream().map(e -> new Pair<>(e.getKey(), e.getValue())).collect(Collectors.toList()));
    }

    /**
     * construct a $varName (rolemap) isa $typeVariable relation
     * @param varName variable name
     * @param typeVariable type variable name
     * @param rolePlayerMappings  list of rolePlayer-roleType mappings
     * @return corresponding Var
     */
    private static VarAdmin constructRelationVar(VarName varName, VarName typeVariable, List<Pair<VarName, Var>> rolePlayerMappings) {
        Var var;
        if (!varName.getValue().isEmpty()) var = Graql.var(varName);
        else var = Graql.var();
        rolePlayerMappings.forEach(mapping -> {
            VarName rp = mapping.getKey();
            Var role = mapping.getValue();
            if (role == null) var.rel(Graql.var(rp));
            else var.rel(role, Graql.var(rp));
        });
        var.isa(Graql.var(typeVariable));
        return var.admin().asVar();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || this.getClass() != obj.getClass()) return false;
        if (obj == this) return true;
        Relation a2 = (Relation) obj;
        return Objects.equals(this.typeId, a2.getTypeId())
                && this.getVarNames().equals(a2.getVarNames())
                && getRelationPlayers().equals(a2.getRelationPlayers());
    }

    @Override
    public int hashCode() {
        int hashCode = 1;
        hashCode = hashCode * 37 + (getTypeId() != null? getTypeId().hashCode() : 0);
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
        return Objects.equals(this.typeId, a2.getTypeId())
                && map.equals(map2);
    }

    @Override
    public int equivalenceHashCode() {
        int hashCode = 1;
        hashCode = hashCode * 37 + (this.typeId != null? this.typeId.hashCode() : 0);
        hashCode = hashCode * 37 + this.getRoleConceptIdMap().hashCode();
        return hashCode;
    }

    @Override
    public boolean isRelation() {
        return true;
    }

    @Override
    public boolean isSelectable() {
        return true;
    }

    private boolean isRuleApplicableViaType(Atom childAtom) {
        boolean ruleRelevant = true;
        Map<VarName, Type> varTypeMap = getParentQuery().getVarTypeMap();
        Iterator<Type> it = varTypeMap.entrySet().stream()
                .filter(entry -> containsVar(entry.getKey()))
                .map(Map.Entry::getValue)
                .filter(Objects::nonNull)
                .iterator();
        Set<RoleType> roles = childAtom.getRoleVarTypeMap().keySet();
        while (it.hasNext() && ruleRelevant) {
            Type type = it.next();
            if (!Schema.MetaSchema.isMetaName(type.getName())) {
                Set<RoleType> roleIntersection = new HashSet<>(roles);
                roleIntersection.retainAll(type.playsRoles());
                ruleRelevant = !roleIntersection.isEmpty();
            }
        }
        return ruleRelevant;
    }

    private boolean isRuleApplicableViaAtom(Atom childAtom, InferenceRule child) {
        boolean ruleRelevant = true;
        ReasonerQueryImpl parent = (ReasonerQueryImpl) getParentQuery();
        Map<RoleType, Pair<VarName, Type>> childRoleVarTypeMap = childAtom.getRoleVarTypeMap();
        Map<RoleType, Pair<VarName, Type>> parentRoleVarTypeMap = getRoleVarTypeMap();

        Iterator<Map.Entry<RoleType, Pair<VarName, Type>>> it = parentRoleVarTypeMap.entrySet().iterator();
        while (it.hasNext() && ruleRelevant) {
            Map.Entry<RoleType, Pair<VarName, Type>> entry = it.next();
            RoleType parentRole = entry.getKey();

            //check roletypes compatible
            Iterator<RoleType> childRolesIt = childRoleVarTypeMap.keySet().iterator();
            //if child roles are unspecified then compatible
            boolean roleCompatible = !childRolesIt.hasNext();
            while (childRolesIt.hasNext() && !roleCompatible) {
                roleCompatible = checkTypesCompatible(parentRole, childRolesIt.next());
            }
            ruleRelevant = roleCompatible;

            //check type compatibility
            Type pType = entry.getValue().getValue();
            //vars can be matched by role types
            if (pType != null && ruleRelevant && childRoleVarTypeMap.containsKey(parentRole)) {
                Type chType = childRoleVarTypeMap.get(parentRole).getValue();
                //check type compatibility
                if (chType != null) {
                    ruleRelevant = checkTypesCompatible(pType, chType);

                    //Check for any constraints on the variables
                    VarName chVar = childRoleVarTypeMap.get(parentRole).getKey();
                    VarName pVar = entry.getValue().getKey();
                    Predicate childPredicate = child.getBody().getIdPredicate(chVar);
                    Predicate parentPredicate = parent.getIdPredicate(pVar);
                    if (childPredicate != null && parentPredicate != null) {
                        ruleRelevant &= childPredicate.getPredicateValue().equals(parentPredicate.getPredicateValue());
                    }
                }
            }
        }
        return ruleRelevant;
    }

    @Override
    protected boolean isRuleApplicable(InferenceRule child) {
        Atom ruleAtom = child.getRuleConclusionAtom();
        if (!(ruleAtom instanceof Relation)) return false;

        Relation childAtom = (Relation) ruleAtom;
        //discard if child has less rolePlayers
        if (childAtom.getRelationPlayers().size() < this.getRelationPlayers().size()) return false;

        Type type = getType();
        //Case: relation without type - match all
        if (type == null) {
            return isRuleApplicableViaType(childAtom);
        } else {
            return isRuleApplicableViaAtom(childAtom, child);
        }
    }

    @Override
    public boolean isRuleResolvable() {
        Type t = getType();
        if (t != null) {
            return !t.getRulesOfConclusion().isEmpty()
                    && !this.getApplicableRules().isEmpty();
        } else {
            GraknGraph graph = getParentQuery().graph();
            Set<Rule> rules = Reasoner.getRules(graph);
            return rules.stream()
                    .flatMap(rule -> rule.getConclusionTypes().stream())
                    .filter(Type::isRelationType).count() != 0
                    && !this.getApplicableRules().isEmpty();
        }
    }

    private Set<RoleType> getExplicitRoleTypes() {
        Set<RoleType> roleTypes = new HashSet<>();
        GraknGraph graph = getParentQuery().graph();
        getRelationPlayers().stream()
                .map(RelationPlayer::getRoleType)
                .flatMap(CommonUtil::optionalToStream)
                .map(VarAdmin::getTypeName)
                .flatMap(CommonUtil::optionalToStream)
                .map(graph::<RoleType>getType)
                .forEach(roleTypes::add);
        return roleTypes;
    }

    public Relation addType(Type type) {
        typeId = type.getId();
        VarName typeVariable = getValueVariable().getValue().isEmpty()?
                VarName.of("rel-" + UUID.randomUUID().toString()) : getValueVariable();
        setPredicate(new IdPredicate(Graql.var(typeVariable).id(typeId).admin(), getParentQuery()));
        atomPattern = atomPattern.asVar().isa(Graql.var(typeVariable)).admin();
        setValueVariable(typeVariable);
        return this;
    }

    private void inferTypeFromRoles() {
        //look at available roles
        RelationType type = null;
        Set<RelationType> compatibleTypes = getCompatibleRelationTypes(getExplicitRoleTypes(), roleToRelationTypes);
        if (compatibleTypes.size() == 1) type = compatibleTypes.iterator().next();

        //look at types
        if (type == null) {
            Map<VarName, Type> varTypeMap = getParentQuery().getVarTypeMap();
            Set<Type> types = getRolePlayers().stream()
                    .filter(varTypeMap::containsKey)
                    .map(varTypeMap::get)
                    .collect(Collectors.toSet());

            Set<RelationType> compatibleTypesFromTypes = getCompatibleRelationTypes(types, typeToRelationTypes);
            if (compatibleTypesFromTypes.size() == 1) type = compatibleTypesFromTypes.iterator().next();
            else {
                compatibleTypesFromTypes.retainAll(compatibleTypes);
                if (compatibleTypesFromTypes.size() == 1) type = compatibleTypesFromTypes.iterator().next();
            }
        }
        if (type != null) addType(type);
    }

    private void inferTypeFromHasRole(){
        ReasonerQueryImpl parent = (ReasonerQueryImpl) getParentQuery();
        VarName valueVariable = getValueVariable();
        TypeAtom hrAtom = parent.getAtoms().stream()
                .filter(at -> at.getVarName().equals(valueVariable))
                .filter(Atomic::isAtom).map(at -> (Atom) at)
                .filter(Atom::isType).map(at -> (TypeAtom) at)
                .findFirst().orElse(null);
        if (hrAtom != null) {
            ReasonerAtomicQuery hrQuery = new ReasonerAtomicQuery(hrAtom);
            hrQuery.DBlookup();
            if (hrQuery.getAnswers().size() == 1) {
                IdPredicate newPredicate = new IdPredicate(IdPredicate.createIdVar(hrAtom.getVarName(),
                        hrQuery.getAnswers().stream().findFirst().orElse(null).get(hrAtom.getVarName()).getId()), parent);

                Relation newRelation = new Relation(getPattern().asVar(), newPredicate, parent);
                parent.removeAtom(hrAtom.getPredicate());
                parent.removeAtom(hrAtom);
                parent.removeAtom(this);
                parent.addAtom(newRelation);
                parent.addAtom(newPredicate);
            }
        }
    }

    @Override
    public void inferTypes(){
        if (getPredicate() == null) inferTypeFromRoles();
        if (getPredicate() == null) inferTypeFromHasRole();
    }

    @Override
    public boolean containsVar(VarName name) {
        boolean varFound = false;
        Iterator<RelationPlayer> it = getRelationPlayers().iterator();
        while(it.hasNext() && !varFound) {
            varFound = it.next().getRolePlayer().getVarName().equals(name);
        }
        return varFound;
    }

    @Override
    public void unify (Map<VarName, VarName> mappings) {
        super.unify(mappings);
        getRelationPlayers().forEach(c -> {
            VarName var = c.getRolePlayer().getVarName();
            if (mappings.containsKey(var) ) {
                VarName target = mappings.get(var);
                c.getRolePlayer().setVarName(target);
            }
            else if (mappings.containsValue(var)) {
                c.getRolePlayer().setVarName(capture(var));
            }
        });
    }

    @Override
    public Set<VarName> getVarNames(){
        Set<VarName> vars = super.getVarNames();
        vars.addAll(getRolePlayers());
        //add user specified role type vars
        getRelationPlayers().stream()
                .map(RelationPlayer::getRoleType)
                .flatMap(CommonUtil::optionalToStream)
                .filter(VarAdmin::isUserDefinedName)
                .forEach(r -> vars.add(r.getVarName()));
        return vars;
    }

    /**
     * @return set constituting the role player var names
     */
    public Set<VarName> getRolePlayers(){
        Set<VarName> vars = new HashSet<>();
        getRelationPlayers().forEach(c -> vars.add(c.getRolePlayer().getVarName()));
        return vars;
    }

    private Set<VarName> getMappedRolePlayers() {
        return getRoleVarTypeMap().values().stream().map(Pair::getKey).collect(Collectors.toSet());
    }

    /**
     *
     * @return set constituting the role player var names that do not have a specified role type
     */
    public Set<VarName> getUnmappedRolePlayers() {
        Set<VarName> unmappedVars = getRolePlayers();
        unmappedVars.removeAll(getMappedRolePlayers());
        return unmappedVars;
    }

    /**
     * Attempts to infer the implicit roleTypes and matching types based on contents of the parent query
     * @return map containing roleType - (rolePlayer var - rolePlayer type) pairs
     */
    private Map<RoleType, Pair<VarName, Type>> computeRoleVarTypeMap() {
        this.roleVarTypeMap = new HashMap<>();
        Map<Var, Pair<VarName, Type>> roleVarMap = new HashMap<>();
        if (getParentQuery() == null || getType() == null){ return roleVarTypeMap;}

        GraknGraph graph =  getParentQuery().graph();
        Map<VarName, Type> varTypeMap = getParentQuery().getVarTypeMap();
        Set<RelationPlayer> allocatedRelationPlayers = new HashSet<>();
        Set<RoleType> allocatedRoles = new HashSet<>();

        //explicit role types from castings
        getRelationPlayers().forEach(c -> {
            VarName var = c.getRolePlayer().getVarName();
            VarAdmin role = c.getRoleType().orElse(null);
            if (role != null) {
                Type type = varTypeMap.get(var);
                roleVarMap.put(role, new Pair<>(var, type));
                //try directly
                TypeName typeName = role.getTypeName().orElse(null);
                RoleType roleType = typeName != null ? graph.getType(typeName): null;
                //try indirectly
                if (roleType == null && role.isUserDefinedName()) {
                    IdPredicate rolePredicate = ((ReasonerQueryImpl) getParentQuery()).getIdPredicate(role.getVarName());
                    if (rolePredicate != null) roleType = graph.getConcept(rolePredicate.getPredicate());
                }
                allocatedRelationPlayers.add(c);
                if (roleType != null) {
                    allocatedRoles.add(roleType);
                    roleVarTypeMap.put(roleType, new Pair<>(var, type));
                }
            }
        });

        //remaining roles
        RelationType relType = (RelationType) getType();
        Set<RelationPlayer> relationPlayersToAllocate = getRelationPlayers();
        relationPlayersToAllocate.removeAll(allocatedRelationPlayers);
        relationPlayersToAllocate.forEach(casting -> {
            VarName varName = casting.getRolePlayer().getVarName();
            Type type = varTypeMap.get(varName);
            if (type != null && relType != null) {
                Set<RoleType> cRoles = Utility.getCompatibleRoleTypes(type, relType);
                //if roleType is unambiguous
                if (cRoles.size() == 1) {
                    RoleType roleType = cRoles.iterator().next();
                    VarAdmin roleVar = Graql.var().name(roleType.getName()).admin();
                    roleVarMap.put(roleVar, new Pair<>(varName, type));
                    allocatedRelationPlayers.add(casting);
                    allocatedRoles.add(roleType);
                    roleVarTypeMap.put(roleType, new Pair<>(varName, type));
                }
            }
        });

        Collection<RoleType> rolesToAllocate = relType.hasRoles();
        //remove sub and super roles of allocated roles
        allocatedRoles.forEach(role -> {
            RoleType topRole = getNonMetaTopRole(role);
            rolesToAllocate.removeAll(topRole.subTypes());
        });
        relationPlayersToAllocate.removeAll(allocatedRelationPlayers);
        //if unambiguous assign top role
        if (relationPlayersToAllocate.size() == 1 && !rolesToAllocate.isEmpty()) {
            RoleType topRole = getNonMetaTopRole(rolesToAllocate.iterator().next());
            RelationPlayer casting = relationPlayersToAllocate.iterator().next();
            VarName varName = casting.getRolePlayer().getVarName();
            Type type = varTypeMap.get(varName);
            roleVarTypeMap.put(topRole, new Pair<>(varName, type));
            roleVarMap.put(Graql.var().name(topRole.getName()).admin(), new Pair<>(varName, type));
            relationPlayersToAllocate.remove(casting);
        }

        //update pattern and castings
        List<Pair<VarName, Var>> rolePlayerMappings = new ArrayList<>();
        roleVarMap.forEach( (r, tp) -> rolePlayerMappings.add(new Pair<>(tp.getKey(), r)));
        relationPlayersToAllocate.forEach(casting -> rolePlayerMappings.add(new Pair<>(casting.getRolePlayer().getVarName(), null)));

        //pattern mutation!
        atomPattern = constructRelationVar(isUserDefinedName()? varName : VarName.of(""), getValueVariable(), rolePlayerMappings);
        return roleVarTypeMap;
    }

    @Override
    public Map<RoleType, Pair<VarName, Type>> getRoleVarTypeMap() {
        if (roleVarTypeMap == null) computeRoleVarTypeMap();
        return roleVarTypeMap;
    }

    private Map<RoleType, VarName> getRoleMap(){
        return getRoleVarTypeMap().entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getKey()));
    }

    /**
     * @return map of role variable - role type from a predicate
     */
    private Map<RoleType, VarName> getIndirectRoleMap(){
        GraknGraph graph =  getParentQuery().graph();
        return getRelationPlayers().stream()
                .map(RelationPlayer::getRoleType)
                .flatMap(CommonUtil::optionalToStream)
                .map(rt -> new AbstractMap.SimpleEntry<>(rt, ((ReasonerQueryImpl) getParentQuery()).getIdPredicate(rt.getVarName())))
                .filter(e -> e.getValue() != null)
                .collect(Collectors.toMap(e -> graph.getConcept(e.getValue().getPredicate()), e -> e.getKey().getVarName()));
    }

    //varsToAllocate <= childBVs
    private Map<VarName, VarName> getUnifiers(Map<RoleType, VarName> childMap, Map<RoleType, VarName> parentMap,
                                              List<VarName> varsToMap, List<VarName> varsToAllocate){
        Map<VarName, VarName> unifiers = new HashMap<>();
        List<VarName> allocatedVars = new ArrayList<>();

        childMap.forEach( (role, chVar) -> {
            if(!varsToAllocate.isEmpty()) {
                //map to empty if no var matching
                VarName pVar = VarName.of("");
                //go up in role hierarchy to find matching parent variable name
                while(role != null && pVar.getValue().isEmpty()
                        && !Schema.MetaSchema.isMetaName(role.getName())) {
                    pVar = parentMap.getOrDefault(role, VarName.of(""));
                    role = role.superType();
                }
                if (!pVar.getValue().isEmpty() && !chVar.equals(pVar)){
                    unifiers.put(chVar, pVar);
                    allocatedVars.add(chVar);
                    varsToAllocate.remove(pVar);
                }
            }
        });

        //assign unallocated vars
        varsToMap.removeAll(allocatedVars);
        Iterator<VarName> cit = varsToMap.iterator();
        Iterator<VarName> pit = varsToAllocate.iterator();
        while(pit.hasNext() && cit.hasNext()) unifiers.put(cit.next(), pit.next());
        return unifiers;
    }


    private Map<VarName, VarName> getRoleTypeUnifiers(Relation parentAtom){
        Map<RoleType, VarName> childMap = getIndirectRoleMap();
        Map<RoleType, VarName> parentMap =  parentAtom.getIndirectRoleMap();
        return getUnifiers(childMap,
                parentMap,
                Lists.newArrayList(childMap.values()),
                Lists.newArrayList(parentMap.values()));
    }


    private Map<VarName, VarName> getRolePlayerUnifiers(Relation parentAtom){
        Map<RoleType, VarName> childMap = getRoleMap();
        Map<RoleType, VarName> parentMap = parentAtom.getRoleMap();
        return getUnifiers(
                childMap,
                parentMap,
                getRelationPlayers().stream().map(rp -> rp.getRolePlayer().getVarName()).collect(Collectors.toList()),
                parentAtom.getRelationPlayers().stream().map(rp -> rp.getRolePlayer().getVarName()).collect(Collectors.toList()));
    }

    @Override
    public Map<VarName, VarName> getUnifiers(Atomic pAtom) {
        if (!(pAtom instanceof TypeAtom)) {
            throw new IllegalArgumentException(ErrorMessage.UNIFICATION_ATOM_INCOMPATIBILITY.getMessage());
        }

        Map<VarName, VarName> unifiers = super.getUnifiers(pAtom);
        if (((Atom) pAtom).isRelation()){
            Relation parentAtom = (Relation) pAtom;
            //get role player unifiers
            unifiers.putAll(getRolePlayerUnifiers(parentAtom));
            //get role type unifiers
            unifiers.putAll(getRoleTypeUnifiers(parentAtom));
        }
        return unifiers;
    }

    /**
     * @return map of pairs: varName - Id predicate (substitution) describing this variable
     */
    private Map<VarName, IdPredicate> getVarSubMap() {
        Map<VarName, IdPredicate> map = new HashMap<>();
        getIdPredicates()
            .forEach( sub -> {
                VarName var = sub.getVarName();
                map.put(var, sub);
        });
        return map;
    }

    /**
     * @return map of pairs role type - Id predicate describing the role player playing this role (substitution)
     */
    private Map<RoleType, String> getRoleConceptIdMap(){
        Map<RoleType, String> roleConceptMap = new HashMap<>();
        Map<VarName, IdPredicate> varSubMap = getVarSubMap();
        Map<RoleType, VarName> roleMap = getRoleMap();

        roleMap.forEach( (role, var) -> {
            roleConceptMap.put(role, varSubMap.containsKey(var) ? varSubMap.get(var).getPredicateValue() : "");
        });
        return roleConceptMap;
    }

    @Override
    public Pair<Atom, Map<VarName, VarName>> rewrite(Atom parentAtom, ReasonerQuery parent){
        if(parentAtom.isUserDefinedName()){
            Map<VarName, VarName> unifiers = new HashMap<>();
            Var relVar = Graql.var(UUID.randomUUID().toString());
            getPattern().asVar().getProperty(IsaProperty.class).ifPresent(prop -> relVar.isa(prop.getType()));

            getRelationPlayers()
                    .forEach(c -> {
                        VarAdmin rolePlayer = c.getRolePlayer();
                        VarName rolePlayerVarName = VarName.anon();
                        unifiers.put(rolePlayer.getVarName(), rolePlayerVarName);
                        VarAdmin roleType = c.getRoleType().orElse(null);
                        if (roleType != null) {
                            relVar.rel(roleType, Graql.var(rolePlayerVarName));
                        } else {
                            relVar.rel(Graql.var(rolePlayerVarName));
                        }
                    });
            return new Pair<>(new Relation(relVar.admin(), getPredicate(), parent), unifiers);
        } else {
            return new Pair<>(this, new HashMap<>());
        }
    }
}
