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
import ai.grakn.concept.Type;
import ai.grakn.concept.TypeLabel;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Var;
import ai.grakn.graql.VarName;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.RelationPlayer;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.pattern.property.IsaProperty;
import ai.grakn.graql.internal.pattern.property.RelationProperty;
import ai.grakn.graql.internal.reasoner.ReasonerUtils;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.atom.AtomBase;
import ai.grakn.graql.internal.reasoner.atom.AtomicFactory;
import ai.grakn.graql.internal.reasoner.atom.ResolutionStrategy;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;
import ai.grakn.graql.internal.reasoner.atom.predicate.Predicate;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueryImpl;
import ai.grakn.graql.internal.reasoner.query.UnifierImpl;
import ai.grakn.graql.internal.reasoner.rule.InferenceRule;
import ai.grakn.graql.internal.util.CommonUtil;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.Schema;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import java.util.Collection;
import java.util.Comparator;
import javafx.util.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static ai.grakn.graql.internal.reasoner.ReasonerUtils.capture;
import static ai.grakn.graql.internal.reasoner.ReasonerUtils.checkTypesDisjoint;
import static ai.grakn.graql.internal.reasoner.ReasonerUtils.getCompatibleRelationTypes;
import static ai.grakn.graql.internal.reasoner.ReasonerUtils.getListPermutations;
import static ai.grakn.graql.internal.reasoner.ReasonerUtils.getUnifiersFromPermutations;
import static ai.grakn.graql.internal.reasoner.ReasonerUtils.roleToRelationTypes;
import static ai.grakn.graql.internal.reasoner.ReasonerUtils.typeToRelationTypes;
import static ai.grakn.graql.internal.util.CommonUtil.toImmutableMultiset;
import static java.util.stream.Collectors.toSet;

/**
 *
 * <p>
 * Atom implementation defining a relation atom corresponding to a combined {@link RelationProperty}
 * and (optional) {@link IsaProperty}.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public class Relation extends TypeAtom {

    private int hashCode = 0;
    private Multimap<RoleType, Pair<VarName, Type>> roleVarTypeMap = null;
    private Multimap<RoleType, String> roleConceptIdMap = null;
    private Set<RelationPlayer> relationPlayers = null;

    public Relation(VarAdmin pattern, IdPredicate predicate, ReasonerQuery par) { super(pattern, predicate, par);}

    public Relation(VarName name, VarName typeVariable, Map<VarName, Var> roleMap, IdPredicate pred, ReasonerQuery par) {
        super(constructRelationVar(name, typeVariable, roleMap), pred, par);
    }

    private Relation(Relation a) {
        super(a);
    }

    @Override
    public String toString(){
        String relationString = (isUserDefinedName()? getVarName() + " ": " ") +
                        (getType() != null? getType().getLabel() : "") +
                        getRelationPlayers().toString();
        return relationString + getIdPredicates().stream().map(IdPredicate::toString).collect(Collectors.joining(""));
    }

    public Set<RelationPlayer> getRelationPlayers() {
        if (relationPlayers == null) {
            relationPlayers = new HashSet<>();
            this.atomPattern.asVar().getProperty(RelationProperty.class)
                    .ifPresent(prop -> prop.getRelationPlayers().forEach(relationPlayers::add));
        }
        return relationPlayers;
    }

    private void modifyRelationPlayers(UnaryOperator<RelationPlayer> mapper) {
        this.atomPattern = this.atomPattern.asVar().mapProperty(RelationProperty.class,
                prop -> new RelationProperty(prop.getRelationPlayers().map(mapper).collect(toImmutableMultiset())));
        relationPlayers = null;
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
            atomPattern = atomPattern.asVar().mapProperty(IsaProperty.class, prop -> new IsaProperty(prop.getType().setVarName(var)));
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
     *
     * @param varName            variable name
     * @param typeVariable       type variable name
     * @param rolePlayerMappings list of rolePlayer-roleType mappings
     * @return corresponding Var
     */
    private static VarAdmin constructRelationVar(VarName varName, VarName typeVariable, List<Pair<VarName, Var>> rolePlayerMappings) {
        Var var = !varName.getValue().isEmpty()? Graql.var(varName) : Graql.var();
        for (Pair<VarName, Var> mapping : rolePlayerMappings) {
            VarName rp = mapping.getKey();
            Var role = mapping.getValue();
            var = role == null? var.rel(Graql.var(rp)) : var.rel(role, Graql.var(rp));
        }
        var = var.isa(Graql.var(typeVariable));
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
        if (hashCode == 0) {
            hashCode = 1;
            hashCode = hashCode * 37 + (getTypeId() != null ? getTypeId().hashCode() : 0);
            hashCode = hashCode * 37 + getVarNames().hashCode();
        }
        return hashCode;
    }

    @Override
    public boolean isEquivalent(Object obj) {
        if (obj == null || this.getClass() != obj.getClass()) return false;
        if (obj == this) return true;
        Relation a2 = (Relation) obj;
        return (isUserDefinedName() == a2.isUserDefinedName())
                && Objects.equals(this.typeId, a2.getTypeId())
                && getRoleConceptIdMap().equals(a2.getRoleConceptIdMap())
                && getRoleTypeMap().equals(a2.getRoleTypeMap());
    }

    @Override
    public int equivalenceHashCode() {
        int hashCode = 1;
        hashCode = hashCode * 37 + (this.typeId != null ? this.typeId.hashCode() : 0);
        hashCode = hashCode * 37 + this.getRoleConceptIdMap().hashCode();
        hashCode = hashCode * 37 + this.getRoleTypeMap().hashCode();
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

    @Override
    public boolean isType() {
        return getType() != null;
    }

    @Override
    public boolean requiresMaterialisation() {
        return isUserDefinedName();
    }

    @Override
    public boolean isAllowedToFormRuleHead(){
        //can form a rule head if specified type and all relation players have a specified/unambiguously inferrable role type
        return super.isAllowedToFormRuleHead()
                && !hasMetaRoles();
    }

    @Override
    public int resolutionPriority() {
        int priority = super.resolutionPriority();
        priority += ResolutionStrategy.IS_RELATION_ATOM;
        return priority;
    }

    @Override
    public Set<IdPredicate> getPartialSubstitutions() {
        Set<VarName> rolePlayers = getRolePlayers();
        return getIdPredicates().stream()
                .filter(pred -> rolePlayers.contains(pred.getVarName()))
                .collect(toSet());
    }

    /**
     * @return map of pairs role type - Id predicate describing the role player playing this role (substitution)
     */
    private Multimap<RoleType, String> getRoleConceptIdMap() {
        if (roleConceptIdMap != null) return roleConceptIdMap;
        roleConceptIdMap =  ArrayListMultimap.create();
        Map<VarName, IdPredicate> varSubMap = getIdPredicates().stream()
                .collect(Collectors.toMap(AtomBase::getVarName, pred -> pred));
        Multimap<RoleType, VarName> roleMap = getRoleMap();

        roleMap.entries().forEach(e -> {
            RoleType role = e.getKey();
            VarName var = e.getValue();
            roleConceptIdMap.put(role, varSubMap.containsKey(var) ? varSubMap.get(var).getPredicateValue() : "");
        });
        return roleConceptIdMap;
    }

    private Multimap<RoleType, VarName> getRoleMap() {
        Multimap<RoleType, VarName> roleMap = ArrayListMultimap.create();
        getRoleVarTypeMap().entries()
                .forEach(e -> roleMap.put(e.getKey(), e.getValue().getKey()));
        return roleMap;
    }

    private Multimap<RoleType, Type> getRoleTypeMap() {
        Multimap<RoleType, Type> roleTypeMap = ArrayListMultimap.create();
        getRoleVarTypeMap().entries().stream()
                .filter(e -> Objects.nonNull(e.getValue().getValue()))
                .forEach(e -> roleTypeMap.put(e.getKey(), e.getValue().getValue()));
        return roleTypeMap;
    }

    //rule head atom is applicable if it is unifiable
    private boolean isRuleApplicableViaAtom(Relation headAtom) {
        return headAtom.getRelationPlayers().size() >= this.getRelationPlayers().size()
            && headAtom.getRelationPlayerMappings(this).size() == this.getRolePlayers().size();
    }

    @Override
    public boolean isRuleApplicable(InferenceRule child) {
        Atom ruleAtom = child.getRuleConclusionAtom();
        if (!(ruleAtom.isRelation())) return false;

        Relation headAtom = (Relation) ruleAtom;
        Type type = getType();

        Relation atomWithType = type == null?
                ((Relation) AtomicFactory.create(this, this.getParentQuery())).addType(headAtom.getType()) : this;
        return atomWithType.isRuleApplicableViaAtom(headAtom);
    }

    /**
     * @return true if any of the relation's role types are meta role types
     */
    public boolean hasMetaRoles(){
        Set<RoleType> parentRoles = getRoleVarTypeMap().keySet();
        for(RoleType role : parentRoles) {
            if (Schema.MetaSchema.isMetaLabel(role.getLabel())) return true;
        }
        return false;
    }

    private Set<RoleType> getExplicitRoleTypes() {
        Set<RoleType> roleTypes = new HashSet<>();
        ReasonerQueryImpl parent = (ReasonerQueryImpl) getParentQuery();
        GraknGraph graph = parent.graph();

        Set<VarAdmin> roleVars = getRelationPlayers().stream()
                .map(RelationPlayer::getRoleType)
                .flatMap(CommonUtil::optionalToStream)
                .collect(Collectors.toSet());
        //try directly
        roleVars.stream()
                .map(VarAdmin::getTypeLabel)
                .flatMap(CommonUtil::optionalToStream)
                .map(graph::<RoleType>getType)
                .forEach(roleTypes::add);

        //try indirectly
        roleVars.stream()
                .filter(VarAdmin::isUserDefinedName)
                .map(VarAdmin::getVarName)
                .map(parent::getIdPredicate)
                .filter(Objects::nonNull)
                .map(Predicate::getPredicate)
                .map(graph::<RoleType>getConcept)
                .forEach(roleTypes::add);
        return roleTypes;
    }

    public Relation addType(Type type) {
        typeId = type.getId();
        VarName typeVariable = getValueVariable().getValue().isEmpty() ?
                VarName.of("rel-" + UUID.randomUUID().toString()) : getValueVariable();
        setPredicate(new IdPredicate(Graql.var(typeVariable).id(typeId).admin(), getParentQuery()));
        atomPattern = atomPattern.asVar().isa(Graql.var(typeVariable)).admin();
        setValueVariable(typeVariable);
        return this;
    }

    private void inferRelationTypeFromTypes() {
        //look at available role types
        Type type = null;
        Set<Type> compatibleTypes = ReasonerUtils.getTopTypes(
                getCompatibleRelationTypes(getExplicitRoleTypes(), roleToRelationTypes)
        );
        if (compatibleTypes.size() == 1) type = compatibleTypes.iterator().next();

        //look at types
        if (type == null) {
            Map<VarName, Type> varTypeMap = getParentQuery().getVarTypeMap();
            Set<Type> types = getRolePlayers().stream()
                    .filter(varTypeMap::containsKey)
                    .map(varTypeMap::get)
                    .collect(toSet());

            Set<RelationType> compatibleTypesFromTypes = getCompatibleRelationTypes(types, typeToRelationTypes);
            if (compatibleTypesFromTypes.size() == 1) type = compatibleTypesFromTypes.iterator().next();
            else {
                //do intersection with types recovered from role types
                compatibleTypesFromTypes.retainAll(compatibleTypes);
                if (compatibleTypesFromTypes.size() == 1) type = compatibleTypesFromTypes.iterator().next();
            }
        }
        if (type != null) addType(type);
    }


    @Override
    public void inferTypes() {
        if (getPredicate() == null) inferRelationTypeFromTypes();
    }

    @Override
    public void unify(Unifier mappings) {
        super.unify(mappings);
        modifyRelationPlayers(c -> {
            VarName var = c.getRolePlayer().getVarName();
            if (mappings.containsKey(var)) {
                VarName target = mappings.get(var);
                return c.setRolePlayer(c.getRolePlayer().setVarName(target));
            } else if (mappings.containsValue(var)) {
                return c.setRolePlayer(c.getRolePlayer().setVarName(capture(var)));
            } else {
                return c;
            }
        });
    }

    @Override
    public Set<VarName> getVarNames() {
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
    public Set<VarName> getRolePlayers() {
        Set<VarName> vars = new HashSet<>();
        getRelationPlayers().forEach(c -> vars.add(c.getRolePlayer().getVarName()));
        return vars;
    }

    private Set<VarName> getMappedRolePlayers() {
        return getRoleVarTypeMap().entries().stream()
                .filter(e -> !Schema.MetaSchema.isMetaLabel(e.getKey().getLabel()))
                .map(Map.Entry::getValue)
                .map(Pair::getKey).collect(toSet());
    }

    /**
     * @return set constituting the role player var names that do not have a specified role type
     */
    public Set<VarName> getUnmappedRolePlayers() {
        Set<VarName> unmappedVars = getRolePlayers();
        unmappedVars.removeAll(getMappedRolePlayers());
        return unmappedVars;
    }

    @Override
    public Set<IdPredicate> getUnmappedIdPredicates() {
        Set<VarName> unmappedVars = getUnmappedRolePlayers();
        //filter by checking substitutions
        return getIdPredicates().stream()
                .filter(pred -> unmappedVars.contains(pred.getVarName()))
                .collect(toSet());
    }

    @Override
    public Set<TypeAtom> getMappedTypeConstraints() {
        Set<VarName> mappedVars = getMappedRolePlayers();
        return getTypeConstraints().stream()
                .filter(t -> mappedVars.contains(t.getVarName()))
                .filter(t -> Objects.nonNull(t.getType()))
                .collect(toSet());
    }

    @Override
    public Set<TypeAtom> getUnmappedTypeConstraints() {
        Set<VarName> unmappedVars = getUnmappedRolePlayers();
        return getTypeConstraints().stream()
                .filter(t -> unmappedVars.contains(t.getVarName()))
                .filter(t -> Objects.nonNull(t.getType()))
                .collect(toSet());
    }

    @Override
    public Set<Unifier> getPermutationUnifiers(Atom headAtom) {
        if (!headAtom.isRelation()) return new HashSet<>();
        List<VarName> permuteVars = new ArrayList<>();
        //if atom is match all atom, add type from rule head and find unmapped roles
        Relation relAtom = getValueVariable().getValue().isEmpty() ?
                ((Relation) AtomicFactory.create(this, getParentQuery())).addType(headAtom.getType()) : this;
        relAtom.getUnmappedRolePlayers().forEach(permuteVars::add);

        List<List<VarName>> varPermutations = getListPermutations(new ArrayList<>(permuteVars));
        return getUnifiersFromPermutations(permuteVars, varPermutations);
    }

    /**
     * Attempts to infer the implicit roleTypes and matching types based on contents of the parent query
     *
     * @return map containing roleType - (rolePlayer var - rolePlayer type) pairs
     */
    private Multimap<RoleType, Pair<VarName, Type>> computeRoleVarTypeMap() {
        this.roleVarTypeMap = ArrayListMultimap.create();
        if (getParentQuery() == null || getType() == null) return roleVarTypeMap;

        GraknGraph graph = getParentQuery().graph();
        RelationType relType = (RelationType) getType();
        Map<VarName, Type> varTypeMap = getParentQuery().getVarTypeMap();

        Set<RelationPlayer> allocatedRelationPlayers = new HashSet<>();

        //explicit role types from castings
        List<Pair<VarName, Var>> rolePlayerMappings = new ArrayList<>();
        getRelationPlayers().forEach(c -> {
            VarName varName = c.getRolePlayer().getVarName();
            VarAdmin role = c.getRoleType().orElse(null);
            if (role != null) {
                Type type = varTypeMap.get(varName);
                rolePlayerMappings.add(new Pair<>(varName, role));
                //try directly
                TypeLabel typeLabel = role.getTypeLabel().orElse(null);
                RoleType roleType = typeLabel != null ? graph.getType(typeLabel) : null;
                //try indirectly
                if (roleType == null && role.isUserDefinedName()) {
                    IdPredicate rolePredicate = ((ReasonerQueryImpl) getParentQuery()).getIdPredicate(role.getVarName());
                    if (rolePredicate != null) roleType = graph.getConcept(rolePredicate.getPredicate());
                }
                allocatedRelationPlayers.add(c);
                if (roleType != null) {
                    roleVarTypeMap.put(roleType, new Pair<>(varName, type));
                }
            }
        });

        //remaining roles
        //role types can repeat so no mather what has been allocated still the full spectrum of possibilities is present
        //TODO make restrictions based on cardinality constraints
        Set<RoleType> possibleRoles = Sets.newHashSet(relType.relates());

        //possible role types for each casting based on its type
        Map<RelationPlayer, Set<RoleType>> mappings = new HashMap<>();
        Sets.difference(getRelationPlayers(), allocatedRelationPlayers)
                .forEach(casting -> {
                    VarName varName = casting.getRolePlayer().getVarName();
                    Type type = varTypeMap.get(varName);
                    if (type != null && !Schema.MetaSchema.isMetaLabel(type.getLabel())) {
                        mappings.put(casting, ReasonerUtils.getCompatibleRoleTypes(type, possibleRoles));
                    } else {
                        mappings.put(casting, ReasonerUtils.getTopTypes(possibleRoles).stream().map(t -> (RoleType) t).collect(toSet()));
                    }
                });


        //resolve ambiguities until no unambiguous mapping exist
        while( mappings.values().stream().filter(s -> s.size() == 1).count() != 0) {
            Map.Entry<RelationPlayer, Set<RoleType>> entry = mappings.entrySet().stream()
                    .filter(e -> e.getValue().size() == 1)
                    .findFirst().orElse(null);

            RelationPlayer casting = entry.getKey();
            VarName varName = casting.getRolePlayer().getVarName();
            Type type = varTypeMap.get(varName);
            RoleType roleType = entry.getValue().iterator().next();
            VarAdmin roleVar = Graql.var().label(roleType.getLabel()).admin();

            //TODO remove from all mappings if it follows from cardinality constraints
            mappings.get(casting).remove(roleType);

            rolePlayerMappings.add(new Pair<>(varName, roleVar));
            roleVarTypeMap.put(roleType, new Pair<>(varName, type));
            allocatedRelationPlayers.add(casting);
        }

        //fill in unallocated roles with metarole
        RoleType metaRole = graph.admin().getMetaRoleType();
        VarAdmin metaRoleVar = Graql.var().label(metaRole.getLabel()).admin();
        Sets.difference(getRelationPlayers(), allocatedRelationPlayers)
                .forEach(casting -> {
                    VarName varName = casting.getRolePlayer().getVarName();
                    roleVarTypeMap.put(metaRole, new Pair<>(varName, varTypeMap.get(varName)));
                    rolePlayerMappings.add(new Pair<>(varName, metaRoleVar));
                });

        //pattern mutation!
        atomPattern = constructRelationVar(isUserDefinedName() ? varName : VarName.of(""), getValueVariable(), rolePlayerMappings);
        relationPlayers = null;
        return roleVarTypeMap;
    }

    @Override
    public Multimap<RoleType, Pair<VarName, Type>> getRoleVarTypeMap() {
        if (roleVarTypeMap == null) computeRoleVarTypeMap();
        return roleVarTypeMap;
    }

    private Multimap<RoleType, RelationPlayer> getRoleRelationPlayerMap(){
        Multimap<RoleType, RelationPlayer> roleRelationPlayerMap = HashMultimap.create();
        Multimap<RoleType, Pair<VarName, Type>> roleVarTypeMap = getRoleVarTypeMap();
        Set<RelationPlayer> relationPlayers = getRelationPlayers();
        roleVarTypeMap.asMap().entrySet()
                .forEach(e -> {
                    RoleType role = e.getKey();
                    TypeLabel roleLabel = role.getLabel();
                    relationPlayers.stream()
                            .filter(rp -> rp.getRoleType().isPresent())
                            .forEach(rp -> {
                                VarAdmin roleTypeVar = rp.getRoleType().orElse(null);
                                TypeLabel rl = roleTypeVar != null ? roleTypeVar.getTypeLabel().orElse(null) : null;
                                if (roleLabel != null && roleLabel.equals(rl)) {
                                    roleRelationPlayerMap.put(role, rp);
                                }
                            });
                });
        return roleRelationPlayerMap;
    }

    private Set<Pair<RelationPlayer, RelationPlayer>> getRelationPlayerMappings(Relation parentAtom) {
        Set<Pair<RelationPlayer, RelationPlayer>> rolePlayerMappings = new HashSet<>();

        //establish compatible castings for each parent casting
        Multimap<RelationPlayer, RelationPlayer> compatibleMappings = HashMultimap.create();
        parentAtom.getRoleRelationPlayerMap();
        Multimap<RoleType, RelationPlayer> childRoleRPMap = getRoleRelationPlayerMap();
        Map<VarName, Type> parentVarTypeMap = parentAtom.getParentQuery().getVarTypeMap();
        Map<VarName, Type> childVarTypeMap = this.getParentQuery().getVarTypeMap();

        Set<RoleType> relationRoles = new HashSet<>(getType().asRelationType().relates());
        Set<RoleType> childRoles = new HashSet<>(childRoleRPMap.keySet());

        parentAtom.getRelationPlayers().stream()
                .filter(prp -> prp.getRoleType().isPresent())
                .forEach(prp -> {
                    VarAdmin parentRoleTypeVar = prp.getRoleType().orElse(null);
                    TypeLabel parentRoleTypeLabel = parentRoleTypeVar.getTypeLabel().orElse(null);

                    //TODO take into account indirect roles
                    RoleType parentRole = parentRoleTypeLabel != null ? graph().getType(parentRoleTypeLabel) : null;

                    if (parentRole != null) {
                        boolean isMetaRole = Schema.MetaSchema.isMetaLabel(parentRole.getLabel());
                        VarName parentRolePlayer = prp.getRolePlayer().getVarName();
                        Type parentType = parentVarTypeMap.get(parentRolePlayer);

                        Set<RoleType> compatibleChildRoles = isMetaRole? childRoles : Sets.intersection(new HashSet<>(parentRole.subTypes()), childRoles);

                        if (parentType != null){
                            boolean isMetaType = Schema.MetaSchema.isMetaLabel(parentType.getLabel());
                            Set<RoleType> typeRoles = isMetaType? childRoles : new HashSet<>(parentType.plays());

                            //incompatible type
                            if (Sets.intersection(relationRoles, typeRoles).isEmpty()) compatibleChildRoles = new HashSet<>();
                            else {
                                compatibleChildRoles = compatibleChildRoles.stream()
                                        .filter(rc -> Schema.MetaSchema.isMetaLabel(rc.getLabel()) || typeRoles.contains(rc))
                                        .collect(toSet());
                            }
                        }

                        compatibleChildRoles.stream()
                                .filter(childRoleRPMap::containsKey)
                                .forEach(r -> {
                                    Collection<RelationPlayer> childRPs = parentType != null ?
                                            childRoleRPMap.get(r).stream()
                                                .filter(rp -> {
                                                    VarName childRolePlayer = rp.getRolePlayer().getVarName();
                                                    Type childType = childVarTypeMap.get(childRolePlayer);
                                                    return childType == null || !checkTypesDisjoint(parentType, childType);
                                                }).collect(toSet()) :
                                            childRoleRPMap.get(r);

                                    childRPs.forEach(rp -> compatibleMappings.put(prp, rp));
                                });
                    }
                });

        //self-consistent procedure until no non-empty mappings present
        while( compatibleMappings.asMap().values().stream().filter(s -> !s.isEmpty()).count() > 0) {
            //prioritise mappings with equivalent types and unambiguous mappings
            Map.Entry<RelationPlayer, RelationPlayer> entry = compatibleMappings.entries().stream()
                    .sorted(Comparator.comparing(e -> {
                        Type parentType = parentVarTypeMap.get(e.getKey().getRolePlayer().getVarName());
                        Type childType = childVarTypeMap.get(e.getValue().getRolePlayer().getVarName());
                        return !(parentType != null && childType != null && parentType.equals(childType));
                    }))
                    .sorted(Comparator.comparing(e -> compatibleMappings.get(e.getKey()).size()))
                    .findFirst().orElse(null);

            RelationPlayer parentCasting = entry.getKey();
            RelationPlayer childCasting = entry.getValue();

            rolePlayerMappings.add(new Pair<>(childCasting, parentCasting));
            compatibleMappings.removeAll(parentCasting);
            compatibleMappings.values().remove(childCasting);

        }
        return rolePlayerMappings;
    }

    @Override
    public Unifier getUnifier(Atomic pAtom) {
        if (!(pAtom instanceof TypeAtom)) {
            throw new IllegalArgumentException(ErrorMessage.UNIFICATION_ATOM_INCOMPATIBILITY.getMessage());
        }

        Unifier unifier = super.getUnifier(pAtom);
        if (((Atom) pAtom).isRelation()) {
            Relation parentAtom = (Relation) pAtom;

            getRelationPlayerMappings(parentAtom)
                    .forEach(rpm -> unifier.addMapping(rpm.getKey().getRolePlayer().getVarName(), rpm.getValue().getRolePlayer().getVarName()));
        }
        return unifier.removeTrivialMappings();
    }

    @Override
    public Atom rewriteToUserDefined(){
        Var newVar = Graql.var(VarName.anon());
        Var relVar = getPattern().asVar().getProperty(IsaProperty.class)
                .map(prop -> newVar.isa(prop.getType()))
                .orElse(newVar);

        for (RelationPlayer c: getRelationPlayers()) {
            VarAdmin roleType = c.getRoleType().orElse(null);
            if (roleType != null) {
                relVar = relVar.rel(roleType, c.getRolePlayer());
            } else {
                relVar = relVar.rel(c.getRolePlayer());
            }
        }
        return new Relation(relVar.admin(), getPredicate(), getParentQuery());
    }

    /**
     * rewrites the atom to one with user defined name, need unifiers for cases when we have variable clashes
     * between the relation variable and relation players
     * @return pair of (rewritten atom, unifiers required to unify child with rewritten atom)
     */
    @Override
    public Pair<Atom, Unifier> rewriteToUserDefinedWithUnifiers() {
        Unifier unifier = new UnifierImpl();
        Var newVar = Graql.var(VarName.anon());
        Var relVar = getPattern().asVar().getProperty(IsaProperty.class)
                .map(prop -> newVar.isa(prop.getType()))
                .orElse(newVar);

        for (RelationPlayer c: getRelationPlayers()) {
            VarAdmin rolePlayer = c.getRolePlayer();
            VarName rolePlayerVarName = VarName.anon();
            unifier.addMapping(rolePlayer.getVarName(), rolePlayerVarName);
            VarAdmin roleType = c.getRoleType().orElse(null);
            if (roleType != null) {
                relVar = relVar.rel(roleType, Graql.var(rolePlayerVarName));
            } else {
                relVar = relVar.rel(Graql.var(rolePlayerVarName));
            }
        }
        return new Pair<>(new Relation(relVar.admin(), getPredicate(), getParentQuery()), unifier);
    }
}
