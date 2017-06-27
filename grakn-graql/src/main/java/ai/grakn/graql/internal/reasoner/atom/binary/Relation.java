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
import ai.grakn.concept.Concept;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.Type;
import ai.grakn.concept.TypeLabel;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Var;
import ai.grakn.graql.VarPattern;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.RelationPlayer;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.pattern.property.IsaProperty;
import ai.grakn.graql.internal.pattern.property.RelationProperty;
import ai.grakn.graql.internal.query.QueryAnswer;
import ai.grakn.graql.internal.reasoner.UnifierImpl;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.atom.AtomicFactory;
import ai.grakn.graql.internal.reasoner.atom.ResolutionStrategy;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;
import ai.grakn.graql.internal.reasoner.atom.predicate.Predicate;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueryImpl;
import ai.grakn.graql.internal.reasoner.rule.InferenceRule;
import ai.grakn.graql.internal.reasoner.utils.ReasonerUtils;
import ai.grakn.graql.internal.reasoner.utils.conversion.RoleTypeConverter;
import ai.grakn.graql.internal.reasoner.utils.conversion.OntologyConceptConverterImpl;
import ai.grakn.util.CommonUtil;
import ai.grakn.util.Schema;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import javafx.util.Pair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static ai.grakn.graql.internal.reasoner.utils.ReasonerUtils.checkTypesDisjoint;
import static ai.grakn.graql.internal.reasoner.utils.ReasonerUtils.getCompatibleRelationTypesWithRoles;
import static ai.grakn.graql.internal.reasoner.utils.ReasonerUtils.getListPermutations;
import static ai.grakn.graql.internal.reasoner.utils.ReasonerUtils.getSupers;
import static ai.grakn.graql.internal.reasoner.utils.ReasonerUtils.getUnifiersFromPermutations;
import static ai.grakn.graql.internal.reasoner.utils.ReasonerUtils.multimapIntersection;
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
    private Multimap<RoleType, Var> roleVarMap = null;
    private Multimap<RoleType, String> roleConceptIdMap = null;
    private Set<RelationPlayer> relationPlayers = null;

    public Relation(VarPatternAdmin pattern, IdPredicate predicate, ReasonerQuery par) { super(pattern, predicate, par);}
    public Relation(Var name, Var typeVariable, Map<Var, VarPattern> roleMap, IdPredicate pred, ReasonerQuery par) {
        super(constructRelationVar(name, typeVariable, roleMap), pred, par);
    }

    private Relation(Relation a) {
        super(a);
        this.relationPlayers = a.relationPlayers;
        this.roleVarMap = a.roleVarMap;
    }

    @Override
    public String toString(){
        String relationString = (isUserDefinedName()? getVarName() + " ": "") +
                (getType() != null? getType().getLabel() : "") +
                getRelationPlayers().toString();
        return relationString + getIdPredicates().stream().map(IdPredicate::toString).collect(Collectors.joining(""));
    }

    private Set<RelationPlayer> getRelationPlayers() {
        if (relationPlayers == null) {
            relationPlayers = new HashSet<>();
            this.atomPattern.asVar().getProperty(RelationProperty.class)
                    .ifPresent(prop -> prop.getRelationPlayers().forEach(relationPlayers::add));
        }
        return relationPlayers;
    }

    @Override
    protected Var extractValueVariableName(VarPatternAdmin var) {
        IsaProperty isaProp = var.getProperty(IsaProperty.class).orElse(null);
        return isaProp != null ? isaProp.getType().getVarName() : Graql.var("");
    }

    @Override
    protected void setValueVariable(Var var) {
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


    private static VarPatternAdmin constructRelationVar(Var varName, Var typeVariable, Map<Var, VarPattern> roleMap) {
        return constructRelationVar(varName, typeVariable, roleMap.entrySet().stream().map(e -> new Pair<>(e.getKey(), e.getValue())).collect(Collectors.toList()));
    }

    /**
     * construct a $varName (rolemap) isa $typeVariable relation
     *
     * @param varName            variable name
     * @param typeVariable       type variable name
     * @param rolePlayerMappings list of rolePlayer-roleType mappings
     * @return corresponding {@link VarPatternAdmin}
     */
    private static VarPatternAdmin constructRelationVar(Var varName, Var typeVariable, List<Pair<Var, VarPattern>> rolePlayerMappings) {
        VarPattern var = !varName.getValue().isEmpty()? varName : Graql.var();
        for (Pair<Var, VarPattern> mapping : rolePlayerMappings) {
            Var rp = mapping.getKey();
            VarPattern role = mapping.getValue();
            var = role == null? var.rel(rp) : var.rel(role, rp);
        }
        var = var.isa(typeVariable);
        return var.admin();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || this.getClass() != obj.getClass()) return false;
        if (obj == this) return true;
        Relation a2 = (Relation) obj;
        return Objects.equals(this.getTypeId(), a2.getTypeId())
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
                && Objects.equals(this.getTypeId(), a2.getTypeId())
                && getRoleConceptIdMap().equals(a2.getRoleConceptIdMap())
                && getRoleTypeMap().equals(a2.getRoleTypeMap())
                && getRolePlayers().size() == a2.getRolePlayers().size();
    }

    @Override
    public int equivalenceHashCode() {
        int equivalenceHashCode = 1;
        equivalenceHashCode = equivalenceHashCode * 37 + (this.getTypeId() != null ? this.getTypeId().hashCode() : 0);
        equivalenceHashCode = equivalenceHashCode * 37 + this.getRoleConceptIdMap().hashCode();
        equivalenceHashCode = equivalenceHashCode * 37 + this.getRoleTypeMap().hashCode();
        return equivalenceHashCode;
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
    public int computePriority(Set<Var> subbedVars) {
        int priority = super.computePriority(subbedVars);
        priority += ResolutionStrategy.IS_RELATION_ATOM;
        return priority;
    }

    @Override
    public Set<IdPredicate> getPartialSubstitutions() {
        Set<Var> rolePlayers = getRolePlayers();
        return getIdPredicates().stream()
                .filter(pred -> rolePlayers.contains(pred.getVarName()))
                .collect(toSet());
    }

    /**
     * @return map of pairs role type - Id predicate describing the role player playing this role (substitution)
     */
    private Multimap<RoleType, String> getRoleConceptIdMap() {
        if (roleConceptIdMap == null) {
            roleConceptIdMap = ArrayListMultimap.create();

            Map<Var, IdPredicate> varSubMap = getPartialSubstitutions().stream()
                    .collect(Collectors.toMap(Atomic::getVarName, pred -> pred));
            Multimap<RoleType, Var> roleMap = getRoleVarMap();

            roleMap.entries().forEach(e -> {
                RoleType role = e.getKey();
                Var var = e.getValue();
                roleConceptIdMap.put(role, varSubMap.containsKey(var) ? varSubMap.get(var).getPredicateValue() : "");
            });
        }
        return roleConceptIdMap;
    }

    private Multimap<RoleType, Type> getRoleTypeMap() {
        Multimap<RoleType, Type> roleTypeMap = ArrayListMultimap.create();
        Multimap<RoleType, Var> roleMap = getRoleVarMap();
        Map<Var, Type> varTypeMap = getParentQuery().getVarTypeMap();

        roleMap.entries().stream()
                .filter(e -> varTypeMap.containsKey(e.getValue()))
                .forEach(e -> roleTypeMap.put(e.getKey(), varTypeMap.get(e.getValue())));
        return roleTypeMap;
    }

    //rule head atom is applicable if it is unifiable
    private boolean isRuleApplicableViaAtom(Relation headAtom) {
        return headAtom.getRelationPlayers().size() >= this.getRelationPlayers().size()
                && headAtom.getRelationPlayerMappings(this).size() == this.getRelationPlayers().size();
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
    private boolean hasMetaRoles(){
        Set<RoleType> parentRoles = getRoleVarMap().keySet();
        for(RoleType role : parentRoles) {
            if (Schema.MetaSchema.isMetaLabel(role.getLabel())) return true;
        }
        return false;
    }

    private Set<RoleType> getExplicitRoleTypes() {
        Set<RoleType> roleTypes = new HashSet<>();
        ReasonerQueryImpl parent = (ReasonerQueryImpl) getParentQuery();
        GraknGraph graph = parent.graph();

        Set<VarPatternAdmin> roleVars = getRelationPlayers().stream()
                .map(RelationPlayer::getRoleType)
                .flatMap(CommonUtil::optionalToStream)
                .collect(Collectors.toSet());
        //try directly
        roleVars.stream()
                .map(VarPatternAdmin::getTypeLabel)
                .flatMap(CommonUtil::optionalToStream)
                .map(graph::<RoleType>getOntologyConcept)
                .forEach(roleTypes::add);

        //try indirectly
        roleVars.stream()
                .filter(v -> v.getVarName().isUserDefinedName())
                .map(VarPatternAdmin::getVarName)
                .map(parent::getIdPredicate)
                .filter(Objects::nonNull)
                .map(Predicate::getPredicate)
                .map(graph::<RoleType>getConcept)
                .forEach(roleTypes::add);
        return roleTypes;
    }

    public Relation addType(Type type) {
        typeId = type.getId();
        Var typeVariable = getValueVariable().getValue().isEmpty() ?
                Graql.var("rel-" + UUID.randomUUID().toString()) : getValueVariable();
        setPredicate(new IdPredicate(typeVariable.id(typeId).admin(), getParentQuery()));
        atomPattern = atomPattern.asVar().isa(typeVariable).admin();
        setValueVariable(typeVariable);

        //reset applicable rules
        applicableRules = null;
        return this;
    }

    /**
     * @param sub answer
     * @return entity types inferred from answer entity information
     */
    private Set<Type> inferEntityTypes(Answer sub) {
        if (sub.isEmpty()) return Collections.emptySet();

        Set<Var> subbedVars = Sets.intersection(getRolePlayers(), sub.keySet());
        Set<Var> untypedVars = Sets.difference(subbedVars, getParentQuery().getVarTypeMap().keySet());
        return untypedVars.stream()
                .map(v -> new Pair<>(v, sub.get(v)))
                .filter(p -> p.getValue().isEntity())
                .map(e -> {
                    Concept c = e.getValue();
                    return c.asInstance().type();
                })
                .collect(toSet());
    }

    /**
     * infer relation types that this relation atom can potentially have
     * NB: entity types and role types are treated separately as they behave differently:
     * entity types only play the explicitly defined roles (not the relevant part of the hierarchy of the specified role)
     * @return list of relation types this atom can have ordered by the number of compatible role types
     */
    public List<RelationType> inferPossibleRelationTypes(Answer sub) {
        //look at available role types
        Multimap<RelationType, RoleType> compatibleTypesFromRoles = getCompatibleRelationTypesWithRoles(getExplicitRoleTypes(), new RoleTypeConverter());

        //look at entity types
        Map<Var, Type> varTypeMap = getParentQuery().getVarTypeMap();

        //explicit types
        Set<Type> types = getRolePlayers().stream()
                .filter(varTypeMap::containsKey)
                .map(varTypeMap::get)
                .collect(toSet());

        //types deduced from substitution
        inferEntityTypes(sub).forEach(types::add);

        Multimap<RelationType, RoleType> compatibleTypesFromTypes = getCompatibleRelationTypesWithRoles(types, new OntologyConceptConverterImpl());

        Multimap<RelationType, RoleType> compatibleTypes;
        //intersect relation types from roles and types
        if (compatibleTypesFromRoles.isEmpty()){
            compatibleTypes = compatibleTypesFromTypes;
        } else if (!compatibleTypesFromTypes.isEmpty()){
            compatibleTypes = multimapIntersection(compatibleTypesFromTypes, compatibleTypesFromRoles);
        } else {
            compatibleTypes = compatibleTypesFromRoles;
        }

        return compatibleTypes.asMap().entrySet().stream()
                .sorted(Comparator.comparing(e -> -e.getValue().size()))
                .map(Map.Entry::getKey)
                .filter(t -> Sets.intersection(getSupers(t), compatibleTypes.keySet()).isEmpty())
                .collect(Collectors.toList());
    }

    private Relation inferRelationType(Answer sub){
        List<RelationType> relationTypes = inferPossibleRelationTypes(sub);
        if (relationTypes.size() == 1) addType(relationTypes.iterator().next());
        return this;
    }

    @Override
    public void inferTypes() {
        if (getPredicate() == null) inferRelationType(new QueryAnswer());
        if (getExplicitRoleTypes().size() < getRelationPlayers().size() && getType() != null) computeRoleVarTypeMap();
    }

    @Override
    public Set<Var> getVarNames() {
        Set<Var> vars = super.getVarNames();
        vars.addAll(getRolePlayers());
        //add user specified role type vars
        getRelationPlayers().stream()
                .map(RelationPlayer::getRoleType)
                .flatMap(CommonUtil::optionalToStream)
                .filter(v -> v.getVarName().isUserDefinedName())
                .forEach(r -> vars.add(r.getVarName()));
        return vars;
    }

    /**
     * @return set constituting the role player var names
     */
    public Set<Var> getRolePlayers() {
        Set<Var> vars = new HashSet<>();
        getRelationPlayers().forEach(c -> vars.add(c.getRolePlayer().getVarName()));
        return vars;
    }

    private Set<Var> getMappedRolePlayers() {
        return getRoleVarMap().entries().stream()
                .filter(e -> !Schema.MetaSchema.isMetaLabel(e.getKey().getLabel()))
                .map(Map.Entry::getValue)
                .collect(toSet());
    }

    /**
     * @return set constituting the role player var names that do not have a specified role type
     */
    public Set<Var> getUnmappedRolePlayers() {
        Set<Var> unmappedVars = getRolePlayers();
        unmappedVars.removeAll(getMappedRolePlayers());
        return unmappedVars;
    }

    @Override
    public Set<IdPredicate> getUnmappedIdPredicates() {
        Set<Var> unmappedVars = getUnmappedRolePlayers();
        //filter by checking substitutions
        return getIdPredicates().stream()
                .filter(pred -> unmappedVars.contains(pred.getVarName()))
                .collect(toSet());
    }

    @Override
    public Set<TypeAtom> getMappedTypeConstraints() {
        Set<Var> mappedVars = getMappedRolePlayers();
        return getTypeConstraints().stream()
                .filter(t -> mappedVars.contains(t.getVarName()))
                .filter(t -> Objects.nonNull(t.getType()))
                .collect(toSet());
    }

    @Override
    public Set<TypeAtom> getUnmappedTypeConstraints() {
        Set<Var> unmappedVars = getUnmappedRolePlayers();
        return getTypeConstraints().stream()
                .filter(t -> unmappedVars.contains(t.getVarName()))
                .filter(t -> Objects.nonNull(t.getType()))
                .collect(toSet());
    }

    @Override
    public Set<Unifier> getPermutationUnifiers(Atom headAtom) {
        if (!headAtom.isRelation()) return new HashSet<>();
        List<Var> permuteVars = new ArrayList<>();
        //if atom is match all atom, add type from rule head and find unmapped roles
        Relation relAtom = getValueVariable().getValue().isEmpty() ?
                ((Relation) AtomicFactory.create(this, getParentQuery())).addType(headAtom.getType()) : this;
        relAtom.getUnmappedRolePlayers().forEach(permuteVars::add);

        List<List<Var>> varPermutations = getListPermutations(new ArrayList<>(permuteVars));
        return getUnifiersFromPermutations(permuteVars, varPermutations);
    }

    /**
     * Attempts to infer the implicit roleTypes and matching types based on contents of the parent query
     *
     * @return map containing roleType - (rolePlayer var - rolePlayer type) pairs
     */
    private Multimap<RoleType, Var> computeRoleVarTypeMap() {
        this.roleVarMap = ArrayListMultimap.create();
        if (getParentQuery() == null || getType() == null) return roleVarMap;

        GraknGraph graph = getParentQuery().graph();
        RelationType relType = (RelationType) getType();
        Map<Var, Type> varTypeMap = getParentQuery().getVarTypeMap();

        Set<RelationPlayer> allocatedRelationPlayers = new HashSet<>();

        //explicit role types from castings
        List<Pair<Var, VarPattern>> rolePlayerMappings = new ArrayList<>();
        getRelationPlayers().forEach(c -> {
            Var varName = c.getRolePlayer().getVarName();
            VarPatternAdmin role = c.getRoleType().orElse(null);
            if (role != null) {
                rolePlayerMappings.add(new Pair<>(varName, role));
                //try directly
                TypeLabel typeLabel = role.getTypeLabel().orElse(null);
                RoleType roleType = typeLabel != null ? graph.getOntologyConcept(typeLabel) : null;
                //try indirectly
                if (roleType == null && role.getVarName().isUserDefinedName()) {
                    IdPredicate rolePredicate = ((ReasonerQueryImpl) getParentQuery()).getIdPredicate(role.getVarName());
                    if (rolePredicate != null) roleType = graph.getConcept(rolePredicate.getPredicate());
                }
                allocatedRelationPlayers.add(c);
                if (roleType != null) roleVarMap.put(roleType, varName);
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
                    Var varName = casting.getRolePlayer().getVarName();
                    Type type = varTypeMap.get(varName);
                    if (type != null && !Schema.MetaSchema.isMetaLabel(type.getLabel())) {
                        mappings.put(casting, ReasonerUtils.getCompatibleRoleTypes(type, possibleRoles));
                    } else {
                        mappings.put(casting, ReasonerUtils.getOntologyConcepts(possibleRoles));
                    }
                });


        //resolve ambiguities until no unambiguous mapping exist
        while( mappings.values().stream().filter(s -> s.size() == 1).count() != 0) {
            Map.Entry<RelationPlayer, Set<RoleType>> entry = mappings.entrySet().stream()
                    .filter(e -> e.getValue().size() == 1)
                    .findFirst().orElse(null);

            RelationPlayer casting = entry.getKey();
            Var varName = casting.getRolePlayer().getVarName();
            RoleType roleType = entry.getValue().iterator().next();
            VarPatternAdmin roleVar = Graql.var().label(roleType.getLabel()).admin();

            //TODO remove from all mappings if it follows from cardinality constraints
            mappings.get(casting).remove(roleType);

            rolePlayerMappings.add(new Pair<>(varName, roleVar));
            roleVarMap.put(roleType, varName);
            allocatedRelationPlayers.add(casting);
        }

        //fill in unallocated roles with metarole
        RoleType metaRole = graph.admin().getMetaRoleType();
        VarPatternAdmin metaRoleVar = Graql.var().label(metaRole.getLabel()).admin();
        Sets.difference(getRelationPlayers(), allocatedRelationPlayers)
                .forEach(casting -> {
                    Var varName = casting.getRolePlayer().getVarName();
                    roleVarMap.put(metaRole, varName);
                    rolePlayerMappings.add(new Pair<>(varName, metaRoleVar));
                });

        //pattern mutation!
        atomPattern = constructRelationVar(isUserDefinedName() ? getVarName() : Graql.var(""), getValueVariable(), rolePlayerMappings);
        relationPlayers = null;
        return roleVarMap;
    }

    public Multimap<RoleType, Var> getRoleVarMap() {
        if (roleVarMap == null) computeRoleVarTypeMap();
        return roleVarMap;
    }

    private Multimap<RoleType, RelationPlayer> getRoleRelationPlayerMap(){
        Multimap<RoleType, RelationPlayer> roleRelationPlayerMap = HashMultimap.create();
        Multimap<RoleType, Var> roleVarTypeMap = getRoleVarMap();
        Set<RelationPlayer> relationPlayers = getRelationPlayers();
        roleVarTypeMap.asMap().entrySet()
                .forEach(e -> {
                    RoleType role = e.getKey();
                    TypeLabel roleLabel = role.getLabel();
                    relationPlayers.stream()
                            .filter(rp -> rp.getRoleType().isPresent())
                            .forEach(rp -> {
                                VarPatternAdmin roleTypeVar = rp.getRoleType().orElse(null);
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
        Map<Var, Type> parentVarTypeMap = parentAtom.getParentQuery().getVarTypeMap();
        Map<Var, Type> childVarTypeMap = this.getParentQuery().getVarTypeMap();

        Set<RoleType> relationRoles = new HashSet<>(getType().asRelationType().relates());
        Set<RoleType> childRoles = new HashSet<>(childRoleRPMap.keySet());

        parentAtom.getRelationPlayers().stream()
                .filter(prp -> prp.getRoleType().isPresent())
                .forEach(prp -> {
                    VarPatternAdmin parentRoleTypeVar = prp.getRoleType().orElse(null);
                    TypeLabel parentRoleTypeLabel = parentRoleTypeVar.getTypeLabel().orElse(null);

                    //TODO take into account indirect roles
                    RoleType parentRole = parentRoleTypeLabel != null ? graph().getOntologyConcept(parentRoleTypeLabel) : null;

                    if (parentRole != null) {
                        boolean isMetaRole = Schema.MetaSchema.isMetaLabel(parentRole.getLabel());
                        Var parentRolePlayer = prp.getRolePlayer().getVarName();
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
                                                        Var childRolePlayer = rp.getRolePlayer().getVarName();
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
            Map.Entry<RelationPlayer, RelationPlayer> entry = compatibleMappings.entries().stream()
                    //prioritise mappings with equivalent types and unambiguous mappings
                    .sorted(Comparator.comparing(e -> {
                        Type parentType = parentVarTypeMap.get(e.getKey().getRolePlayer().getVarName());
                        Type childType = childVarTypeMap.get(e.getValue().getRolePlayer().getVarName());
                        return !(parentType != null && childType != null && parentType.equals(childType));
                    }))
                    //prioritise mappings with sam var substitution (idpredicates)
                    .sorted(Comparator.comparing(e -> {
                        IdPredicate parentId = parentAtom.getIdPredicates().stream()
                                .filter(p -> p.getVarName().equals(e.getKey().getRolePlayer().getVarName()))
                                .findFirst().orElse(null);
                        IdPredicate childId = getIdPredicates().stream()
                                .filter(p -> p.getVarName().equals(e.getValue().getRolePlayer().getVarName()))
                                .findFirst().orElse(null);
                        return !(parentId != null && childId != null && parentId.getPredicate().equals(childId.getPredicate()));
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
    public Unifier getUnifier(Atom pAtom) {
        if (this.equals(pAtom)) return new UnifierImpl();

        Unifier unifier = super.getUnifier(pAtom);
        if (pAtom.isRelation()) {
            Relation parentAtom = (Relation) pAtom;

            getRelationPlayerMappings(parentAtom)
                    .forEach(rpm -> unifier.addMapping(rpm.getKey().getRolePlayer().getVarName(), rpm.getValue().getRolePlayer().getVarName()));
        }
        return unifier.removeTrivialMappings();
    }

    @Override
    public Atom rewriteToUserDefined(){
        VarPattern newVar = Graql.var().asUserDefined();
        VarPattern relVar = getPattern().asVar().getProperty(IsaProperty.class)
                .map(prop -> newVar.isa(prop.getType()))
                .orElse(newVar);

        for (RelationPlayer c: getRelationPlayers()) {
            VarPatternAdmin roleType = c.getRoleType().orElse(null);
            if (roleType != null) {
                relVar = relVar.rel(roleType, c.getRolePlayer());
            } else {
                relVar = relVar.rel(c.getRolePlayer());
            }
        }
        return new Relation(relVar.admin(), getPredicate(), getParentQuery());
    }
}
