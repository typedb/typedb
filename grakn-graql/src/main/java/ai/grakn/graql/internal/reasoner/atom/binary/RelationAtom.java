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
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Label;
import ai.grakn.concept.OntologyConcept;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Role;
import ai.grakn.concept.Type;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Var;
import ai.grakn.graql.VarPattern;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.PatternAdmin;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.RelationPlayer;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.pattern.property.IsaProperty;
import ai.grakn.graql.internal.pattern.property.RelationProperty;
import ai.grakn.graql.internal.query.QueryAnswer;
import ai.grakn.graql.internal.reasoner.UnifierImpl;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.atom.binary.type.IsaAtom;
import ai.grakn.graql.internal.reasoner.query.ResolutionPlan;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;
import ai.grakn.graql.internal.reasoner.atom.predicate.Predicate;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueryImpl;
import ai.grakn.graql.internal.reasoner.rule.InferenceRule;
import ai.grakn.graql.internal.reasoner.utils.ReasonerUtils;
import ai.grakn.graql.internal.reasoner.utils.conversion.OntologyConceptConverterImpl;
import ai.grakn.graql.internal.reasoner.utils.conversion.RoleTypeConverter;
import ai.grakn.util.CommonUtil;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.Schema;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import javafx.util.Pair;

import javax.annotation.Nullable;
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
import java.util.stream.Collectors;

import static ai.grakn.graql.internal.reasoner.utils.ReasonerUtils.checkDisjoint;
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
 * and (optional) {@link IsaProperty}. The relation atom is a {@link TypeAtom} with relation players.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public class RelationAtom extends IsaAtom {

    private int hashCode = 0;
    private Multimap<Role, Var> roleVarMap = null;
    private Multimap<Role, String> roleConceptIdMap = null;
    private List<RelationPlayer> relationPlayers = null;

    public RelationAtom(VarPatternAdmin pattern, Var predicateVar, @Nullable IdPredicate predicate, ReasonerQuery par) {
        super(pattern, predicateVar, predicate, par);}

    private RelationAtom(RelationAtom a) {
        super(a);
        this.relationPlayers = a.relationPlayers;
        this.roleVarMap = a.roleVarMap;
    }

    @Override
    public String toString(){
        String relationString = (isUserDefinedName()? getVarName() + " ": "") +
                (getOntologyConcept() != null? getOntologyConcept().getLabel() : "") +
                getRelationPlayers().toString();
        return relationString + getIdPredicates().stream().map(IdPredicate::toString).collect(Collectors.joining(""));
    }

    private List<RelationPlayer> getRelationPlayers() {
        if (relationPlayers == null) {
            relationPlayers = new ArrayList<>();
            getPattern().asVar()
                    .getProperty(RelationProperty.class)
                    .ifPresent(prop -> prop.getRelationPlayers()
                            .forEach(relationPlayers::add));
        }
        return relationPlayers;
    }

    @Override
    public Atomic copy() {
        return new RelationAtom(this);
    }

    /**
     * construct a $varName (rolemap) isa $typeVariable relation
     *
     * @param varName            variable name
     * @param typeVariable       type variable name
     * @param rolePlayerMappings list of rolePlayer-roleType mappings
     * @return corresponding {@link VarPatternAdmin}
     */
    private static VarPatternAdmin constructRelationVarPattern(Var varName, Var typeVariable, List<Pair<Var, VarPattern>> rolePlayerMappings) {
        VarPattern var = !varName.getValue().isEmpty()? varName : Graql.var();
        for (Pair<Var, VarPattern> mapping : rolePlayerMappings) {
            Var rp = mapping.getKey();
            VarPattern role = mapping.getValue();
            var = role == null? var.rel(rp) : var.rel(role, rp);
        }
        if (!typeVariable.getValue().isEmpty()) var = var.isa(typeVariable);
        return var.admin();
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
    public boolean equals(Object obj) {
        if (obj == null || this.getClass() != obj.getClass()) return false;
        if (obj == this) return true;
        RelationAtom a2 = (RelationAtom) obj;
        return Objects.equals(this.getTypeId(), a2.getTypeId())
                && this.getVarNames().equals(a2.getVarNames())
                && getRelationPlayers().equals(a2.getRelationPlayers());
    }

    @Override
    public boolean isEquivalent(Object obj) {
        if (obj == null || this.getClass() != obj.getClass()) return false;
        if (obj == this) return true;
        RelationAtom a2 = (RelationAtom) obj;
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
        return getOntologyConcept() != null;
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
    public Set<String> validateOntologically() {
        Set<String> errors = new HashSet<>();
        OntologyConcept type = getOntologyConcept();
        if (type != null && !type.isRelationType()){
            errors.add(ErrorMessage.VALIDATION_RULE_INVALID_RELATION_TYPE.getMessage(type.getLabel()));
            return errors;
        }

        //check roles are ok
        Collection<Role> possibleRoles = type != null? type.asRelationType().relates() : Collections.EMPTY_SET;
        Map<Var, OntologyConcept> varOntologyConceptMap = getParentQuery().getVarOntologyConceptMap();

        for (Map.Entry<Role, Collection<Var>> e : getRoleVarMap().asMap().entrySet() ){
            Role role = e.getKey();
            if (!Schema.MetaSchema.isMetaLabel(role.getLabel())) {
                //check whether this role can be played in this relation
                if (type != null && !possibleRoles.contains(role)) {
                    errors.add(ErrorMessage.VALIDATION_RULE_ROLE_CANNOT_BE_PLAYED.getMessage(role.getLabel(), type.getLabel()));
                }

                //check whether the role player's type allows playing this role
                for (Var player : e.getValue()) {
                    OntologyConcept playerType = varOntologyConceptMap.get(player);
                    if (playerType != null && !playerType.asType().plays().contains(role)) {
                        errors.add(ErrorMessage.VALIDATION_RULE_TYPE_CANNOT_PLAY_ROLE.getMessage(playerType.getLabel(), role.getLabel(), type == null? "" : type.getLabel()));
                    }
                }
            }
        }
        return errors;
    }

    @Override
    public int computePriority(Set<Var> subbedVars) {
        int priority = super.computePriority(subbedVars);
        priority += ResolutionPlan.IS_RELATION_ATOM;
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
    private Multimap<Role, String> getRoleConceptIdMap() {
        if (roleConceptIdMap == null) {
            roleConceptIdMap = ArrayListMultimap.create();

            Map<Var, IdPredicate> varSubMap = getPartialSubstitutions().stream()
                    .collect(Collectors.toMap(Atomic::getVarName, pred -> pred));
            Multimap<Role, Var> roleMap = getRoleVarMap();

            roleMap.entries().forEach(e -> {
                Role role = e.getKey();
                Var var = e.getValue();
                roleConceptIdMap.put(role, varSubMap.containsKey(var) ? varSubMap.get(var).getPredicateValue() : "");
            });
        }
        return roleConceptIdMap;
    }

    private Multimap<Role, OntologyConcept> getRoleTypeMap() {
        Multimap<Role, OntologyConcept> roleTypeMap = ArrayListMultimap.create();
        Multimap<Role, Var> roleMap = getRoleVarMap();
        Map<Var, OntologyConcept> varTypeMap = getParentQuery().getVarOntologyConceptMap();

        roleMap.entries().stream()
                .filter(e -> varTypeMap.containsKey(e.getValue()))
                .sorted(Comparator.comparing(e -> varTypeMap.get(e.getValue()).getLabel()))
                .forEach(e -> roleTypeMap.put(e.getKey(), varTypeMap.get(e.getValue())));
        return roleTypeMap;
    }

    //rule head atom is applicable if it is unifiable
    private boolean isRuleApplicableViaAtom(RelationAtom headAtom) {
        return headAtom.getRelationPlayers().size() >= this.getRelationPlayers().size()
                && headAtom.getRelationPlayerMappings(this).size() == this.getRelationPlayers().size();
    }

    @Override
    public boolean isRuleApplicable(InferenceRule child) {
        Atom ruleAtom = child.getRuleConclusionAtom();
        if (!(ruleAtom.isRelation())) return false;

        RelationAtom headAtom = (RelationAtom) ruleAtom;
        RelationAtom atomWithType = this.addType(headAtom.getOntologyConcept()).inferRoleTypes();
        return atomWithType.isRuleApplicableViaAtom(headAtom);
    }

    /**
     * @return true if any of the relation's role types are meta role types
     */
    private boolean hasMetaRoles(){
        Set<Role> parentRoles = getRoleVarMap().keySet();
        for(Role role : parentRoles) {
            if (Schema.MetaSchema.isMetaLabel(role.getLabel())) return true;
        }
        return false;
    }

    private Set<Role> getExplicitRoleTypes() {
        Set<Role> roles = new HashSet<>();
        ReasonerQueryImpl parent = (ReasonerQueryImpl) getParentQuery();
        GraknGraph graph = parent.graph();

        Set<VarPatternAdmin> roleVars = getRelationPlayers().stream()
                .map(RelationPlayer::getRole)
                .flatMap(CommonUtil::optionalToStream)
                .collect(Collectors.toSet());
        //try directly
        roleVars.stream()
                .map(VarPatternAdmin::getTypeLabel)
                .flatMap(CommonUtil::optionalToStream)
                .map(graph::<Role>getOntologyConcept)
                .forEach(roles::add);

        //try indirectly
        roleVars.stream()
                .filter(v -> v.getVarName().isUserDefinedName())
                .map(VarPatternAdmin::getVarName)
                .map(parent::getIdPredicate)
                .filter(Objects::nonNull)
                .map(Predicate::getPredicate)
                .map(graph::<Role>getConcept)
                .forEach(roles::add);
        return roles;
    }

    /**
     * @param type to be added to this relation
     * @return new relation with specified type
     */
    public RelationAtom addType(OntologyConcept  type) {
        ConceptId typeId = type.getId();
        Var typeVariable = getPredicateVariable().getValue().isEmpty() ? Graql.var().asUserDefined() : getPredicateVariable();

        VarPatternAdmin newPattern = getPattern().asVar().isa(typeVariable).admin();
        IdPredicate newPredicate = new IdPredicate(typeVariable.id(typeId).admin(), getParentQuery());

        return new RelationAtom(newPattern, typeVariable, newPredicate, this.getParentQuery());
    }

    /**
     * @param sub answer
     * @return entity types inferred from answer entity information
     */
    private Set<Type> inferEntityTypes(Answer sub) {
        if (sub.isEmpty()) return Collections.emptySet();

        Set<Var> subbedVars = Sets.intersection(getRolePlayers(), sub.keySet());
        Set<Var> untypedVars = Sets.difference(subbedVars, getParentQuery().getVarOntologyConceptMap().keySet());
        return untypedVars.stream()
                .map(v -> new Pair<>(v, sub.get(v)))
                .filter(p -> p.getValue().isThing())
                .map(e -> {
                    Concept c = e.getValue();
                    return c.asThing().type();
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
        if (getPredicate() != null) return Collections.singletonList(getOntologyConcept().asRelationType());

        //look at available role types
        Multimap<RelationType, Role> compatibleTypesFromRoles = getCompatibleRelationTypesWithRoles(getExplicitRoleTypes(), new RoleTypeConverter());

        //look at entity types
        Map<Var, OntologyConcept> varTypeMap = getParentQuery().getVarOntologyConceptMap();

        //explicit types
        Set<OntologyConcept> types = getRolePlayers().stream()
                .filter(varTypeMap::containsKey)
                .map(varTypeMap::get)
                .collect(toSet());

        //types deduced from substitution
        inferEntityTypes(sub).forEach(types::add);

        Multimap<RelationType, Role> compatibleTypesFromTypes = getCompatibleRelationTypesWithRoles(types, new OntologyConceptConverterImpl());

        Multimap<RelationType, Role> compatibleTypes;
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

    /**
     * attempt to infer the relation type of this relation
     * @param sub extra instance information to aid entity type inference
     * @return either this if relation type can't be inferred or a fresh relation with inferred relation type
     */
    private RelationAtom inferRelationType(Answer sub){
        if (getPredicate() != null) return this;

        List<RelationType> relationTypes = inferPossibleRelationTypes(sub);
        if (relationTypes.size() == 1){
            return addType(relationTypes.iterator().next());
        } else {
            return this;
        }
    }

    @Override
    public Atom inferTypes() {
        return this
                .inferRelationType(new QueryAnswer())
                .inferRoleTypes();
    }

    @Override
    public Set<Var> getVarNames() {
        Set<Var> vars = super.getVarNames();
        vars.addAll(getRolePlayers());
        //add user specified role type vars
        getRelationPlayers().stream()
                .map(RelationPlayer::getRole)
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

    private Set<Var> getSpecificRolePlayers() {
        return getRoleVarMap().entries().stream()
                .filter(e -> !Schema.MetaSchema.isMetaLabel(e.getKey().getLabel()))
                .map(Map.Entry::getValue)
                .collect(toSet());
    }

    /**
     * @return set constituting the role player var names that do not have a specified role type
     */
    private Set<Var> getNonSpecificRolePlayers() {
        Set<Var> unmappedVars = getRolePlayers();
        unmappedVars.removeAll(getSpecificRolePlayers());
        return unmappedVars;
    }

    @Override
    public Set<TypeAtom> getSpecificTypeConstraints() {
        Set<Var> mappedVars = getSpecificRolePlayers();
        return getTypeConstraints().stream()
                .filter(t -> mappedVars.contains(t.getVarName()))
                .filter(t -> Objects.nonNull(t.getOntologyConcept()))
                .collect(toSet());
    }

    @Override
    public Set<Unifier> getPermutationUnifiers(Atom headAtom) {
        if (!headAtom.isRelation()) return Collections.singleton(new UnifierImpl());

        //if this atom is a match all atom, add type from rule head and find unmapped roles
        RelationAtom relAtom = getPredicateVariable().getValue().isEmpty() ? this.addType(headAtom.getOntologyConcept()) : this;
        List<Var> permuteVars = new ArrayList<>(relAtom.getNonSpecificRolePlayers());
        if (permuteVars.isEmpty()) return Collections.singleton(new UnifierImpl());

        List<List<Var>> varPermutations = getListPermutations(
                new ArrayList<>(permuteVars)).stream()
                .filter(l -> !l.isEmpty())
                .collect(Collectors.toList()
                );
        return getUnifiersFromPermutations(permuteVars, varPermutations);
    }

    /**
     * attempt to infer role types of this relation and return a fresh relation with inferred role types
     * @return either this if nothing/no roles can be inferred or fresh relation with inferred role types
     */
    private RelationAtom inferRoleTypes(){
        if (getExplicitRoleTypes().size() == getRelationPlayers().size() || getOntologyConcept() == null) return this;

        GraknGraph graph = getParentQuery().graph();
        Role metaRole = graph.admin().getMetaRole();
        RelationType relType = (RelationType) getOntologyConcept();
        Map<Var, OntologyConcept> varOntologyConceptMap = getParentQuery().getVarOntologyConceptMap();

        List<RelationPlayer> allocatedRelationPlayers = new ArrayList<>();

        //explicit role types from castings
        List<Pair<Var, VarPattern>> rolePlayerMappings = new ArrayList<>();
        getRelationPlayers().forEach(c -> {
            Var varName = c.getRolePlayer().getVarName();
            VarPatternAdmin role = c.getRole().orElse(null);
            if (role != null) {
                rolePlayerMappings.add(new Pair<>(varName, role));
                //try directly
                Label typeLabel = role.getTypeLabel().orElse(null);
                Role roleType = typeLabel != null ? graph.getRole(typeLabel.getValue()) : null;

                //try indirectly
                if (roleType == null && role.getVarName().isUserDefinedName()) {
                    IdPredicate rolePredicate = ((ReasonerQueryImpl) getParentQuery()).getIdPredicate(role.getVarName());
                    if (rolePredicate != null) roleType = graph.getConcept(rolePredicate.getPredicate());
                }
                allocatedRelationPlayers.add(c);
            }
        });

        //remaining roles
        //role types can repeat so no matter what has been allocated still the full spectrum of possibilities is present
        //TODO make restrictions based on cardinality constraints
        Set<Role> possibleRoles = Sets.newHashSet(relType.relates());

        //possible role types for each casting based on its type
        Map<RelationPlayer, Set<Role>> mappings = new HashMap<>();
        getRelationPlayers().stream()
                .filter(rp -> !allocatedRelationPlayers.contains(rp))
                .forEach(casting -> {
                    Var varName = casting.getRolePlayer().getVarName();
                    OntologyConcept ontologyConcept = varOntologyConceptMap.get(varName);
                    if (ontologyConcept != null && !Schema.MetaSchema.isMetaLabel(ontologyConcept.getLabel()) && ontologyConcept.isType()) {
                        mappings.put(casting, ReasonerUtils.getCompatibleRoleTypes(ontologyConcept.asType(), possibleRoles));
                    } else {
                        mappings.put(casting, ReasonerUtils.getOntologyConcepts(possibleRoles));
                    }
                });


        //resolve ambiguities until no unambiguous mapping exist
        while( mappings.values().stream().filter(s -> s.size() == 1).count() != 0) {
            Map.Entry<RelationPlayer, Set<Role>> entry = mappings.entrySet().stream()
                    .filter(e -> e.getValue().size() == 1)
                    .findFirst().orElse(null);

            RelationPlayer casting = entry.getKey();
            Var varName = casting.getRolePlayer().getVarName();
            Role role = entry.getValue().iterator().next();
            VarPatternAdmin roleVar = Graql.var().label(role.getLabel()).admin();

            //TODO remove from all mappings if it follows from cardinality constraints
            mappings.get(casting).remove(role);

            rolePlayerMappings.add(new Pair<>(varName, roleVar));
            allocatedRelationPlayers.add(casting);
        }

        //fill in unallocated roles with metarole
        VarPatternAdmin metaRoleVar = Graql.var().label(metaRole.getLabel()).admin();
        getRelationPlayers().stream()
                .filter(rp -> !allocatedRelationPlayers.contains(rp))
                .forEach(casting -> {
                    Var varName = casting.getRolePlayer().getVarName();
                    rolePlayerMappings.add(new Pair<>(varName, metaRoleVar));
                });

        PatternAdmin newPattern = constructRelationVarPattern(getVarName(), getPredicateVariable(), rolePlayerMappings);
        return new RelationAtom(newPattern.asVar(), getPredicateVariable(), getPredicate(), getParentQuery());
    }

    /**
     * @return map containing roleType - (rolePlayer var - rolePlayer type) pairs
     */
    private Multimap<Role, Var> computeRoleVarMap() {
        Multimap<Role, Var> roleMap = ArrayListMultimap.create();
        if (getParentQuery() == null || getOntologyConcept() == null){ return roleMap;}

        GraknGraph graph = getParentQuery().graph();
        getRelationPlayers().forEach(c -> {
            Var varName = c.getRolePlayer().getVarName();
            VarPatternAdmin role = c.getRole().orElse(null);
            if (role != null) {
                //try directly
                Label typeLabel = role.getTypeLabel().orElse(null);
                Role roleType = typeLabel != null ? graph.getRole(typeLabel.getValue()) : null;
                //try indirectly
                if (roleType == null && role.getVarName().isUserDefinedName()) {
                    IdPredicate rolePredicate = ((ReasonerQueryImpl) getParentQuery()).getIdPredicate(role.getVarName());
                    if (rolePredicate != null) roleType = graph.getConcept(rolePredicate.getPredicate());
                }
                if (roleType != null) roleMap.put(roleType, varName);
            }
        });
        return roleMap;
    }

    public Multimap<Role, Var> getRoleVarMap() {
        if (roleVarMap == null){
            roleVarMap = computeRoleVarMap();
        }
        return roleVarMap;
    }

    private Multimap<Role, RelationPlayer> getRoleRelationPlayerMap(){
        Multimap<Role, RelationPlayer> roleRelationPlayerMap = ArrayListMultimap.create();
        Multimap<Role, Var> roleVarTypeMap = getRoleVarMap();
        List<RelationPlayer> relationPlayers = getRelationPlayers();
        roleVarTypeMap.asMap().entrySet()
                .forEach(e -> {
                    Role role = e.getKey();
                    Label roleLabel = role.getLabel();
                    relationPlayers.stream()
                            .filter(rp -> rp.getRole().isPresent())
                            .forEach(rp -> {
                                VarPatternAdmin roleTypeVar = rp.getRole().orElse(null);
                                Label rl = roleTypeVar != null ? roleTypeVar.getTypeLabel().orElse(null) : null;
                                if (roleLabel != null && roleLabel.equals(rl)) {
                                    roleRelationPlayerMap.put(role, rp);
                                }
                            });
                });
        return roleRelationPlayerMap;
    }

    private List<Pair<RelationPlayer, RelationPlayer>> getRelationPlayerMappings(RelationAtom parentAtom) {
        List<Pair<RelationPlayer, RelationPlayer>> rolePlayerMappings = new ArrayList<>();

        //establish compatible castings for each parent casting
        List<Pair<RelationPlayer, List<RelationPlayer>>> compatibleMappings = new ArrayList<>();
        parentAtom.getRoleRelationPlayerMap();
        Multimap<Role, RelationPlayer> childRoleRPMap = getRoleRelationPlayerMap();
        Map<Var, OntologyConcept> parentVarOntologyConceptMap = parentAtom.getParentQuery().getVarOntologyConceptMap();
        Map<Var, OntologyConcept> childVarOntologyConceptMap = this.getParentQuery().getVarOntologyConceptMap();

        Set<Role> relationRoles = new HashSet<>(getOntologyConcept().asRelationType().relates());
        Set<Role> childRoles = new HashSet<>(childRoleRPMap.keySet());

        parentAtom.getRelationPlayers().stream()
                .filter(prp -> prp.getRole().isPresent())
                .forEach(prp -> {
                    VarPatternAdmin parentRoleTypeVar = prp.getRole().orElse(null);
                    Label parentRoleLabel = parentRoleTypeVar.getTypeLabel().orElse(null);

                    //TODO take into account indirect roles
                    Role parentRole = parentRoleLabel != null ? graph().getOntologyConcept(parentRoleLabel) : null;

                    if (parentRole != null) {
                        boolean isMetaRole = Schema.MetaSchema.isMetaLabel(parentRole.getLabel());
                        Var parentRolePlayer = prp.getRolePlayer().getVarName();
                        OntologyConcept parent = parentVarOntologyConceptMap.get(parentRolePlayer);

                        Set<Role> compatibleChildRoles = isMetaRole? childRoles : Sets.intersection(new HashSet<>(parentRole.subs()), childRoles);

                        if (parent != null && parent.isType()){
                            boolean isMetaType = Schema.MetaSchema.isMetaLabel(parent.getLabel());
                            Set<Role> typeRoles = isMetaType? childRoles : new HashSet<>(parent.asType().plays());

                            //incompatible type
                            if (Sets.intersection(relationRoles, typeRoles).isEmpty()) compatibleChildRoles = new HashSet<>();
                            else {
                                compatibleChildRoles = compatibleChildRoles.stream()
                                        .filter(rc -> Schema.MetaSchema.isMetaLabel(rc.getLabel()) || typeRoles.contains(rc))
                                        .collect(toSet());
                            }
                        }

                        List<RelationPlayer> compatibleRelationPlayers = new ArrayList<>();
                        compatibleChildRoles.stream()
                                .filter(childRoleRPMap::containsKey)
                                .forEach(r -> {
                                    Collection<RelationPlayer> childRPs = parent != null ?
                                            childRoleRPMap.get(r).stream()
                                                    .filter(rp -> {
                                                        Var childRolePlayer = rp.getRolePlayer().getVarName();
                                                        OntologyConcept childType = childVarOntologyConceptMap.get(childRolePlayer);
                                                        return childType == null || !checkDisjoint(parent, childType);
                                                    }).collect(Collectors.toList()) :
                                            childRoleRPMap.get(r);

                                    childRPs.forEach(compatibleRelationPlayers::add);
                                });
                        compatibleMappings.add(new Pair<>(prp, compatibleRelationPlayers));
                    }
                });

        //self-consistent procedure until no non-empty mappings present
        while( compatibleMappings.stream().map(Pair::getValue).filter(s -> !s.isEmpty()).count() > 0) {
            //find optimal parent-child RP pair
            Pair<RelationPlayer, RelationPlayer> rpPair = compatibleMappings.stream()
                    .filter(e -> e.getValue().size() == 1).map(e -> new Pair<>(e.getKey(), e.getValue().iterator().next()))
                    .findFirst().orElse(
                            compatibleMappings.stream()
                                    .flatMap(e -> e.getValue().stream().map(childRP -> new Pair<>(e.getKey(), childRP)))
                                    //prioritise mappings with equivalent types and unambiguous mappings
                                    .sorted(Comparator.comparing(e -> {
                                        OntologyConcept parentType = parentVarOntologyConceptMap.get(e.getKey().getRolePlayer().getVarName());
                                        OntologyConcept childType = childVarOntologyConceptMap.get(e.getValue().getRolePlayer().getVarName());
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
                                    .findFirst().orElse(null)
                    );

            RelationPlayer parentCasting = rpPair.getKey();
            RelationPlayer childCasting = rpPair.getValue();
            rolePlayerMappings.add(new Pair<>(childCasting, parentCasting));

            //remove corresponding entries
            Pair<RelationPlayer, List<RelationPlayer>> entryToRemove = compatibleMappings.stream()
                    .filter(e -> e.getKey() == parentCasting)
                    .findFirst().orElse(null);
            compatibleMappings.remove(entryToRemove);
            compatibleMappings.stream()
                    .filter(e -> e.getValue().contains(childCasting))
                    .forEach(e -> e.getValue().remove(childCasting));
        }
        return rolePlayerMappings;
    }

    @Override
    public Unifier getUnifier(Atom pAtom) {
        if (this.equals(pAtom)) return new UnifierImpl();

        Unifier unifier = super.getUnifier(pAtom);
        if (pAtom.isRelation()) {
            RelationAtom parentAtom = (RelationAtom) pAtom;

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
            VarPatternAdmin roleType = c.getRole().orElse(null);
            if (roleType != null) {
                relVar = relVar.rel(roleType, c.getRolePlayer());
            } else {
                relVar = relVar.rel(c.getRolePlayer());
            }
        }
        return new RelationAtom(relVar.admin(), getPredicateVariable(), getPredicate(), getParentQuery());
    }
}
