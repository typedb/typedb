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
import ai.grakn.graql.internal.reasoner.Utility;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.atom.AtomBase;
import ai.grakn.graql.internal.reasoner.atom.AtomicFactory;
import ai.grakn.graql.internal.reasoner.atom.ResolutionStrategy;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;
import ai.grakn.graql.internal.reasoner.atom.predicate.Predicate;
import ai.grakn.graql.internal.reasoner.query.QueryAnswers;
import ai.grakn.graql.internal.reasoner.query.ReasonerAtomicQuery;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueryImpl;
import ai.grakn.graql.internal.reasoner.query.UnifierImpl;
import ai.grakn.graql.internal.reasoner.rule.InferenceRule;
import ai.grakn.graql.internal.util.CommonUtil;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.Schema;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import javafx.util.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static ai.grakn.graql.internal.reasoner.Utility.capture;
import static ai.grakn.graql.internal.reasoner.Utility.checkTypesDisjoint;
import static ai.grakn.graql.internal.reasoner.Utility.getCompatibleRelationTypes;
import static ai.grakn.graql.internal.reasoner.Utility.getListPermutations;
import static ai.grakn.graql.internal.reasoner.Utility.getUnifiersFromPermutations;
import static ai.grakn.graql.internal.reasoner.Utility.roleToRelationTypes;
import static ai.grakn.graql.internal.reasoner.Utility.typeToRelationTypes;
import static ai.grakn.graql.internal.util.CommonUtil.toImmutableMultiset;
import static java.util.stream.Collectors.toSet;

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

    private int hashCode = 0;
    private Multimap<RoleType, Pair<VarName, Type>> roleVarTypeMap = null;
    private Multimap<RoleType, String> roleConceptIdMap = null;

    public Relation(VarAdmin pattern, IdPredicate predicate, ReasonerQuery par) {
        super(pattern, predicate, par);
    }

    public Relation(VarName name, VarName typeVariable, Map<VarName, Var> roleMap, IdPredicate pred, ReasonerQuery par) {
        super(constructRelationVar(name, typeVariable, roleMap), pred, par);
    }

    private Relation(Relation a) {
        super(a);
    }

    public Set<RelationPlayer> getRelationPlayers() {
        Set<RelationPlayer> rps = new HashSet<>();
        this.atomPattern.asVar().getProperty(RelationProperty.class)
                .ifPresent(prop -> prop.getRelationPlayers().forEach(rps::add));
        return rps;
    }

    private void modifyRelationPlayers(UnaryOperator<RelationPlayer> mapper) {
        this.atomPattern = this.atomPattern.asVar().mapProperty(RelationProperty.class,
                prop -> new RelationProperty(prop.getRelationPlayers().map(mapper).collect(toImmutableMultiset())));
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
        Var var;
        if (!varName.getValue().isEmpty()) var = Graql.var(varName);
        else var = Graql.var();
        for (Pair<VarName, Var> mapping : rolePlayerMappings) {
            VarName rp = mapping.getKey();
            Var role = mapping.getValue();
            if (role == null) var = var.rel(Graql.var(rp));
            else var = var.rel(role, Graql.var(rp));
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

    private boolean isRuleApplicableViaType(Relation headAtom) {
        Map<VarName, Type> varTypeMap = getParentQuery().getVarTypeMap();
        Multimap<RoleType, Pair<VarName, Type>> childRoleMap = headAtom.getRoleVarTypeMap();

        //TODO consider adding type and reusing ViaAtom applicability

        //rule not applicable if there's an empty role intersection
        //each role player without a role or type corresponds to role metatype - role wildcard
        List<RoleType> mappedRoles = new ArrayList<>();
        Set<RoleType> roles = childRoleMap.keySet();
        int roleWildcards = 0;
        for (VarName rolePlayer : getRolePlayers()){
            Type type = varTypeMap.get(rolePlayer);
            if (type != null && !Schema.MetaSchema.isMetaLabel(type.getLabel())) {
                Set<RoleType> roleIntersection = new HashSet<>(roles);
                roleIntersection.retainAll(type.plays());
                if (roleIntersection.isEmpty()){
                    return false;
                } else {
                    //child types corresponding to matched role types
                    Set<Type> childTypes = roleIntersection.stream()
                            .filter(childRoleMap::containsKey)
                            .flatMap(r -> childRoleMap.get(r).stream())
                            .map(Pair::getValue)
                            .filter(Objects::nonNull)
                            .collect(toSet());
                    //check if from all possible mappings at least one doesn't lead to type contradiction
                    if (!childTypes.isEmpty()
                            && childTypes.stream().filter(t -> !checkTypesDisjoint(type, t)).count() == 0){
                        return false;
                    }
                    mappedRoles.addAll(roleIntersection);
                }
            } else {
                roleWildcards++;
            }
        }

        //rule not applicable if not a single mapping between all relation players and role types can be found
        //>= takes into account the case when the parent has less relation players than child (rule head)
        return mappedRoles.size() + roleWildcards >= getRolePlayers().size();
    }

    private boolean isRuleApplicableViaAtom(Relation headAtom, ReasonerQueryImpl ruleBody) {
        ReasonerQueryImpl parent = (ReasonerQueryImpl) getParentQuery();

        //get unifier for role players
        Unifier unificationMappings = getRelationPlayerMappings(
                headAtom.getRoleMap(),
                getRoleMap(),
                headAtom.getRelationPlayers().stream().map(rp -> rp.getRolePlayer().getVarName()).collect(Collectors.toList()),
                getRelationPlayers().stream().map(rp -> rp.getRolePlayer().getVarName()).collect(Collectors.toList())
        );

        //case when child atom non-unifiable - not all parent variables mapped
        if (unificationMappings.size() < this.getRolePlayers().size()) return false;

        //look at type and predicate compatibility
        Map<VarName, Type> parentVarTypeMap = parent.getVarTypeMap();
        Map<VarName, Type> childVarTypeMap = ruleBody.getVarTypeMap();

        for(VarName chVar : unificationMappings.keySet()){
            VarName pVar = unificationMappings.get(chVar);
            Type chType = childVarTypeMap.get(chVar);
            Type pType = parentVarTypeMap.get(pVar);
            //check type compatibility
            if (chType != null && pType != null) {
                if (checkTypesDisjoint(pType, chType)) {
                    return false;
                }
                //Check for any constraints on the variables
                Predicate childPredicate = ruleBody.getIdPredicate(chVar);
                Predicate parentPredicate = parent.getIdPredicate(pVar);
                if (childPredicate != null
                        && parentPredicate != null
                        && !childPredicate.getPredicateValue().equals(parentPredicate.getPredicateValue())) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    protected boolean isRuleApplicable(InferenceRule child) {
        Atom ruleAtom = child.getRuleConclusionAtom();
        if (!(ruleAtom instanceof Relation)) return false;

        Relation headAtom = (Relation) ruleAtom;
        //discard if child has less rolePlayers
        if (headAtom.getRelationPlayers().size() < this.getRelationPlayers().size()) return false;

        Type type = getType();
        //Case: relation without type - match all
        if (type == null) {
            return isRuleApplicableViaType(headAtom);
        } else {
            return isRuleApplicableViaAtom(headAtom, child.getBody());
        }
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
        GraknGraph graph = getParentQuery().graph();
        getRelationPlayers().stream()
                .map(RelationPlayer::getRoleType)
                .flatMap(CommonUtil::optionalToStream)
                .map(VarAdmin::getTypeLabel)
                .flatMap(CommonUtil::optionalToStream)
                .map(graph::<RoleType>getType)
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
        Set<Type> compatibleTypes = Utility.getTopTypes(
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

    private void inferRelationTypeFromRelates() {
        ReasonerQueryImpl parent = (ReasonerQueryImpl) getParentQuery();
        VarName valueVariable = getValueVariable();
        TypeAtom hrAtom = parent.getAtoms().stream()
                .filter(at -> at.getVarName().equals(valueVariable))
                .filter(Atomic::isAtom).map(at -> (Atom) at)
                .filter(Atom::isType).map(at -> (TypeAtom) at)
                .findFirst().orElse(null);
        if (hrAtom != null) {
            ReasonerAtomicQuery hrQuery = new ReasonerAtomicQuery(hrAtom);
            QueryAnswers answers = new QueryAnswers(hrQuery.DBlookup().collect(toSet()));
            if (answers.size() == 1) {
                IdPredicate newPredicate = new IdPredicate(IdPredicate.createIdVar(hrAtom.getVarName(),
                        answers.stream().findFirst().orElse(null).get(hrAtom.getVarName()).getId()), parent);

                Relation newRelation = new Relation(getPattern().asVar(), newPredicate, parent);
                parent.removeAtomic(hrAtom.getPredicate());
                parent.removeAtomic(hrAtom);
                parent.removeAtomic(this);
                parent.addAtomic(newRelation);
                parent.addAtomic(newPredicate);
            }
        }
    }

    @Override
    public void inferTypes() {
        if (getPredicate() == null) inferRelationTypeFromTypes();
        if (getPredicate() == null) inferRelationTypeFromRelates();
    }

    @Override
    public boolean containsVar(VarName name) {
        boolean varFound = false;
        Iterator<RelationPlayer> it = getRelationPlayers().iterator();
        while (it.hasNext() && !varFound) {
            varFound = it.next().getRolePlayer().getVarName().equals(name);
        }
        return varFound;
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

    //move to relation
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
                        mappings.put(casting, Utility.getCompatibleRoleTypes(type, possibleRoles));
                    } else {
                        mappings.put(casting, Utility.getTopTypes(possibleRoles).stream().map(t -> (RoleType) t).collect(toSet()));
                    }
                });

        //resolve ambiguities until no  unambiguous mapping exist
        while( mappings.values().stream().filter(s -> s.size() == 1).count() != 0) {
            for (Map.Entry<RelationPlayer, Set<RoleType>> entry : mappings.entrySet()) {
                Set<RoleType> compatibleRoles = entry.getValue();
                if (compatibleRoles.size() == 1) {
                    RelationPlayer casting = entry.getKey();
                    VarName varName = casting.getRolePlayer().getVarName();
                    Type type = varTypeMap.get(varName);
                    RoleType roleType = entry.getValue().iterator().next();
                    VarAdmin roleVar = Graql.var().label(roleType.getLabel()).admin();
                    mappings.values().forEach(s -> s.remove(roleType));

                    rolePlayerMappings.add(new Pair<>(varName, roleVar));
                    roleVarTypeMap.put(roleType, new Pair<>(varName, type));
                    allocatedRelationPlayers.add(casting);
                }
            }
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
        return roleVarTypeMap;
    }

    @Override
    public Multimap<RoleType, Pair<VarName, Type>> getRoleVarTypeMap() {
        if (roleVarTypeMap == null) computeRoleVarTypeMap();
        return roleVarTypeMap;
    }

    /**
     * @return map of role variable - role type from a predicate
     */
    //TODO
    /*
    @SuppressWarnings("unchecked")
    private Multimap<RoleType, VarName> getIndirectRoleMap() {
        ReasonerQueryImpl parent = (ReasonerQueryImpl) getParentQuery();
        GraknGraph graph = parent.graph();
        ArrayListMultimap<RoleType, VarName> indirectRoleMap = ArrayListMultimap.create();
        Object result = getRelationPlayers().stream()
                .map(RelationPlayer::getRoleType)
                .flatMap(CommonUtil::optionalToStream)
                .map(rt -> new Pair<>(rt, parent.getIdPredicate(rt.getVarName())))
                .filter(e -> e.getValue() != null)
                .collect(Collectors.toMap(e -> graph.getConcept(e.getValue().getPredicate()), e -> e.getKey().getVarName()));
        return (Map<RoleType, VarName>)result;
    }
    */

    //varsToAllocate <= childBVs
    private Unifier getRelationPlayerMappings(Multimap<RoleType, VarName> childMap,
                                              Multimap<RoleType, VarName> parentMap,
                                              List<VarName> childVars,
                                              List<VarName> parentVars) {
        Unifier unifier = new UnifierImpl();

        //case when all parent roles are meta roles (not specified)
        Set<RoleType> parentRoles = parentMap.keySet();
        if (parentRoles.size() == 1 && Schema.MetaSchema.isMetaLabel(parentRoles.iterator().next().getLabel())) {
            Iterator<VarName> cit = childVars.iterator();
            for (VarName pVar : parentVars) {
                VarName chVar = cit.next();
                unifier.addMapping(chVar, pVar);
            }
            return unifier;
        }

        //roles satisfy P >= C in terms of generality
        //self-consistent procedure
        List<RoleType> rolesToAllocate = childMap.entries().stream().map(Map.Entry::getKey).collect(Collectors.toList());
        while(!rolesToAllocate.isEmpty()) {
            parentMap.asMap().entrySet()
                    .forEach(entry -> {
                        RoleType parentRole = entry.getKey();

                        Set<RoleType> rolesAvailable = new HashSet<>(rolesToAllocate);
                        Set<RoleType> compatibleChildRoles = !Schema.MetaSchema.isMetaLabel(parentRole.getLabel()) ?
                                Sets.intersection(new HashSet<>(parentRole.subTypes()), rolesAvailable) : rolesAvailable;
                        if (compatibleChildRoles.size() == 1) {
                            RoleType childRole = compatibleChildRoles.iterator().next();

                            Iterator<VarName> pVars = entry.getValue().iterator();
                            Iterator<VarName> chVars = childMap.get(childRole).iterator();
                            while (chVars.hasNext() && pVars.hasNext()) {
                                unifier.addMapping(chVars.next(), pVars.next());
                                rolesToAllocate.remove(childRole);
                            }
                        }
                        else if (compatibleChildRoles.isEmpty()){
                            rolesToAllocate.clear();
                        }
                    });
        }
        return unifier;
    }

    //TODO
/*
    private Unifier getRoleTypeUnifier(Relation parentAtom) {
        Map<RoleType, VarName> childMap = getIndirectRoleMap();
        Map<RoleType, VarName> parentMap = parentAtom.getIndirectRoleMap();
        return getRelationPlayerMappings(
                childMap,
                parentMap,
                Lists.newArrayList(childMap.values()),
                Lists.newArrayList(parentMap.values()));
    }
    */
    private Unifier getRolePlayerUnifier(Relation parentAtom) {
        Multimap<RoleType, VarName> childMap = getRoleMap();
        Multimap<RoleType, VarName> parentMap = parentAtom.getRoleMap();
        return getRelationPlayerMappings(
                childMap,
                parentMap,
                getRelationPlayers().stream().map(rp -> rp.getRolePlayer().getVarName()).collect(Collectors.toList()),
                parentAtom.getRelationPlayers().stream().map(rp -> rp.getRolePlayer().getVarName()).collect(Collectors.toList()));
    }

    @Override
    public Unifier getUnifier(Atomic pAtom) {
        if (!(pAtom instanceof TypeAtom)) {
            throw new IllegalArgumentException(ErrorMessage.UNIFICATION_ATOM_INCOMPATIBILITY.getMessage());
        }

        Unifier unifier = super.getUnifier(pAtom);
        if (((Atom) pAtom).isRelation()) {
            Relation parentAtom = (Relation) pAtom;
            //get role player unifiers
            unifier.merge(getRolePlayerUnifier(parentAtom));
            //get role type unifiers
            //TODO
            //unifier.merge(getRoleTypeUnifier(parentAtom));
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
