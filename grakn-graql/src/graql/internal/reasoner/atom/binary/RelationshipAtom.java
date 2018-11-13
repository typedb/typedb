/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package grakn.core.graql.internal.reasoner.atom.binary;

import grakn.core.server.Transaction;
import grakn.core.concept.Concept;
import grakn.core.concept.ConceptId;
import grakn.core.concept.EntityType;
import grakn.core.concept.Label;
import grakn.core.concept.Relationship;
import grakn.core.concept.RelationshipType;
import grakn.core.concept.Role;
import grakn.core.concept.Rule;
import grakn.core.concept.SchemaConcept;
import grakn.core.concept.Type;
import grakn.core.server.exception.GraqlQueryException;
import grakn.core.graql.Graql;
import grakn.core.graql.Pattern;
import grakn.core.graql.Var;
import grakn.core.graql.VarPattern;
import grakn.core.graql.answer.ConceptMap;
import grakn.core.graql.admin.Atomic;
import grakn.core.graql.admin.MultiUnifier;
import grakn.core.graql.admin.ReasonerQuery;
import grakn.core.graql.admin.RelationPlayer;
import grakn.core.graql.admin.Unifier;
import grakn.core.graql.admin.UnifierComparison;
import grakn.core.graql.admin.VarPatternAdmin;
import grakn.core.graql.admin.VarProperty;
import grakn.core.graql.internal.pattern.property.IsaProperty;
import grakn.core.graql.internal.pattern.property.RelationshipProperty;
import grakn.core.graql.internal.query.answer.ConceptMapImpl;
import grakn.core.graql.internal.reasoner.unifier.MultiUnifierImpl;
import grakn.core.graql.internal.reasoner.unifier.UnifierImpl;
import grakn.core.graql.internal.reasoner.unifier.UnifierType;
import grakn.core.graql.internal.reasoner.atom.Atom;
import grakn.core.graql.internal.reasoner.atom.AtomicEquivalence;
import grakn.core.graql.internal.reasoner.atom.predicate.IdPredicate;
import grakn.core.graql.internal.reasoner.atom.predicate.NeqPredicate;
import grakn.core.graql.internal.reasoner.atom.predicate.Predicate;
import grakn.core.graql.internal.reasoner.atom.predicate.ValuePredicate;
import grakn.core.graql.internal.reasoner.query.ReasonerQueryImpl;
import grakn.core.graql.internal.reasoner.utils.Pair;
import grakn.core.graql.internal.reasoner.utils.ReasonerUtils;
import grakn.core.graql.internal.reasoner.utils.conversion.RoleConverter;
import grakn.core.graql.internal.reasoner.utils.conversion.TypeConverter;
import grakn.core.server.kb.internal.concept.RelationshipTypeImpl;
import grakn.core.util.CommonUtil;
import grakn.core.util.ErrorMessage;
import grakn.core.graql.internal.Schema;
import com.google.auto.value.AutoValue;
import com.google.auto.value.extension.memoized.Memoized;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import java.util.function.BiFunction;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static grakn.core.graql.internal.reasoner.utils.ReasonerUtils.compatibleRelationTypesWithRoles;
import static grakn.core.graql.internal.reasoner.utils.ReasonerUtils.compatibleRoles;
import static grakn.core.graql.internal.reasoner.utils.ReasonerUtils.multimapIntersection;
import static grakn.core.graql.internal.reasoner.utils.ReasonerUtils.supers;
import static grakn.core.graql.internal.reasoner.utils.ReasonerUtils.top;
import static java.util.stream.Collectors.toSet;

/**
 *
 * <p>
 * Atom implementation defining a relation atom corresponding to a combined {@link RelationshipProperty}
 * and (optional) {@link IsaProperty}. The relation atom is a {@link TypeAtom} with relationship players.
 * </p>
 *
 *
 */
@AutoValue
public abstract class RelationshipAtom extends IsaAtomBase {

    public abstract ImmutableList<RelationPlayer> getRelationPlayers();
    public abstract ImmutableSet<Label> getRoleLabels();

    private ImmutableList<SchemaConcept> possibleTypes = null;

    public static RelationshipAtom create(VarPattern pattern, Var predicateVar, @Nullable ConceptId predicateId, ReasonerQuery parent) {
        List<RelationPlayer> rps = new ArrayList<>();
        pattern.admin()
                .getProperty(RelationshipProperty.class)
                .ifPresent(prop -> prop.relationPlayers().stream()
                        .sorted(Comparator.comparing(Object::hashCode))
                        .forEach(rps::add)
                );
        ImmutableList<RelationPlayer> relationPlayers = ImmutableList.copyOf(rps);
        ImmutableSet<Label> roleLabels = ImmutableSet.<Label>builder().addAll(
                relationPlayers.stream()
                        .map(RelationPlayer::getRole)
                        .flatMap(CommonUtil::optionalToStream)
                        .map(VarPatternAdmin::getTypeLabel)
                        .flatMap(CommonUtil::optionalToStream)
                        .iterator()
        ).build();
        return new AutoValue_RelationshipAtom(pattern.admin().var(), pattern, parent, predicateVar, predicateId, relationPlayers, roleLabels);
    }

    private static RelationshipAtom create(VarPattern pattern, Var predicateVar, @Nullable ConceptId predicateId, @Nullable ImmutableList<SchemaConcept> possibleTypes, ReasonerQuery parent) {
        RelationshipAtom atom = create(pattern, predicateVar, predicateId, parent);
        atom.possibleTypes = possibleTypes;
        return atom;
    }

    private static RelationshipAtom create(RelationshipAtom a, ReasonerQuery parent) {
        RelationshipAtom atom = new AutoValue_RelationshipAtom( a.getVarName(), a.getPattern(), parent, a.getPredicateVariable(), a.getTypeId(), a.getRelationPlayers(), a.getRoleLabels());
        atom.applicableRules = a.applicableRules;
        atom.possibleTypes = a.possibleTypes;
        return atom;
    }

    //NB: overriding as these require a derived property
    @Override
    public final boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || this.getClass() != obj.getClass()) return false;
        RelationshipAtom that = (RelationshipAtom) obj;
        return Objects.equals(this.getTypeId(), that.getTypeId())
                && this.isUserDefined() == that.isUserDefined()
                && this.isDirect() == that.isDirect()
                && this.getVarNames().equals(that.getVarNames())
                && this.getRelationPlayers().equals(that.getRelationPlayers());
    }

    @Memoized
    @Override
    public int hashCode() {
        int hashCode = 1;
        hashCode = hashCode * 37 + (getTypeId() != null ? getTypeId().hashCode() : 0);
        hashCode = hashCode * 37 + getVarNames().hashCode();
        hashCode = hashCode * 37 + getRelationPlayers().hashCode();
        return hashCode;
    }

    @Override
    public Class<? extends VarProperty> getVarPropertyClass(){ return RelationshipProperty.class;}

    @Override
    public void checkValid(){
        super.checkValid();
        getRoleLabels().stream()
                .filter(label -> tx().getRole(label.getValue()) == null)
                .findFirst()
                .ifPresent(label -> {
                    throw GraqlQueryException.labelNotFound(label);
                });
    }

    @Override
    public RelationshipAtom toRelationshipAtom(){ return this;}

    @Override
    public IsaAtom toIsaAtom(){ return IsaAtom.create(getVarName(), getPredicateVariable(), getTypeId(), false, getParentQuery()); }

    @Override
    public Set<Atom> rewriteToAtoms(){
        return this.getRelationPlayers().stream()
                .map(rp -> create(relationPattern(getVarName(), Sets.newHashSet(rp)), getPredicateVariable(), getTypeId(), null, this.getParentQuery()))
                .collect(toSet());
    }

    @Override
    public String toString(){
        String typeString = getSchemaConcept() != null?
                getSchemaConcept().label().getValue() :
                "{" + inferPossibleTypes(new ConceptMapImpl()).stream().map(rt -> rt.label().getValue()).collect(Collectors.joining(", ")) + "}";
        String relationString = (isUserDefined()? getVarName() + " ": "") +
                typeString +
                (getPredicateVariable().isUserDefinedName()? "(" + getPredicateVariable() + ")" : "") +
                (isDirect()? "!" : "") +
                getRelationPlayers().toString();
        return relationString + getPredicates(Predicate.class).map(Predicate::toString).collect(Collectors.joining(""));
    }

    @Override
    public Set<Var> getVarNames() {
        Set<Var> vars = super.getVarNames();
        vars.addAll(getRolePlayers());
        vars.addAll(getRoleVariables());
        return vars;
    }

    /**
     * @return set constituting the role player var names
     */
    private Set<Var> getRolePlayers() {
        return getRelationPlayers().stream().map(c -> c.getRolePlayer().var()).collect(toSet());
    }

    /**
     * @return set of user defined role variables if any
     */
    private Set<Var> getRoleVariables(){
        return getRelationPlayers().stream()
                .map(RelationPlayer::getRole)
                .flatMap(CommonUtil::optionalToStream)
                .map(VarPatternAdmin::var)
                .filter(Var::isUserDefinedName)
                .collect(Collectors.toSet());
    }

    private ConceptMap getRoleSubstitution(){
        Map<Var, Concept> roleSub = new HashMap<>();
        getRolePredicates().forEach(p -> roleSub.put(p.getVarName(), tx().getConcept(p.getPredicate())));
        return new ConceptMapImpl(roleSub);
    }

    @Override
    public Atomic copy(ReasonerQuery parent) {
        return create(this, parent);
    }

    @Override
    protected Pattern createCombinedPattern(){
        if (getPredicateVariable().isUserDefinedName()) return super.createCombinedPattern();
        return getSchemaConcept() == null?
                relationPattern() :
                isDirect()?
                        relationPattern().isaExplicit(getSchemaConcept().label().getValue()):
                        relationPattern().isa(getSchemaConcept().label().getValue());
    }

    private VarPattern relationPattern() {
        return relationPattern(getVarName(), getRelationPlayers());
    }

    /**
     * construct a $varName (rolemap) isa $typeVariable relation
     * @param varName            variable name
     * @param relationPlayers collection of rolePlayer-roleType mappings
     * @return corresponding {@link VarPatternAdmin}
     */
    private VarPattern relationPattern(Var varName, Collection<RelationPlayer> relationPlayers) {
        VarPattern var = varName;
        for (RelationPlayer rp : relationPlayers) {
            VarPatternAdmin rolePattern = rp.getRole().orElse(null);
            var = rolePattern != null? var.rel(rolePattern, rp.getRolePlayer()) : var.rel(rp.getRolePlayer());
        }
        return var.admin();
    }

    private boolean isBaseEquivalent(Object obj){
        if (obj == null || this.getClass() != obj.getClass()) return false;
        if (obj == this) return true;
        RelationshipAtom that = (RelationshipAtom) obj;
        return this. isUserDefined() == that.isUserDefined()
                && this.getPredicateVariable().isUserDefinedName() == that.getPredicateVariable().isUserDefinedName()
                && this.isDirect() == that.isDirect()
                && Objects.equals(this.getTypeId(), that.getTypeId())
                //check relation players equivalent
                && this.getRolePlayers().size() == that.getRolePlayers().size()
                && this.getRelationPlayers().size() == that.getRelationPlayers().size()
                && this.getRoleLabels().equals(that.getRoleLabels());
    }

    private int baseHashCode(){
        int baseHashCode = 1;
        baseHashCode = baseHashCode * 37 + (this.getTypeId() != null ? this.getTypeId().hashCode() : 0);
        baseHashCode = baseHashCode * 37 + this.getRoleLabels().hashCode();
        return baseHashCode;
    }

    @Override
    public boolean isAlphaEquivalent(Object obj) {
        if (!isBaseEquivalent(obj) || !super.isAlphaEquivalent(obj)) return false;
        RelationshipAtom that = (RelationshipAtom) obj;
        //check role-type and id predicate bindings
        return this.getRoleTypeMap().equals(that.getRoleTypeMap())
                && this.predicateBindingsAlphaEquivalent(that);
    }

    @Override
    public int alphaEquivalenceHashCode() {
        int equivalenceHashCode = baseHashCode();
        equivalenceHashCode = equivalenceHashCode * 37 + this.getRoleTypeMap().hashCode();
        equivalenceHashCode = equivalenceHashCode * 37 + this.getRoleConceptIdMap().hashCode();
        return equivalenceHashCode;
    }

    private boolean predicateBindingsEquivalent(RelationshipAtom atom,
                                                BiFunction<String, String, Boolean> conceptComparison,
                                                AtomicEquivalence equivalence) {
        Multimap<Role, String> thisIdMap = this.getRoleConceptIdMap();
        Multimap<Role, String> thatIdMap = atom.getRoleConceptIdMap();
        Multimap<Role, NeqPredicate> thisNeqMap = this.getRoleNeqPredicateMap();
        Multimap<Role, NeqPredicate> thatNeqMap = atom.getRoleNeqPredicateMap();
        Multimap<Role, ValuePredicate> thisValueMap = this.getRoleValueMap();
        Multimap<Role, ValuePredicate> thatValueMap = atom.getRoleValueMap();
        return thisIdMap.keySet().equals(thatIdMap.keySet())
                && thisIdMap.keySet().stream().allMatch(k -> ReasonerUtils.isEquivalentCollection(thisIdMap.get(k), thatIdMap.get(k), conceptComparison))
                && thisNeqMap.keySet().equals(thatNeqMap.keySet())
                && thisNeqMap.keySet().stream().allMatch(k -> ReasonerUtils.isEquivalentCollection(thisNeqMap.get(k), thatNeqMap.get(k), equivalence))
                && thisValueMap.keySet().equals(thatValueMap.keySet())
                && thisValueMap.keySet().stream().allMatch(k -> ReasonerUtils.isEquivalentCollection(thisValueMap.get(k), thatValueMap.get(k), equivalence));
    }

    private boolean predicateBindingsAlphaEquivalent(RelationshipAtom atom) {
        return predicateBindingsEquivalent(atom, String::equals, AtomicEquivalence.AlphaEquivalence);
    }

    private boolean predicateBindingsStructurallyEquivalent(RelationshipAtom atom) {
        return predicateBindingsEquivalent(atom, (a, b) -> true, AtomicEquivalence.StructuralEquivalence);
    }

    @Override
    public boolean isStructurallyEquivalent(Object obj) {
        if (!isBaseEquivalent(obj) || !super.isStructurallyEquivalent(obj)) return false;
        RelationshipAtom that = (RelationshipAtom) obj;
        // check bindings
        return this.getRoleTypeMap(false).equals(that.getRoleTypeMap(false))
                && this.predicateBindingsStructurallyEquivalent(that);
    }

    @Override
    public int structuralEquivalenceHashCode() {
        int equivalenceHashCode = baseHashCode();
        equivalenceHashCode = equivalenceHashCode * 37 + this.getRoleTypeMap(false).hashCode();
        equivalenceHashCode = equivalenceHashCode * 37 + this.getRoleConceptIdMap().keySet().hashCode();
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
        return getSchemaConcept() != null;
    }

    @Override
    public boolean requiresMaterialisation() {
        return isUserDefined();
    }

    @Override
    public boolean requiresRoleExpansion() {
        return !getRoleVariables().isEmpty();
    }

    @Override
    public Set<String> validateAsRuleHead(Rule rule){
        //can form a rule head if type is specified, type is not implicit and all relation players are insertable
        return Sets.union(super.validateAsRuleHead(rule), validateRelationPlayers(rule));
    }

    private Set<String> validateRelationPlayers(Rule rule){
        Set<String> errors = new HashSet<>();
        getRelationPlayers().forEach(rp -> {
            VarPatternAdmin role = rp.getRole().orElse(null);
            if (role == null){
                errors.add(ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_RELATION_WITH_AMBIGUOUS_ROLE.getMessage(rule.then(), rule.label()));
            } else {
                Label roleLabel = role.getTypeLabel().orElse(null);
                if (roleLabel == null){
                    errors.add(ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_RELATION_WITH_AMBIGUOUS_ROLE.getMessage(rule.then(), rule.label()));
                } else {
                    if (Schema.MetaSchema.isMetaLabel(roleLabel)) {
                        errors.add(ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_RELATION_WITH_AMBIGUOUS_ROLE.getMessage(rule.then(), rule.label()));
                    }
                    Role roleType = tx().getRole(roleLabel.getValue());
                    if (roleType != null && roleType.isImplicit()) {
                        errors.add(ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_RELATION_WITH_IMPLICIT_ROLE.getMessage(rule.then(), rule.label()));
                    }
                }
            }
        });
        return errors;
    }

    @Override
    public Set<String> validateOntologically() {
        Set<String> errors = new HashSet<>();
        SchemaConcept type = getSchemaConcept();
        if (type != null && !type.isRelationshipType()){
            errors.add(ErrorMessage.VALIDATION_RULE_INVALID_RELATION_TYPE.getMessage(type.label()));
            return errors;
        }

        //check role-type compatibility
        Map<Var, Type> varTypeMap = getParentQuery().getVarTypeMap();
        for (Map.Entry<Role, Collection<Var>> e : getRoleVarMap().asMap().entrySet() ){
            Role role = e.getKey();
            if (!Schema.MetaSchema.isMetaLabel(role.label())) {
                //check whether this role can be played in this relation
                if (type != null && type.asRelationshipType().roles().noneMatch(r -> r.equals(role))) {
                    errors.add(ErrorMessage.VALIDATION_RULE_ROLE_CANNOT_BE_PLAYED.getMessage(role.label(), type.label()));
                }

                //check whether the role player's type allows playing this role
                for (Var player : e.getValue()) {
                    Type playerType = varTypeMap.get(player);
                    if (playerType != null && playerType.playing().noneMatch(plays -> plays.equals(role))) {
                        errors.add(ErrorMessage.VALIDATION_RULE_TYPE_CANNOT_PLAY_ROLE.getMessage(playerType.label(), role.label(), type == null? "" : type.label()));
                    }
                }
            }
        }
        return errors;
    }

    @Override
    public Stream<IdPredicate> getPartialSubstitutions() {
        Set<Var> varNames = getVarNames();
        return getPredicates(IdPredicate.class)
                .filter(pred -> varNames.contains(pred.getVarName()));
    }

    public Stream<IdPredicate> getRolePredicates(){
        return getRelationPlayers().stream()
                .map(RelationPlayer::getRole)
                .flatMap(CommonUtil::optionalToStream)
                .filter(var -> var.var().isUserDefinedName())
                .filter(vp -> vp.getTypeLabel().isPresent())
                .map(vp -> {
                    Label label = vp.getTypeLabel().orElse(null);
                    return IdPredicate.create(vp.var(), tx().getRole(label.getValue()), getParentQuery());
                });
    }

    private <T extends Predicate> Multimap<Role, T> getRolePredicateMap(Class<T> type) {
        HashMultimap<Role, T> rolePredicateMap = HashMultimap.create();

        HashMultimap<Var, T> predicateMap = HashMultimap.create();
        getPredicates(type).forEach(p -> p.getVarNames().forEach(v -> predicateMap.put(v, p)));
        Multimap<Role, Var> roleMap = getRoleVarMap();

        roleMap.entries().stream()
                .filter(e -> predicateMap.containsKey(e.getValue()))
                .forEach(e ->  rolePredicateMap.putAll(e.getKey(), predicateMap.get(e.getValue())));
        return rolePredicateMap;
    }

    /**
     * @return map of pairs role type - Id predicate describing the role player playing this role (substitution)
     */
    @Memoized
    public Multimap<Role, String> getRoleConceptIdMap() {
        ImmutableMultimap.Builder<Role, String> builder = ImmutableMultimap.builder();
        getRolePredicateMap(IdPredicate.class)
                .entries()
                .forEach(e -> builder.put(e.getKey(), e.getValue().getPredicateValue()));
        return builder.build();
    }

    @Memoized
    public Multimap<Role, ValuePredicate> getRoleValueMap() {
        ImmutableMultimap.Builder<Role, ValuePredicate> builder = ImmutableMultimap.builder();
        getRolePredicateMap(ValuePredicate.class)
                .entries()
                .forEach(e -> builder.put(e.getKey(), e.getValue()));
        return builder.build();
    }

    @Memoized
    public Multimap<Role, NeqPredicate> getRoleNeqPredicateMap() {
        ImmutableMultimap.Builder<Role, NeqPredicate> builder = ImmutableMultimap.builder();
        getRolePredicateMap(NeqPredicate.class)
                .entries()
                .forEach(e -> builder.put(e.getKey(), e.getValue()));
        return builder.build();
    }

    @Memoized
    public Multimap<Role, Type> getRoleTypeMap() {
        return getRoleTypeMap(false);
    }

    public Multimap<Role, Type> getRoleTypeMap(boolean inferTypes) {
        ImmutableMultimap.Builder<Role, Type> builder = ImmutableMultimap.builder();
        Multimap<Role, Var> roleMap = getRoleVarMap();
        Map<Var, Type> varTypeMap = getParentQuery().getVarTypeMap();

        roleMap.entries().stream()
                .filter(e -> varTypeMap.containsKey(e.getValue()))
                .filter(e -> {
                    return inferTypes
                            || getParentQuery().getAtoms(TypeAtom.class)
                            .filter(t -> t.getVarName().equals(e.getValue()))
                            .filter(t -> Objects.nonNull(t.getSchemaConcept()))
                            .anyMatch(t -> t.getSchemaConcept().equals(varTypeMap.get(e.getValue())));
                })
                .sorted(Comparator.comparing(e -> varTypeMap.get(e.getValue()).label()))
                .forEach(e -> builder.put(e.getKey(), varTypeMap.get(e.getValue())));
        return builder.build();
    }

    private Stream<Role> getExplicitRoles() {
        ReasonerQueryImpl parent = (ReasonerQueryImpl) getParentQuery();
        Transaction graph = parent.tx();

        return getRelationPlayers().stream()
                .map(RelationPlayer::getRole)
                .flatMap(CommonUtil::optionalToStream)
                .map(VarPatternAdmin::getTypeLabel)
                .flatMap(CommonUtil::optionalToStream)
                .map(graph::<Role>getSchemaConcept);
    }

    @Override
    public boolean isRuleApplicableViaAtom(Atom ruleAtom) {
        if (!(ruleAtom instanceof RelationshipAtom)) return isRuleApplicableViaAtom(ruleAtom.toRelationshipAtom());
        RelationshipAtom atomWithType = this.addType(ruleAtom.getSchemaConcept()).inferRoles(new ConceptMapImpl());
        return ruleAtom.isUnifiableWith(atomWithType);
    }

    @Override
    public RelationshipAtom addType(SchemaConcept type) {
        if (getTypeId() != null) return this;
        //NB: do not cache possible types
        return create(this.getPattern(), this.getPredicateVariable(), type.id(), this.getParentQuery());
    }

    /**
     * infer {@link RelationshipType}s that this {@link RelationshipAtom} can potentially have
     * NB: {@link EntityType}s and link {@link Role}s are treated separately as they behave differently:
     * {@link EntityType}s only play the explicitly defined {@link Role}s (not the relevant part of the hierarchy of the specified {@link Role}) and the {@link Role} inherited from parent
     * @return list of {@link RelationshipType}s this atom can have ordered by the number of compatible {@link Role}s
     */
    private Set<Type> inferPossibleEntityTypePlayers(ConceptMap sub){
        return inferPossibleRelationConfigurations(sub).asMap().entrySet().stream()
                .flatMap(e -> {
                    Set<Role> rs = e.getKey().roles().collect(toSet());
                    rs.removeAll(e.getValue());
                    return rs.stream().flatMap(Role::players);
                }).collect(Collectors.toSet());
    }

    /**
     * @return a map of relationships and corresponding roles that could be played by this atom
     */
    private Multimap<RelationshipType, Role> inferPossibleRelationConfigurations(ConceptMap sub){
        Set<Role> roles = getExplicitRoles().filter(r -> !Schema.MetaSchema.isMetaLabel(r.label())).collect(toSet());
        Map<Var, Type> varTypeMap = getParentQuery().getVarTypeMap(sub);
        Set<Type> types = getRolePlayers().stream().filter(varTypeMap::containsKey).map(varTypeMap::get).collect(toSet());

        if (roles.isEmpty() && types.isEmpty()){
            RelationshipType metaRelationType = tx().getMetaRelationType();
            Multimap<RelationshipType, Role> compatibleTypes = HashMultimap.create();
            metaRelationType.subs()
                    .filter(rt -> !rt.equals(metaRelationType))
                    .forEach(rt -> compatibleTypes.putAll(rt, rt.roles().collect(toSet())));
            return compatibleTypes;
        }

        //intersect relation types from roles and types
        Multimap<RelationshipType, Role> compatibleTypes;

        Multimap<RelationshipType, Role> compatibleTypesFromRoles = compatibleRelationTypesWithRoles(roles, new RoleConverter());
        Multimap<RelationshipType, Role> compatibleTypesFromTypes = compatibleRelationTypesWithRoles(types, new TypeConverter());

        if (roles.isEmpty()){
            compatibleTypes = compatibleTypesFromTypes;
        }
        //no types from roles -> roles correspond to mutually exclusive relations
        else if(compatibleTypesFromRoles.isEmpty() || types.isEmpty()){
            compatibleTypes = compatibleTypesFromRoles;
        } else {
            compatibleTypes = multimapIntersection(compatibleTypesFromTypes, compatibleTypesFromRoles);
        }
        return compatibleTypes;
    }

    @Override
    public ImmutableList<SchemaConcept> getPossibleTypes(){ return inferPossibleTypes(new ConceptMapImpl());}

    /**
     * infer {@link RelationshipType}s that this {@link RelationshipAtom} can potentially have
     * NB: {@link EntityType}s and link {@link Role}s are treated separately as they behave differently:
     * NB: Not using Memoized as memoized methods can't have parameters
     * {@link EntityType}s only play the explicitly defined {@link Role}s (not the relevant part of the hierarchy of the specified {@link Role}) and the {@link Role} inherited from parent
     * @return list of {@link RelationshipType}s this atom can have ordered by the number of compatible {@link Role}s
     */
    private ImmutableList<SchemaConcept> inferPossibleTypes(ConceptMap sub) {
        if (possibleTypes == null) {
            if (getSchemaConcept() != null) return ImmutableList.of(getSchemaConcept());

            Multimap<RelationshipType, Role> compatibleConfigurations = inferPossibleRelationConfigurations(sub);
            Set<Var> untypedRoleplayers = Sets.difference(getRolePlayers(), getParentQuery().getVarTypeMap().keySet());
            Set<RelationshipAtom> untypedNeighbours = getNeighbours(RelationshipAtom.class)
                    .filter(at -> !Sets.intersection(at.getVarNames(), untypedRoleplayers).isEmpty())
                    .collect(toSet());

            ImmutableList.Builder<SchemaConcept> builder = ImmutableList.builder();
            //prioritise relations with higher chance of yielding answers
            compatibleConfigurations.asMap().entrySet().stream()
                    //prioritise relations with more allowed roles
                    .sorted(Comparator.comparing(e -> -e.getValue().size()))
                    //prioritise relations with number of roles equal to arity
                    .sorted(Comparator.comparing(e -> e.getKey().roles().count() != getRelationPlayers().size()))
                    //prioritise relations having more instances
                    .sorted(Comparator.comparing(e -> -tx().getShardCount(e.getKey())))
                    //prioritise relations with highest number of possible types played by untyped role players
                    .map(e -> {
                        if (untypedNeighbours.isEmpty()) return new Pair<>(e.getKey(), 0L);

                        Iterator<RelationshipAtom> neighbourIterator = untypedNeighbours.iterator();
                        Set<Type> typesFromNeighbour = neighbourIterator.next().inferPossibleEntityTypePlayers(sub);
                        while (neighbourIterator.hasNext()) {
                            typesFromNeighbour = Sets.intersection(typesFromNeighbour, neighbourIterator.next().inferPossibleEntityTypePlayers(sub));
                        }

                        Set<Role> rs = e.getKey().roles().collect(toSet());
                        rs.removeAll(e.getValue());
                        return new Pair<>(
                                e.getKey(),
                                rs.stream().flatMap(Role::players).filter(typesFromNeighbour::contains).count()
                        );
                    })
                    .sorted(Comparator.comparing(p -> -p.getValue()))
                    //prioritise non-implicit relations
                    .sorted(Comparator.comparing(e -> e.getKey().isImplicit()))
                    .map(Pair::getKey)
                    //retain super types only
                    .filter(t -> Sets.intersection(supers(t), compatibleConfigurations.keySet()).isEmpty())
                    .forEach(builder::add);

            //TODO need to add THING and meta relation type as well to make it complete
            this.possibleTypes = builder.build();
        }
        return possibleTypes;
    }

    /**
     * attempt to infer the relation type of this relationship
     * @param sub extra instance information to aid entity type inference
     * @return either this if relation type can't be inferred or a fresh relationship with inferred relationship type
     */
    private RelationshipAtom inferRelationshipType(ConceptMap sub){
        if (getTypePredicate() != null) return this;
        if (sub.containsVar(getPredicateVariable())) return addType(sub.get(getPredicateVariable()).asType());
        List<SchemaConcept> relationshipTypes = inferPossibleTypes(sub);
        if (relationshipTypes.size() == 1) return addType(Iterables.getOnlyElement(relationshipTypes));
        return this;
    }

    @Override
    public RelationshipAtom inferTypes(ConceptMap sub) {
        return this
                .inferRelationshipType(sub)
                .inferRoles(sub);
    }

    @Override
    public List<Atom> atomOptions(ConceptMap sub) {
        return this.inferPossibleTypes(sub).stream()
                .map(this::addType)
                .map(at -> at.inferRoles(sub))
                //order by number of distinct roles
                .sorted(Comparator.comparing(at -> -at.getRoleLabels().size()))
                .sorted(Comparator.comparing(Atom::isRuleResolvable))
                .collect(Collectors.toList());
    }

    @Override
    public Set<Var> getRoleExpansionVariables(){
        return getRelationPlayers().stream()
                .map(RelationPlayer::getRole)
                .flatMap(CommonUtil::optionalToStream)
                .filter(p -> p.var().isUserDefinedName())
                .filter(p -> !p.getTypeLabel().isPresent())
                .map(VarPatternAdmin::var)
                .collect(Collectors.toSet());
    }

    @Override
    public Stream<Predicate> getInnerPredicates(){
        return Stream.concat(
                super.getInnerPredicates(),
                getRelationPlayers().stream()
                        .map(RelationPlayer::getRole)
                        .flatMap(CommonUtil::optionalToStream)
                        .filter(vp -> vp.var().isUserDefinedName())
                        .map(vp -> new Pair<>(vp.var(), vp.getTypeLabel().orElse(null)))
                        .filter(p -> Objects.nonNull(p.getValue()))
                        .map(p -> IdPredicate.create(p.getKey(), p.getValue(), getParentQuery()))
        );
    }

    /**
     * attempt to infer role types of this relation and return a fresh relationship with inferred role types
     * @return either this if nothing/no roles can be inferred or fresh relation with inferred role types
     */
    private RelationshipAtom inferRoles(ConceptMap sub){
        //return if all roles known and non-meta
        List<Role> explicitRoles = getExplicitRoles().collect(Collectors.toList());
        Map<Var, Type> varTypeMap = getParentQuery().getVarTypeMap(sub);
        boolean allRolesMeta = explicitRoles.stream().allMatch(role -> Schema.MetaSchema.isMetaLabel(role.label()));
        boolean roleRecomputationViable = allRolesMeta && (!sub.isEmpty() || !Sets.intersection(varTypeMap.keySet(), getRolePlayers()).isEmpty());
        if (explicitRoles.size() == getRelationPlayers().size() && !roleRecomputationViable) return this;

        Transaction graph = getParentQuery().tx();
        Role metaRole = graph.getMetaRole();
        List<RelationPlayer> allocatedRelationPlayers = new ArrayList<>();
        RelationshipType relType = getSchemaConcept() != null? getSchemaConcept().asRelationshipType() : null;

        //explicit role types from castings
        List<RelationPlayer> inferredRelationPlayers = new ArrayList<>();
        getRelationPlayers().forEach(rp -> {
            Var varName = rp.getRolePlayer().var();
            VarPatternAdmin rolePattern = rp.getRole().orElse(null);
            if (rolePattern != null) {
                Label roleLabel = rolePattern.getTypeLabel().orElse(null);
                //allocate if variable role or if label non meta
                if (roleLabel == null || !Schema.MetaSchema.isMetaLabel(roleLabel)) {
                    inferredRelationPlayers.add(RelationPlayer.of(rolePattern, varName.admin()));
                    allocatedRelationPlayers.add(rp);
                }
            }
        });

        //remaining roles
        //role types can repeat so no matter what has been allocated still the full spectrum of possibilities is present
        //TODO make restrictions based on cardinality constraints
        Set<Role> possibleRoles = relType != null?
                relType.roles().collect(toSet()) :
                inferPossibleTypes(sub).stream()
                        .filter(Concept::isRelationshipType)
                        .map(Concept::asRelationshipType)
                        .flatMap(RelationshipType::roles).collect(toSet());

        //possible role types for each casting based on its type
        Map<RelationPlayer, Set<Role>> mappings = new HashMap<>();
        getRelationPlayers().stream()
                .filter(rp -> !allocatedRelationPlayers.contains(rp))
                .forEach(rp -> {
                    Var varName = rp.getRolePlayer().var();
                    Type type = varTypeMap.get(varName);
                    mappings.put(rp, top(type != null? compatibleRoles(type, possibleRoles) : possibleRoles));
                });


        //allocate all unambiguous mappings
        mappings.entrySet().stream()
                .filter(entry -> entry.getValue().size() == 1)
                .forEach(entry -> {
                    RelationPlayer rp = entry.getKey();
                    Var varName = rp.getRolePlayer().var();
                    Role role = Iterables.getOnlyElement(entry.getValue());
                    VarPatternAdmin rolePattern = Graql.var().label(role.label()).admin();
                    inferredRelationPlayers.add(RelationPlayer.of(rolePattern, varName.admin()));
                    allocatedRelationPlayers.add(rp);
                });

        //fill in unallocated roles with metarole
        getRelationPlayers().stream()
                .filter(rp -> !allocatedRelationPlayers.contains(rp))
                .forEach(rp -> {
                    Var varName = rp.getRolePlayer().var();
                    VarPatternAdmin rolePattern = rp.getRole().orElse(null);

                    rolePattern = rolePattern != null ?
                            rolePattern.var().label(metaRole.label()).admin() :
                            Graql.var().label(metaRole.label()).admin();
                    inferredRelationPlayers.add(RelationPlayer.of(rolePattern, varName.admin()));
                });

        VarPattern relationPattern = relationPattern(getVarName(), inferredRelationPlayers);
        VarPatternAdmin newPattern =
                (isDirect()?
                        relationPattern.isaExplicit(getPredicateVariable()) :
                        relationPattern.isa(getPredicateVariable())
                ).admin();
        return create(newPattern, this.getPredicateVariable(), this.getTypeId(), this.getPossibleTypes(), this.getParentQuery());
    }

    /**
     * @return map containing roleType - (rolePlayer var - rolePlayer type) pairs
     */
    @Memoized
    public Multimap<Role, Var> getRoleVarMap() {
        ImmutableMultimap.Builder<Role, Var> builder = ImmutableMultimap.builder();

        Transaction graph = getParentQuery().tx();
        getRelationPlayers().forEach(c -> {
            Var varName = c.getRolePlayer().var();
            VarPatternAdmin rolePattern = c.getRole().orElse(null);
            if (rolePattern != null) {
                //try directly
                Label typeLabel = rolePattern.getTypeLabel().orElse(null);
                Role role = typeLabel != null ? graph.getRole(typeLabel.getValue()) : null;
                //try indirectly
                if (role == null && rolePattern.var().isUserDefinedName()) {
                    IdPredicate rolePredicate = getIdPredicate(rolePattern.var());
                    if (rolePredicate != null){
                        Role r = graph.getConcept(rolePredicate.getPredicate());
                        if (r == null) throw GraqlQueryException.idNotFound(rolePredicate.getPredicate());
                        role = r;
                    }
                }
                if (role != null) builder.put(role, varName);
            }
        });
        return builder.build();
    }

    private Multimap<Role, RelationPlayer> getRoleRelationPlayerMap(){
        Multimap<Role, RelationPlayer> roleRelationPlayerMap = ArrayListMultimap.create();
        Multimap<Role, Var> roleVarMap = getRoleVarMap();
        List<RelationPlayer> relationPlayers = getRelationPlayers();
        roleVarMap.asMap().forEach((role, value) -> {
            Label roleLabel = role.label();
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

    /**
     * @param parentAtom reference atom defining the mapping
     * @param matchType type of match to be performed
     * @return set of possible COMPLETE mappings between this (child) and parent relation players
     */
    private Set<List<Pair<RelationPlayer, RelationPlayer>>> getRelationPlayerMappings(RelationshipAtom parentAtom, UnifierComparison matchType) {
        Multimap<Role, RelationPlayer> childRoleRPMap = this.getRoleRelationPlayerMap();
        Map<Var, Type> childVarTypeMap = this.getParentQuery().getVarTypeMap(!matchType.equals(UnifierType.STRUCTURAL));
        Map<Var, Type> parentVarTypeMap = parentAtom.getParentQuery().getVarTypeMap(!matchType.equals(UnifierType.STRUCTURAL));

        //establish compatible castings for each parent casting
        List<Set<Pair<RelationPlayer, RelationPlayer>>> compatibleMappingsPerParentRP = new ArrayList<>();
        ReasonerQueryImpl childQuery = (ReasonerQueryImpl) getParentQuery();
        Set<Role> childRoles = childRoleRPMap.keySet();
        parentAtom.getRelationPlayers().stream()
                .filter(prp -> prp.getRole().isPresent())
                .forEach(prp -> {
                    VarPatternAdmin parentRolePattern = prp.getRole().orElse(null);
                    if (parentRolePattern == null){
                        throw GraqlQueryException.rolePatternAbsent(this);
                    }
                    Label parentRoleLabel = parentRolePattern.getTypeLabel().orElse(null);

                    if (parentRoleLabel != null) {
                        Var parentRolePlayer = prp.getRolePlayer().var();
                        Type parentType = parentVarTypeMap.get(parentRolePlayer);

                        Set<Role> compatibleRoles = compatibleRoles(
                                tx().getSchemaConcept(parentRoleLabel),
                                parentType,
                                childRoles);

                        List<RelationPlayer> compatibleRelationPlayers = new ArrayList<>();
                        compatibleRoles.stream()
                                .filter(childRoleRPMap::containsKey)
                                .forEach(role ->
                                        childRoleRPMap.get(role).stream()
                                                //check for inter-type compatibility
                                                .filter(crp -> {
                                                    Var childVar = crp.getRolePlayer().var();
                                                    Type childType = childVarTypeMap.get(childVar);
                                                    return matchType.typePlayability(childQuery, childVar, parentType)
                                                            && matchType.typeCompatibility(parentType, childType);
                                                })
                                                //check for substitution compatibility
                                                .filter(crp -> {
                                                    IdPredicate parentId = parentAtom.getIdPredicate(prp.getRolePlayer().var());
                                                    IdPredicate childId = this.getIdPredicate(crp.getRolePlayer().var());
                                                    return matchType.idCompatibility(parentId, childId);
                                                })
                                                //check for value predicate compatibility
                                                .filter(crp -> {
                                                    Set<Atomic> parentVP = parentAtom.getPredicates(prp.getRolePlayer().var(), ValuePredicate.class).collect(toSet());
                                                    Set<Atomic> childVP = this.getPredicates(crp.getRolePlayer().var(), ValuePredicate.class).collect(toSet());
                                                    return matchType.attributeValueCompatibility(parentVP, childVP);
                                                })
                                                //check linked resources
                                                .filter(crp -> {
                                                    Var parentVar = prp.getRolePlayer().var();
                                                    Var childVar = crp.getRolePlayer().var();
                                                    return matchType.attributeCompatibility(parentAtom.getParentQuery(), this.getParentQuery(), parentVar, childVar);
                                                })
                                                .forEach(compatibleRelationPlayers::add)
                                );
                        if (!compatibleRelationPlayers.isEmpty()) {
                            compatibleMappingsPerParentRP.add(
                                    compatibleRelationPlayers.stream()
                                            .map(crp -> new Pair<>(crp, prp))
                                            .collect(Collectors.toSet())
                            );
                        }
                    } else {
                        compatibleMappingsPerParentRP.add(
                                getRelationPlayers().stream()
                                        .map(crp -> new Pair<>(crp, prp))
                                        .collect(Collectors.toSet())
                        );
                    }
                });

        return Sets.cartesianProduct(compatibleMappingsPerParentRP).stream()
                .filter(list -> !list.isEmpty())
                //check the same child rp is not mapped to multiple parent rps
                .filter(list -> {
                    List<RelationPlayer> listChildRps = list.stream().map(Pair::getKey).collect(Collectors.toList());
                    //NB: this preserves cardinality instead of removing all occuring instances which is what we want
                    return ReasonerUtils.subtract(listChildRps, this.getRelationPlayers()).isEmpty();
                })
                //check all parent rps mapped
                .filter(list -> {
                    List<RelationPlayer> listParentRps = list.stream().map(Pair::getValue).collect(Collectors.toList());
                    return listParentRps.containsAll(parentAtom.getRelationPlayers());
                })
                .collect(toSet());
    }

    @Override
    public Unifier getUnifier(Atom pAtom, UnifierComparison unifierType){
        return getMultiUnifier(pAtom, unifierType).getUnifier();
    }

    @Override
    public MultiUnifier getMultiUnifier(Atom pAtom, UnifierComparison unifierType) {
        Unifier baseUnifier = super.getUnifier(pAtom, unifierType);
        if (baseUnifier == null){ return MultiUnifierImpl.nonExistent();}

        Set<Unifier> unifiers = new HashSet<>();
        if (pAtom.isRelation()) {
            RelationshipAtom parentAtom = pAtom.toRelationshipAtom();

            //NB: if two atoms are equal and their sub and type mappings are equal we return the identity unifier
            //this is important for cases like unifying ($r1: $x, $r2: $y) with itself
            if (this.equals(parentAtom)
                    && this.getPartialSubstitutions().collect(toSet()).equals(parentAtom.getPartialSubstitutions().collect(toSet()))
                    && this.getTypeConstraints().collect(toSet()).equals(parentAtom.getTypeConstraints().collect(toSet()))){
                return MultiUnifierImpl.trivial();
            }

            boolean unifyRoleVariables = parentAtom.getRelationPlayers().stream()
                    .map(RelationPlayer::getRole)
                    .flatMap(CommonUtil::optionalToStream)
                    .anyMatch(rp -> rp.var().isUserDefinedName());
            getRelationPlayerMappings(parentAtom, unifierType)
                    .forEach(mappingList -> {
                        Multimap<Var, Var> varMappings = HashMultimap.create();
                        mappingList.forEach(rpm -> {
                            //add role player mapping
                            varMappings.put(rpm.getKey().getRolePlayer().var(), rpm.getValue().getRolePlayer().var());

                            //add role var mapping if needed
                            VarPattern childRolePattern = rpm.getKey().getRole().orElse(null);
                            VarPattern parentRolePattern = rpm.getValue().getRole().orElse(null);
                            if (parentRolePattern != null && childRolePattern != null && unifyRoleVariables){
                                varMappings.put(childRolePattern.admin().var(), parentRolePattern.admin().var());
                            }

                        });
                        unifiers.add(baseUnifier.merge(new UnifierImpl(varMappings)));
                    });
        } else {
            unifiers.add(baseUnifier);
        }
        return new MultiUnifierImpl(unifiers);
    }

    @Override
    public Stream<ConceptMap> materialise(){
        RelationshipType relationType = getSchemaConcept().asRelationshipType();
        Multimap<Role, Var> roleVarMap = getRoleVarMap();
        ConceptMap substitution = getParentQuery().getSubstitution();

        //if the relation already exists, only assign roleplayers, otherwise create a new relation
        Relationship relationship = substitution.containsVar(getVarName())?
                substitution.get(getVarName()).asRelationship() :
                RelationshipTypeImpl.from(relationType).addRelationshipInferred();

        roleVarMap.asMap()
                .forEach((key, value) -> value.forEach(var -> relationship.assign(key, substitution.get(var).asThing())));

        ConceptMap relationSub = getRoleSubstitution().merge(
                getVarName().isUserDefinedName()?
                        new ConceptMapImpl(ImmutableMap.of(getVarName(), relationship)) :
                        new ConceptMapImpl()
        );

        return Stream.of(substitution.merge(relationSub));
    }

    /**
     * if any {@link Role} variable of the parent is user defined rewrite ALL {@link Role} variables to user defined (otherwise unification is problematic)
     * @param parentAtom parent atom that triggers rewrite
     * @return new relation atom with user defined {@link Role} variables if necessary or this
     */
    private RelationshipAtom rewriteWithVariableRoles(Atom parentAtom){
        if (!parentAtom.requiresRoleExpansion()) return this;

        VarPattern relVar = getPattern().admin().getProperty(IsaProperty.class)
                .map(prop -> getVarName().isa(prop.type())).orElse(getVarName());

        for (RelationPlayer rp: getRelationPlayers()) {
            VarPatternAdmin rolePattern = rp.getRole().orElse(null);
            if (rolePattern != null) {
                Var roleVar = rolePattern.var();
                Label roleLabel = rolePattern.getTypeLabel().orElse(null);
                relVar = relVar.rel(roleVar.asUserDefined().label(roleLabel), rp.getRolePlayer());
            } else {
                relVar = relVar.rel(rp.getRolePlayer());
            }
        }
        return create(relVar.admin(), this.getPredicateVariable(), this.getTypeId(), this.getPossibleTypes(), this.getParentQuery());
    }

    /**
     * @param parentAtom parent atom that triggers rewrite
     * @return new relation atom with user defined name if necessary or this
     */
    private RelationshipAtom rewriteWithRelationVariable(Atom parentAtom){
        if (this.getVarName().isUserDefinedName() || !parentAtom.getVarName().isUserDefinedName()) return this;
        return rewriteWithRelationVariable();
    }

    @Override
    public RelationshipAtom rewriteWithRelationVariable(){
        VarPattern newVar = Graql.var().asUserDefined();
        VarPattern relVar = getPattern().admin().getProperty(IsaProperty.class)
                .map(prop -> newVar.isa(prop.type()))
                .orElse(newVar);

        for (RelationPlayer c: getRelationPlayers()) {
            VarPatternAdmin roleType = c.getRole().orElse(null);
            if (roleType != null) {
                relVar = relVar.rel(roleType, c.getRolePlayer());
            } else {
                relVar = relVar.rel(c.getRolePlayer());
            }
        }
        return create(relVar.admin(), this.getPredicateVariable(), this.getTypeId(), this.getPossibleTypes(), this.getParentQuery());
    }

    @Override
    public RelationshipAtom rewriteWithTypeVariable(){
        return create(this.getPattern(), this.getPredicateVariable().asUserDefined(), this.getTypeId(), this.getPossibleTypes(), this.getParentQuery());
    }

    @Override
    public Atom rewriteToUserDefined(Atom parentAtom){
        return this
                .rewriteWithRelationVariable(parentAtom)
                .rewriteWithVariableRoles(parentAtom)
                .rewriteWithTypeVariable(parentAtom);

    }

}
