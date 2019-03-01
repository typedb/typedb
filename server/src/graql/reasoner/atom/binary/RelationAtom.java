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
package grakn.core.graql.reasoner.atom.binary;

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
import grakn.core.common.exception.ErrorMessage;
import grakn.core.common.util.CommonUtil;
import grakn.core.concept.Concept;
import grakn.core.concept.ConceptId;
import grakn.core.concept.Label;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concept.thing.Relation;
import grakn.core.concept.type.EntityType;
import grakn.core.concept.type.RelationType;
import grakn.core.concept.type.Role;
import grakn.core.concept.type.Rule;
import grakn.core.concept.type.SchemaConcept;
import grakn.core.concept.type.Type;
import grakn.core.graql.exception.GraqlQueryException;
import grakn.core.graql.reasoner.atom.Atom;
import grakn.core.graql.reasoner.atom.Atomic;
import grakn.core.graql.reasoner.atom.AtomicEquivalence;
import grakn.core.graql.reasoner.atom.predicate.IdPredicate;
import grakn.core.graql.reasoner.atom.predicate.NeqPredicate;
import grakn.core.graql.reasoner.atom.predicate.Predicate;
import grakn.core.graql.reasoner.atom.predicate.ValuePredicate;
import grakn.core.graql.reasoner.cache.SemanticDifference;
import grakn.core.graql.reasoner.cache.VariableDefinition;
import grakn.core.graql.reasoner.query.ReasonerQuery;
import grakn.core.graql.reasoner.query.ReasonerQueryImpl;
import grakn.core.graql.reasoner.unifier.MultiUnifier;
import grakn.core.graql.reasoner.unifier.MultiUnifierImpl;
import grakn.core.graql.reasoner.unifier.Unifier;
import grakn.core.graql.reasoner.unifier.UnifierComparison;
import grakn.core.graql.reasoner.unifier.UnifierImpl;
import grakn.core.graql.reasoner.unifier.UnifierType;
import grakn.core.graql.reasoner.utils.Pair;
import grakn.core.graql.reasoner.utils.ReasonerUtils;
import grakn.core.graql.reasoner.utils.conversion.RoleConverter;
import grakn.core.graql.reasoner.utils.conversion.TypeConverter;
import grakn.core.server.session.TransactionOLTP;
import grakn.core.server.kb.Schema;
import grakn.core.server.kb.concept.ConceptUtils;
import grakn.core.server.kb.concept.RelationTypeImpl;
import graql.lang.pattern.Pattern;
import graql.lang.property.IsaProperty;
import graql.lang.property.RelationProperty;
import graql.lang.property.VarProperty;
import graql.lang.statement.Statement;
import graql.lang.statement.StatementInstance;
import graql.lang.statement.StatementThing;
import graql.lang.statement.Variable;

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
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static grakn.core.server.kb.concept.ConceptUtils.bottom;
import static grakn.core.server.kb.concept.ConceptUtils.top;
import static graql.lang.Graql.var;
import static java.util.stream.Collectors.toSet;

/**
 * Atom implementation defining a relation atom corresponding to a combined {@link RelationProperty}
 * and (optional) {@link IsaProperty}. The relation atom is a {@link TypeAtom} with relation players.
 */
@AutoValue
public abstract class RelationAtom extends IsaAtomBase {

    abstract ImmutableList<RelationProperty.RolePlayer> getRelationPlayers();
    abstract ImmutableSet<Label> getRoleLabels();

    private ImmutableList<Type> possibleTypes = null;

    public static RelationAtom create(Statement pattern, Variable predicateVar, @Nullable ConceptId predicateId, ReasonerQuery parent) {
        List<RelationProperty.RolePlayer> rps = new ArrayList<>();
        pattern.getProperty(RelationProperty.class)
                .ifPresent(prop -> prop.relationPlayers().stream().sorted(Comparator.comparing(Object::hashCode)).forEach(rps::add));
        ImmutableList<RelationProperty.RolePlayer> relationPlayers = ImmutableList.copyOf(rps);
        ImmutableSet<Label> roleLabels = ImmutableSet.<Label>builder().addAll(
                relationPlayers.stream()
                        .map(RelationProperty.RolePlayer::getRole)
                        .flatMap(CommonUtil::optionalToStream)
                        .map(Statement::getType)
                        .flatMap(CommonUtil::optionalToStream)
                        .map(Label::of).iterator()
        ).build();
        return new AutoValue_RelationAtom(pattern.var(), pattern, parent, predicateVar, predicateId, relationPlayers, roleLabels);
    }

    private static RelationAtom create(Statement pattern, Variable predicateVar, @Nullable ConceptId predicateId, @Nullable ImmutableList<Type> possibleTypes, ReasonerQuery parent) {
        RelationAtom atom = create(pattern, predicateVar, predicateId, parent);
        atom.possibleTypes = possibleTypes;
        return atom;
    }

    private static RelationAtom create(RelationAtom a, ReasonerQuery parent) {
        RelationAtom atom = new AutoValue_RelationAtom(a.getVarName(), a.getPattern(), parent, a.getPredicateVariable(), a.getTypeId(), a.getRelationPlayers(), a.getRoleLabels());
        atom.possibleTypes = a.possibleTypes;
        return atom;
    }

    //NB: overriding as these require a derived property
    @Override
    public final boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || this.getClass() != obj.getClass()) return false;
        RelationAtom that = (RelationAtom) obj;
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
    public Class<? extends VarProperty> getVarPropertyClass(){ return RelationProperty.class;}

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
    public RelationAtom toRelationAtom(){ return this;}

    @Override
    public IsaAtom toIsaAtom(){ return IsaAtom.create(getVarName(), getPredicateVariable(), getTypeId(), false, getParentQuery()); }

    @Override
    public Set<Atom> rewriteToAtoms(){
        return this.getRelationPlayers().stream()
                .map(rp -> create(relationPattern(getVarName().asUserDefined(), Sets.newHashSet(rp)), getPredicateVariable(), getTypeId(), null, this.getParentQuery()))
                .collect(toSet());
    }

    @Override
    public String toString(){
        String typeString = getSchemaConcept() != null?
                getSchemaConcept().label().getValue() :
                "{" + inferPossibleTypes(new ConceptMap()).stream().map(rt -> rt.label().getValue()).collect(Collectors.joining(", ")) + "}";
        String relationString = (isUserDefined()? getVarName() + " ": "") +
                typeString +
                (getPredicateVariable().isUserDefinedName()? "(" + getPredicateVariable() + ")" : "") +
                (isDirect()? "!" : "") +
                getRelationPlayers().toString();
        return relationString + getPredicates(Predicate.class).map(Predicate::toString).collect(Collectors.joining(""));
    }

    @Override
    public Set<Variable> getVarNames() {
        Set<Variable> vars = super.getVarNames();
        vars.addAll(getRolePlayers());
        vars.addAll(getRoleVariables());
        return vars;
    }

    /**
     * @return set constituting the role player var names
     */
    private Set<Variable> getRolePlayers() {
        return getRelationPlayers().stream().map(c -> c.getPlayer().var()).collect(toSet());
    }

    /**
     * @return set of user defined role variables if any
     */
    private Set<Variable> getRoleVariables(){
        return getRelationPlayers().stream()
                .map(RelationProperty.RolePlayer::getRole)
                .flatMap(CommonUtil::optionalToStream)
                .map(Statement::var)
                .filter(Variable::isUserDefinedName)
                .collect(Collectors.toSet());
    }

    private ConceptMap getRoleSubstitution(){
        Map<Variable, Concept> roleSub = new HashMap<>();
        getRolePredicates().forEach(p -> roleSub.put(p.getVarName(), tx().getConcept(p.getPredicate())));
        return new ConceptMap(roleSub);
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
                        relationPattern().isaX(getSchemaConcept().label().getValue()):
                        relationPattern().isa(getSchemaConcept().label().getValue());
    }

    private Statement relationPattern() {
        return relationPattern(getVarName(), getRelationPlayers());
    }

    /**
     * construct a $varName (rolemap) isa $typeVariable relation
     * @param varName            variable name
     * @param relationPlayers collection of rolePlayer-roleType mappings
     * @return corresponding {@link Statement}
     */
    private Statement relationPattern(Variable varName, Collection<RelationProperty.RolePlayer> relationPlayers) {
        Statement var = new Statement(varName);
        for (RelationProperty.RolePlayer rp : relationPlayers) {
            Statement rolePattern = rp.getRole().orElse(null);
            var = rolePattern != null? var.rel(rolePattern, rp.getPlayer()) : var.rel(rp.getPlayer());
        }
        return var;
    }

    private boolean isBaseEquivalent(Object obj){
        if (obj == null || this.getClass() != obj.getClass()) return false;
        if (obj == this) return true;
        RelationAtom that = (RelationAtom) obj;
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
        RelationAtom that = (RelationAtom) obj;
        //check role-type and id predicate bindings
        return this.getRoleTypeMap().equals(that.getRoleTypeMap())
                && this.predicateBindingsAlphaEquivalent(that);
    }

    @Override
    public boolean isStructurallyEquivalent(Object obj) {
        if (!isBaseEquivalent(obj) || !super.isStructurallyEquivalent(obj)) return false;
        RelationAtom that = (RelationAtom) obj;
        // check bindings
        return this.getRoleTypeMap(false).equals(that.getRoleTypeMap(false))
                && this.predicateBindingsStructurallyEquivalent(that);
    }

    private boolean predicateBindingsEquivalent(RelationAtom atom,
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

    private boolean predicateBindingsAlphaEquivalent(RelationAtom atom) {
        return predicateBindingsEquivalent(atom, String::equals, AtomicEquivalence.AlphaEquivalence);
    }

    private boolean predicateBindingsStructurallyEquivalent(RelationAtom atom) {
        return predicateBindingsEquivalent(atom, (a, b) -> true, AtomicEquivalence.StructuralEquivalence);
    }

    @Memoized
    @Override
    public int alphaEquivalenceHashCode() {
        int equivalenceHashCode = baseHashCode();
        SortedSet<Integer> hashes = new TreeSet<>();
        this.getRoleTypeMap().entries().stream()
                .sorted(Comparator.comparing(e -> e.getKey().label()))
                .sorted(Comparator.comparing(e -> e.getValue().label()))
                .forEach(e -> hashes.add(e.hashCode()));
        this.getRoleConceptIdMap().entries().stream()
                .sorted(Comparator.comparing(e -> e.getKey().label()))
                .sorted(Comparator.comparing(Map.Entry::getValue))
                .forEach(e -> hashes.add(e.hashCode()));
        for (Integer hash : hashes) equivalenceHashCode = equivalenceHashCode * 37 + hash;
        return equivalenceHashCode;
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
            Statement role = rp.getRole().orElse(null);
            if (role == null){
                errors.add(ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_RELATION_WITH_AMBIGUOUS_ROLE.getMessage(rule.then(), rule.label()));
            } else {
                String roleLabel = role.getType().orElse(null);
                if (roleLabel == null){
                    errors.add(ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_RELATION_WITH_AMBIGUOUS_ROLE.getMessage(rule.then(), rule.label()));
                } else {
                    if (Schema.MetaSchema.isMetaLabel(Label.of(roleLabel))) {
                        errors.add(ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_RELATION_WITH_AMBIGUOUS_ROLE.getMessage(rule.then(), rule.label()));
                    }
                    Role roleType = tx().getRole(roleLabel);
                    if (roleType != null && roleType.isImplicit()) {
                        errors.add(ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_RELATION_WITH_IMPLICIT_ROLE.getMessage(rule.then(), rule.label()));
                    }
                }
            }
        });
        return errors;
    }

    @Override
    public Set<String> validateAsRuleBody(Label ruleLabel) {
        Set<String> errors = new HashSet<>();
        SchemaConcept type = getSchemaConcept();
        if (type != null && !type.isRelationType()){
            errors.add(ErrorMessage.VALIDATION_RULE_INVALID_RELATION_TYPE.getMessage(ruleLabel, type.label()));
            return errors;
        }

        //check role-type compatibility
        Map<Variable, Type> varTypeMap = getParentQuery().getVarTypeMap();
        for (Map.Entry<Role, Collection<Variable>> e : getRoleVarMap().asMap().entrySet() ){
            Role role = e.getKey();
            if (!Schema.MetaSchema.isMetaLabel(role.label())) {
                //check whether this role can be played in this relation
                if (type != null && type.asRelationType().roles().noneMatch(r -> r.equals(role))) {
                    errors.add(ErrorMessage.VALIDATION_RULE_ROLE_CANNOT_BE_PLAYED.getMessage(ruleLabel, role.label(), type.label()));
                }

                //check whether the role player's type allows playing this role
                for (Variable player : e.getValue()) {
                    Type playerType = varTypeMap.get(player);
                    if (playerType != null && playerType.playing().noneMatch(plays -> plays.equals(role))) {
                        errors.add(ErrorMessage.VALIDATION_RULE_TYPE_CANNOT_PLAY_ROLE.getMessage(ruleLabel, playerType.label(), role.label(), type == null? "" : type.label()));
                    }
                }
            }
        }
        return errors;
    }

    @Override
    public Stream<IdPredicate> getPartialSubstitutions() {
        Set<Variable> varNames = getVarNames();
        return getPredicates(IdPredicate.class)
                .filter(pred -> varNames.contains(pred.getVarName()));
    }

    public Stream<IdPredicate> getRolePredicates(){
        return getRelationPlayers().stream()
                .map(RelationProperty.RolePlayer::getRole)
                .flatMap(CommonUtil::optionalToStream)
                .filter(var -> var.var().isUserDefinedName())
                .filter(vp -> vp.getType().isPresent())
                .map(vp -> {
                    String label = vp.getType().orElse(null);
                    return IdPredicate.create(vp.var(), tx().getRole(label).id(), getParentQuery());
                });
    }

    private <T extends Predicate> Multimap<Role, T> getRolePredicateMap(Class<T> type) {
        HashMultimap<Role, T> rolePredicateMap = HashMultimap.create();

        HashMultimap<Variable, T> predicateMap = HashMultimap.create();
        getPredicates(type).forEach(p -> p.getVarNames().forEach(v -> predicateMap.put(v, p)));
        Multimap<Role, Variable> roleMap = getRoleVarMap();

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

    private Multimap<Role, Type> getRoleTypeMap(boolean inferTypes) {
        ImmutableMultimap.Builder<Role, Type> builder = ImmutableMultimap.builder();
        Multimap<Role, Variable> roleMap = getRoleVarMap();
        Map<Variable, Type> varTypeMap = getParentQuery().getVarTypeMap();

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
        TransactionOLTP graph = parent.tx();

        return getRelationPlayers().stream()
                .map(RelationProperty.RolePlayer::getRole)
                .flatMap(CommonUtil::optionalToStream)
                .map(Statement::getType)
                .flatMap(CommonUtil::optionalToStream)
                .map(label -> graph.getSchemaConcept(Label.of(label)));
    }

    @Override
    public boolean isRuleApplicableViaAtom(Atom ruleAtom) {
        if (!(ruleAtom instanceof RelationAtom)) return isRuleApplicableViaAtom(ruleAtom.toRelationAtom());
        RelationAtom atomWithType = this.addType(ruleAtom.getSchemaConcept()).inferRoles(new ConceptMap());
        return ruleAtom.isUnifiableWith(atomWithType);
    }

    @Override
    public RelationAtom addType(SchemaConcept type) {
        if (getTypeId() != null) return this;
        //NB: do not cache possible types
        return create(this.getPattern(), this.getPredicateVariable(), type.id(), this.getParentQuery());
    }

    /**
     * infer {@link RelationType}s that this {@link RelationAtom} can potentially have
     * NB: {@link EntityType}s and link {@link Role}s are treated separately as they behave differently:
     * {@link EntityType}s only play the explicitly defined {@link Role}s (not the relevant part of the hierarchy of the specified {@link Role}) and the {@link Role} inherited from parent
     * @return list of {@link RelationType}s this atom can have ordered by the number of compatible {@link Role}s
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
     * @return a map of relations and corresponding roles that could be played by this atom
     */
    private Multimap<RelationType, Role> inferPossibleRelationConfigurations(ConceptMap sub){
        Set<Role> roles = getExplicitRoles().filter(r -> !Schema.MetaSchema.isMetaLabel(r.label())).collect(toSet());
        Map<Variable, Type> varTypeMap = getParentQuery().getVarTypeMap(sub);
        Set<Type> types = getRolePlayers().stream().filter(varTypeMap::containsKey).map(varTypeMap::get).collect(toSet());

        if (roles.isEmpty() && types.isEmpty()){
            RelationType metaRelationType = tx().getMetaRelationType();
            Multimap<RelationType, Role> compatibleTypes = HashMultimap.create();
            metaRelationType.subs()
                    .filter(rt -> !rt.equals(metaRelationType))
                    .forEach(rt -> compatibleTypes.putAll(rt, rt.roles().collect(toSet())));
            return compatibleTypes;
        }

        //intersect relation types from roles and types
        Multimap<RelationType, Role> compatibleTypes;

        Multimap<RelationType, Role> compatibleTypesFromRoles = ReasonerUtils.compatibleRelationTypesWithRoles(roles, new RoleConverter());
        Multimap<RelationType, Role> compatibleTypesFromTypes = ReasonerUtils.compatibleRelationTypesWithRoles(types, new TypeConverter());

        if (roles.isEmpty()){
            compatibleTypes = compatibleTypesFromTypes;
        }
        //no types from roles -> roles correspond to mutually exclusive relations
        else if(compatibleTypesFromRoles.isEmpty() || types.isEmpty()){
            compatibleTypes = compatibleTypesFromRoles;
        } else {
            compatibleTypes = ReasonerUtils.multimapIntersection(compatibleTypesFromTypes, compatibleTypesFromRoles);
        }
        return compatibleTypes;
    }

    @Override
    public ImmutableList<Type> getPossibleTypes(){ return inferPossibleTypes(new ConceptMap());}

    /**
     * infer {@link RelationType}s that this {@link RelationAtom} can potentially have
     * NB: {@link EntityType}s and link {@link Role}s are treated separately as they behave differently:
     * NB: Not using Memoized as memoized methods can't have parameters
     * {@link EntityType}s only play the explicitly defined {@link Role}s (not the relevant part of the hierarchy of the specified {@link Role}) and the {@link Role} inherited from parent
     * @return list of {@link RelationType}s this atom can have ordered by the number of compatible {@link Role}s
     */
    private ImmutableList<Type> inferPossibleTypes(ConceptMap sub) {
        if (possibleTypes == null) {
            if (getSchemaConcept() != null) return ImmutableList.of(getSchemaConcept().asType());

            Multimap<RelationType, Role> compatibleConfigurations = inferPossibleRelationConfigurations(sub);
            Set<Variable> untypedRoleplayers = Sets.difference(getRolePlayers(), getParentQuery().getVarTypeMap().keySet());
            Set<RelationAtom> untypedNeighbours = getNeighbours(RelationAtom.class)
                    .filter(at -> !Sets.intersection(at.getVarNames(), untypedRoleplayers).isEmpty())
                    .collect(toSet());

            ImmutableList.Builder<Type> builder = ImmutableList.builder();
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

                        Iterator<RelationAtom> neighbourIterator = untypedNeighbours.iterator();
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
                    .filter(t -> Sets.intersection(ConceptUtils.nonMetaSups(t), compatibleConfigurations.keySet()).isEmpty())
                    .forEach(builder::add);

            //TODO need to add THING and meta relation type as well to make it complete
            this.possibleTypes = builder.build();
        }
        return possibleTypes;
    }

    /**
     * attempt to infer the relation type of this relation
     * @param sub extra instance information to aid entity type inference
     * @return either this if relation type can't be inferred or a fresh relation with inferred relation type
     */
    private RelationAtom inferRelationType(ConceptMap sub){
        if (getTypePredicate() != null) return this;
        if (sub.containsVar(getPredicateVariable())) return addType(sub.get(getPredicateVariable()).asType());
        List<Type> relationTypes = inferPossibleTypes(sub);
        if (relationTypes.size() == 1) return addType(Iterables.getOnlyElement(relationTypes));
        return this;
    }

    @Override
    public RelationAtom inferTypes(ConceptMap sub) {
        return this
                .inferRelationType(sub)
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
    public Set<Variable> getRoleExpansionVariables(){
        return getRelationPlayers().stream()
                .map(RelationProperty.RolePlayer::getRole)
                .flatMap(CommonUtil::optionalToStream)
                .filter(p -> p.var().isUserDefinedName())
                .filter(p -> !p.getType().isPresent())
                .map(Statement::var)
                .collect(Collectors.toSet());
    }

    @Override
    public Stream<Predicate> getInnerPredicates(){
        return Stream.concat(
                super.getInnerPredicates(),
                getRelationPlayers().stream()
                        .map(RelationProperty.RolePlayer::getRole)
                        .flatMap(CommonUtil::optionalToStream)
                        .filter(vp -> vp.var().isUserDefinedName())
                        .map(vp -> new Pair<>(vp.var(), vp.getType().orElse(null)))
                        .filter(p -> Objects.nonNull(p.getValue()))
                        .map(p -> IdPredicate.create(p.getKey(), Label.of(p.getValue()), getParentQuery()))
        );
    }

    /**
     * attempt to infer role types of this relation and return a fresh relation with inferred role types
     * @return either this if nothing/no roles can be inferred or fresh relation with inferred role types
     */
    private RelationAtom inferRoles(ConceptMap sub){
        //return if all roles known and non-meta
        List<Role> explicitRoles = getExplicitRoles().collect(Collectors.toList());
        Map<Variable, Type> varTypeMap = getParentQuery().getVarTypeMap(sub);
        boolean allRolesMeta = explicitRoles.stream().allMatch(role -> Schema.MetaSchema.isMetaLabel(role.label()));
        boolean roleRecomputationViable = allRolesMeta && (!sub.isEmpty() || !Sets.intersection(varTypeMap.keySet(), getRolePlayers()).isEmpty());
        if (explicitRoles.size() == getRelationPlayers().size() && !roleRecomputationViable) return this;

        TransactionOLTP graph = getParentQuery().tx();
        Role metaRole = graph.getMetaRole();
        List<RelationProperty.RolePlayer> allocatedRelationPlayers = new ArrayList<>();
        RelationType relType = getSchemaConcept() != null? getSchemaConcept().asRelationType() : null;

        //explicit role types from castings
        List<RelationProperty.RolePlayer> inferredRelationPlayers = new ArrayList<>();
        getRelationPlayers().forEach(rp -> {
            Variable varName = rp.getPlayer().var();
            Statement rolePattern = rp.getRole().orElse(null);
            if (rolePattern != null) {
                String roleLabel = rolePattern.getType().orElse(null);
                //allocate if variable role or if label non meta
                if (roleLabel == null || !Schema.MetaSchema.isMetaLabel(Label.of(roleLabel))) {
                    inferredRelationPlayers.add(new RelationProperty.RolePlayer(rolePattern, new Statement(varName)));
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
                        .filter(Concept::isRelationType)
                        .map(Concept::asRelationType)
                        .flatMap(RelationType::roles).collect(toSet());

        //possible role types for each casting based on its type
        Map<RelationProperty.RolePlayer, Set<Role>> mappings = new HashMap<>();
        getRelationPlayers().stream()
                .filter(rp -> !allocatedRelationPlayers.contains(rp))
                .forEach(rp -> {
                    Variable varName = rp.getPlayer().var();
                    Type type = varTypeMap.get(varName);
                    mappings.put(rp, top(type != null? ReasonerUtils.compatibleRoles(type, possibleRoles) : possibleRoles));
                });


        //allocate all unambiguous mappings
        mappings.entrySet().stream()
                .filter(entry -> entry.getValue().size() == 1)
                .forEach(entry -> {
                    RelationProperty.RolePlayer rp = entry.getKey();
                    Variable varName = rp.getPlayer().var();
                    Role role = Iterables.getOnlyElement(entry.getValue());
                    Statement rolePattern = var().type(role.label().getValue());
                    inferredRelationPlayers.add(new RelationProperty.RolePlayer(rolePattern, new Statement(varName)));
                    allocatedRelationPlayers.add(rp);
                });

        //fill in unallocated roles with metarole
        getRelationPlayers().stream()
                .filter(rp -> !allocatedRelationPlayers.contains(rp))
                .forEach(rp -> {
                    Variable varName = rp.getPlayer().var();
                    Statement rolePattern = rp.getRole().orElse(null);

                    rolePattern = rolePattern != null ?
                            rolePattern.type(metaRole.label().getValue()) :
                            var().type(metaRole.label().getValue());
                    inferredRelationPlayers.add(new RelationProperty.RolePlayer(rolePattern, new Statement(varName)));
                });

        Statement relationPattern = relationPattern(getVarName(), inferredRelationPlayers);
        Statement newPattern =
                (isDirect()?
                        relationPattern.isaX(new Statement(getPredicateVariable())) :
                        relationPattern.isa(new Statement(getPredicateVariable()))
                );
        return create(newPattern, this.getPredicateVariable(), this.getTypeId(), this.getPossibleTypes(), this.getParentQuery());
    }

    /**
     * @return map containing roleType - (rolePlayer var - rolePlayer type) pairs
     */
    @Memoized
    public Multimap<Role, Variable> getRoleVarMap() {
        ImmutableMultimap.Builder<Role, Variable> builder = ImmutableMultimap.builder();

        TransactionOLTP graph = getParentQuery().tx();
        getRelationPlayers().forEach(c -> {
            Variable varName = c.getPlayer().var();
            Statement rolePattern = c.getRole().orElse(null);
            if (rolePattern != null) {
                //try directly
                String typeLabel = rolePattern.getType().orElse(null);
                Role role = typeLabel != null ? graph.getRole(typeLabel) : null;
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

    private Multimap<Role, RelationProperty.RolePlayer> getRoleRelationPlayerMap(){
        Multimap<Role, RelationProperty.RolePlayer> roleRelationPlayerMap = ArrayListMultimap.create();
        Multimap<Role, Variable> roleVarMap = getRoleVarMap();
        List<RelationProperty.RolePlayer> relationPlayers = getRelationPlayers();
        roleVarMap.asMap().forEach((role, value) -> {
            Label roleLabel = role.label();
            relationPlayers.stream()
                    .filter(rp -> rp.getRole().isPresent())
                    .forEach(rp -> {
                        Statement roleTypeVar = rp.getRole().orElse(null);
                        String rl = roleTypeVar != null ? roleTypeVar.getType().orElse(null) : null;
                        if (roleLabel != null && roleLabel.equals(Label.of(rl))) {
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
    private Set<List<Pair<RelationProperty.RolePlayer, RelationProperty.RolePlayer>>> getRelationPlayerMappings(RelationAtom parentAtom, UnifierComparison matchType) {
        Multimap<Role, RelationProperty.RolePlayer> childRoleRPMap = this.getRoleRelationPlayerMap();
        Map<Variable, Type> childVarTypeMap = this.getParentQuery().getVarTypeMap(matchType.inferTypes());
        Map<Variable, Type> parentVarTypeMap = parentAtom.getParentQuery().getVarTypeMap(matchType.inferTypes());

        //establish compatible castings for each parent casting
        List<Set<Pair<RelationProperty.RolePlayer, RelationProperty.RolePlayer>>> compatibleMappingsPerParentRP = new ArrayList<>();
        if (parentAtom.getRelationPlayers().size() > this.getRelationPlayers().size()) return new HashSet<>();

        ReasonerQueryImpl childQuery = (ReasonerQueryImpl) getParentQuery();
        Set<Role> childRoles = childRoleRPMap.keySet();
        parentAtom.getRelationPlayers().stream()
                .filter(prp -> prp.getRole().isPresent())
                .forEach(prp -> {
                    Statement parentRolePattern = prp.getRole().orElse(null);
                    if (parentRolePattern == null){
                        throw GraqlQueryException.rolePatternAbsent(this);
                    }
                    String parentRoleLabel = parentRolePattern.getType().orElse(null);

                    if (parentRoleLabel != null) {
                        Variable parentRolePlayer = prp.getPlayer().var();
                        Type parentType = parentVarTypeMap.get(parentRolePlayer);

                        Set<Role> compatibleRoles = ReasonerUtils.compatibleRoles(
                                tx().getSchemaConcept(Label.of(parentRoleLabel)),
                                parentType,
                                childRoles);

                        List<RelationProperty.RolePlayer> compatibleRelationPlayers = new ArrayList<>();
                        compatibleRoles.stream()
                                .filter(childRoleRPMap::containsKey)
                                .forEach(role ->
                                        childRoleRPMap.get(role).stream()
                                                //check for inter-type compatibility
                                                .filter(crp -> {
                                                    Variable childVar = crp.getPlayer().var();
                                                    Type childType = childVarTypeMap.get(childVar);
                                                    return matchType.typePlayability(childQuery, childVar, parentType)
                                                            && matchType.typeCompatibility(parentType, childType);
                                                })
                                                //check for substitution compatibility
                                                .filter(crp -> {
                                                    Set<Atomic> parentIds = parentAtom.getPredicates(prp.getPlayer().var(), IdPredicate.class).collect(toSet());
                                                    Set<Atomic> childIds = this.getPredicates(crp.getPlayer().var(), IdPredicate.class).collect(toSet());
                                                    return matchType.idCompatibility(parentIds, childIds);
                                                })
                                                //check for value predicate compatibility
                                                .filter(crp -> {
                                                    Set<Atomic> parentVP = parentAtom.getPredicates(prp.getPlayer().var(), ValuePredicate.class).collect(toSet());
                                                    Set<Atomic> childVP = this.getPredicates(crp.getPlayer().var(), ValuePredicate.class).collect(toSet());
                                                    return matchType.valueCompatibility(parentVP, childVP);
                                                })
                                                //check linked resources
                                                .filter(crp -> {
                                                    Variable parentVar = prp.getPlayer().var();
                                                    Variable childVar = crp.getPlayer().var();
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
                    List<RelationProperty.RolePlayer> listChildRps = list.stream().map(Pair::getKey).collect(Collectors.toList());
                    //NB: this preserves cardinality instead of removing all occuring instances which is what we want
                    return ReasonerUtils.listDifference(listChildRps, this.getRelationPlayers()).isEmpty();
                })
                //check all parent rps mapped
                .filter(list -> {
                    List<RelationProperty.RolePlayer> listParentRps = list.stream().map(Pair::getValue).collect(Collectors.toList());
                    return listParentRps.containsAll(parentAtom.getRelationPlayers());
                })
                .collect(toSet());
    }

    @Override
    public Unifier getUnifier(Atom pAtom, UnifierComparison unifierType){
        return getMultiUnifier(pAtom, unifierType).getUnifier();
    }

    @Override
    public MultiUnifier getMultiUnifier(Atom parentAtom, UnifierComparison unifierType) {
        Unifier baseUnifier = super.getUnifier(parentAtom, unifierType);
        if (baseUnifier == null){ return MultiUnifierImpl.nonExistent();}

        Set<Unifier> unifiers = new HashSet<>();
        if (parentAtom.isRelation()) {
            RelationAtom parent = parentAtom.toRelationAtom();

            //NB: if two atoms are equal and their sub and type mappings are equal we return the identity unifier
            //this is important for cases like unifying ($r1: $x, $r2: $y) with itself
            if (this.equals(parent)
                    && unifierType != UnifierType.SUBSUMPTIVE
                    && this.getPartialSubstitutions().collect(toSet()).equals(parent.getPartialSubstitutions().collect(toSet()))
                    && this.getTypeConstraints().collect(toSet()).equals(parent.getTypeConstraints().collect(toSet()))){
                return MultiUnifierImpl.trivial();
            }

            boolean unifyRoleVariables = parent.getRelationPlayers().stream()
                    .map(RelationProperty.RolePlayer::getRole)
                    .flatMap(CommonUtil::optionalToStream)
                    .anyMatch(rp -> rp.var().isUserDefinedName());
            getRelationPlayerMappings(parent, unifierType)
                    .forEach(mappingList -> {
                        Multimap<Variable, Variable> varMappings = HashMultimap.create();
                        mappingList.forEach(rpm -> {
                            //add role player mapping
                            varMappings.put(rpm.getKey().getPlayer().var(), rpm.getValue().getPlayer().var());

                            //add role var mapping if needed
                            Statement childRolePattern = rpm.getKey().getRole().orElse(null);
                            Statement parentRolePattern = rpm.getValue().getRole().orElse(null);
                            if (parentRolePattern != null && childRolePattern != null && unifyRoleVariables){
                                varMappings.put(childRolePattern.var(), parentRolePattern.var());
                            }

                        });
                        unifiers.add(baseUnifier.merge(new UnifierImpl(varMappings)));
                    });
        } else {
            unifiers.add(baseUnifier);
        }
        return new MultiUnifierImpl(unifiers);
    }

    private HashMultimap<Variable, Role> getVarRoleMap() {
        HashMultimap<Variable, Role> map = HashMultimap.create();
        getRoleVarMap().asMap().forEach((key, value) -> value.forEach(var -> map.put(var, key)));
        return map;
    }
    @Override
    public SemanticDifference semanticDifference(Atom p, Unifier unifier) {
        SemanticDifference baseDiff = super.semanticDifference(p, unifier);
        if (!p.isRelation()) return baseDiff;
        RelationAtom parentAtom = (RelationAtom) p;
        Set<VariableDefinition> diff = new HashSet<>();

        Set<Variable> parentRoleVars= parentAtom.getRoleExpansionVariables();
        HashMultimap<Variable, Role> childVarRoleMap = this.getVarRoleMap();
        HashMultimap<Variable, Role> parentVarRoleMap = parentAtom.getVarRoleMap();
        unifier.mappings().forEach( m -> {
            Variable childVar = m.getKey();
            Variable parentVar = m.getValue();
            Set<Role> childRoles = childVarRoleMap.get(childVar);
            Set<Role> parentRoles = parentVarRoleMap.get(parentVar);
            Role role = null;
            if(parentRoleVars.contains(parentVar)){
                Set<Label> roleLabel = this.getRelationPlayers().stream()
                        .filter(rp -> rp.getRole().isPresent())
                        .filter(rp -> rp.getRole().get().var().equals(childVar))
                        .map(rp -> Label.of(rp.getRole().get().getType().get()))
                        .collect(toSet());
                role = tx().getRole(Iterables.getOnlyElement(roleLabel).getValue());
            }
            diff.add(new VariableDefinition(childVar,null, role, bottom(Sets.difference(childRoles, parentRoles)), new HashSet<>()));
        });
        return baseDiff.merge(new SemanticDifference(diff));
    }

    @Override
    public Stream<ConceptMap> materialise(){
        RelationType relationType = getSchemaConcept().asRelationType();
        Multimap<Role, Variable> roleVarMap = getRoleVarMap();
        ConceptMap substitution = getParentQuery().getSubstitution();

        //if the relation already exists, only assign roleplayers, otherwise create a new relation
        Relation relation = substitution.containsVar(getVarName())?
                substitution.get(getVarName()).asRelation() :
                RelationTypeImpl.from(relationType).addRelationInferred();

        roleVarMap.asMap()
                .forEach((key, value) -> value.forEach(var -> relation.assign(key, substitution.get(var).asThing())));

        ConceptMap relationSub = ConceptUtils.mergeAnswers(
                getRoleSubstitution(),
                getVarName().isUserDefinedName()?
                        new ConceptMap(ImmutableMap.of(getVarName(), relation)) :
                        new ConceptMap()
        );

        ConceptMap answer = ConceptUtils.mergeAnswers(substitution, relationSub);
        return Stream.of(answer);
    }

    /**
     * if any {@link Role} variable of the parent is user defined rewrite ALL {@link Role} variables to user defined (otherwise unification is problematic)
     * @param parentAtom parent atom that triggers rewrite
     * @return new relation atom with user defined {@link Role} variables if necessary or this
     */
    private RelationAtom rewriteWithVariableRoles(Atom parentAtom){
        if (!parentAtom.requiresRoleExpansion()) return this;

        Statement relVar = getPattern().getProperty(IsaProperty.class)
                .map(prop -> new Statement(getVarName()).isa(prop.type()))
                .orElse(new StatementThing(getVarName()));

        for (RelationProperty.RolePlayer rp: getRelationPlayers()) {
            Statement rolePattern = rp.getRole().orElse(null);
            if (rolePattern != null) {
                Variable roleVar = rolePattern.var();
                String roleLabel = rolePattern.getType().orElse(null);
                relVar = relVar.rel(new Statement(roleVar.asUserDefined()).type(roleLabel), rp.getPlayer());
            } else {
                relVar = relVar.rel(rp.getPlayer());
            }
        }
        return create(relVar, this.getPredicateVariable(), this.getTypeId(), this.getPossibleTypes(), this.getParentQuery());
    }

    /**
     * @param parentAtom parent atom that triggers rewrite
     * @return new relation atom with user defined name if necessary or this
     */
    private RelationAtom rewriteWithRelationVariable(Atom parentAtom){
        if (this.getVarName().isUserDefinedName() || !parentAtom.getVarName().isUserDefinedName()) return this;
        return rewriteWithRelationVariable();
    }

    @Override
    public RelationAtom rewriteWithRelationVariable(){
        StatementInstance newVar = new StatementThing(new Variable().asUserDefined());
        Statement relVar = getPattern().getProperty(IsaProperty.class)
                .map(prop -> newVar.isa(prop.type()))
                .orElse(newVar);

        for (RelationProperty.RolePlayer c: getRelationPlayers()) {
            Statement roleType = c.getRole().orElse(null);
            if (roleType != null) {
                relVar = relVar.rel(roleType, c.getPlayer());
            } else {
                relVar = relVar.rel(c.getPlayer());
            }
        }
        return create(relVar, this.getPredicateVariable(), this.getTypeId(), this.getPossibleTypes(), this.getParentQuery());
    }

    @Override
    public RelationAtom rewriteWithTypeVariable(){
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
