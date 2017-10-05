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

import ai.grakn.concept.ConceptId;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.MultiUnifier;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.admin.UnifierComparison;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.admin.VarProperty;
import ai.grakn.graql.internal.reasoner.MultiUnifierImpl;
import ai.grakn.graql.internal.reasoner.ResolutionPlan;
import ai.grakn.graql.internal.reasoner.atom.binary.TypeAtom;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;
import ai.grakn.graql.internal.reasoner.atom.predicate.NeqPredicate;
import ai.grakn.graql.internal.reasoner.atom.predicate.Predicate;
import ai.grakn.graql.internal.reasoner.rule.InferenceRule;
import ai.grakn.graql.internal.reasoner.rule.RuleUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ai.grakn.graql.internal.reasoner.utils.ReasonerUtils.typesCompatible;

/**
 *
 * <p>
 * {@link AtomicBase} extension defining specialised functionalities.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public abstract class Atom extends AtomicBase {

    private int basePriority = Integer.MAX_VALUE;
    private Set<InferenceRule> applicableRules = null;

    protected Atom(VarPatternAdmin pattern, ReasonerQuery par) {
        super(pattern, par);
    }
    protected Atom(Atom a) {
        super(a);
        this.applicableRules = a.applicableRules;
    }

    public abstract boolean isRuleApplicable(InferenceRule child);

    /**
     * @return set of applicable rules - does detailed (slow) check for applicability
     */
    public Stream<InferenceRule> getApplicableRules() {
        if (applicableRules == null) {
            applicableRules = new HashSet<>();
            return RuleUtil.getRulesWithType(getSchemaConcept(), tx())
                    .map(rule -> new InferenceRule(rule, tx()))
                    .filter(this::isRuleApplicable)
                    .map(r -> r.rewriteToUserDefined(this))
                    .peek(applicableRules::add);
        }
        return applicableRules.stream();
    }

    @Override
    public boolean isRuleResolvable() {
        return getApplicableRules().findFirst().isPresent();
    }

    @Override
    public boolean isAtom(){ return true;}

    @Override
    public boolean isRecursive(){
        if (isResource() || getSchemaConcept() == null) return false;
        SchemaConcept schemaConcept = getSchemaConcept();
        return getApplicableRules()
                .filter(rule -> rule.getBody().selectAtoms().stream()
                        .filter(at -> Objects.nonNull(at.getSchemaConcept()))
                        .filter(at -> typesCompatible(schemaConcept, at.getSchemaConcept())).findFirst().isPresent())
                .filter(this::isRuleApplicable)
                .findFirst().isPresent();
    }

    /**
     * @return var properties this atom (its pattern) contains
     */
    public Set<VarProperty> getVarProperties() {
        return getPattern().asVarPattern().getProperties().collect(Collectors.toSet());
    }

    /**
     * @return partial substitutions for this atom (NB: instances)
     */
    protected Stream<IdPredicate> getPartialSubstitutions(){ return Stream.empty();}

    /**
     * @return set of variables that need to be have their roles expanded
     */
    public Set<Var> getRoleExpansionVariables(){ return new HashSet<>();}

    /**
     * compute base resolution priority of this atom
     * @return priority value
     */
    private int computePriority(){
        return computePriority(getPartialSubstitutions().map(IdPredicate::getVarName).collect(Collectors.toSet()));
    }

    /**
     * compute resolution priority based on provided substitution variables
     * @param subbedVars variables having a substitution
     * @return resolution priority value
     */
    public int computePriority(Set<Var> subbedVars){
        int priority = 0;
        priority += Sets.intersection(getVarNames(), subbedVars).size() * ResolutionPlan.PARTIAL_SUBSTITUTION;
        priority += isRuleResolvable()? ResolutionPlan.RULE_RESOLVABLE_ATOM : 0;
        priority += isRecursive()? ResolutionPlan.RECURSIVE_ATOM : 0;

        priority += getTypeConstraints().count() * ResolutionPlan.GUARD;
        Set<Var> otherVars = getParentQuery().getAtoms().stream()
                .filter(a -> a != this)
                .flatMap(at -> at.getVarNames().stream())
                .collect(Collectors.toSet());
        priority += Sets.intersection(getVarNames(), otherVars).size() * ResolutionPlan.BOUND_VARIABLE;

        //inequality predicates with unmapped variable
        priority += getPredicates(NeqPredicate.class)
                .map(Predicate::getPredicate)
                .filter(v -> !subbedVars.contains(v)).count() * ResolutionPlan.INEQUALITY_PREDICATE;
        return priority;
    }

    /**
     * @return measure of priority with which this atom should be resolved
     */
    public int baseResolutionPriority(){
        if (basePriority == Integer.MAX_VALUE) {
            basePriority = computePriority();
        }
        return basePriority;
    }

    /**
     * @return true if the atom requires materialisation in order to be referenced
     */
    public boolean requiresMaterialisation(){ return false; }

    /**
     * @return true if the atom requires role expansion
     */
    public boolean requiresRoleExpansion(){ return false; }

    /**
     * @return corresponding type if any
     */
    public abstract SchemaConcept getSchemaConcept();

    /**
     * @return type id of the corresponding type if any
     */
    public abstract ConceptId getTypeId();

    /**
     * @return value variable name
     */
    public abstract Var getPredicateVariable();

    /**
     * @return set of predicates relevant to this atom
     */
    public Stream<Predicate> getPredicates() {
        return getPredicates(Predicate.class);
    }

    /**
     * @param type the class of {@link Predicate} to return
     * @param <T> the type of {@link Predicate} to return
     * @return stream of predicates relevant to this atom
     */
    public <T extends Predicate> Stream<T> getPredicates(Class<T> type) {
        return getParentQuery().getAtoms(type).filter(atom -> !Sets.intersection(this.getVarNames(), atom.getVarNames()).isEmpty());
    }

    public IdPredicate getIdPredicate(Var var){
        return getPredicates(IdPredicate.class).filter(p -> p.getVarName().equals(var)).findFirst().orElse(null);
    }

    public abstract Stream<Predicate> getInnerPredicates();

    /**
     * @param type the class of {@link Predicate} to return
     * @param <T> the type of {@link Predicate} to return
     * @return stream of predicates relevant to this atom
     */
    public <T extends Predicate> Stream<T> getInnerPredicates(Class<T> type){
        return getInnerPredicates().filter(type::isInstance).map(type::cast);
    }

    /**
     * @return set of types relevant to this atom
     */
    public Stream<TypeAtom> getTypeConstraints(){
        return getParentQuery().getAtoms(TypeAtom.class)
                .filter(atom -> atom != this)
                .filter(atom -> containsVar(atom.getVarName()));
    }

    /**
     * @param type the class of {@link Predicate} to return
     * @param <T> the type of neighbour {@link Atomic} to return
     * @return neighbours of this atoms, i.e. atoms connected to this atom via shared variable
     */
    public <T extends Atomic> Stream<T> getNeighbours(Class<T> type){
        return getParentQuery().getAtoms(type)
                .filter(at -> at != this)
                .filter(at -> !Sets.intersection(this.getVarNames(), at.getVarNames()).isEmpty());
    }

    /**
     * @return set of constraints of this atom (predicates + types) that are not selectable
     */
    public Stream<Atomic> getNonSelectableConstraints() {
        return Stream.concat(
                getPredicates(),
                getTypeConstraints().filter(at -> !at.isSelectable())
                );
    }

    /**
     * @return set of type atoms that describe specific role players or resource owner
     */
    public Set<TypeAtom> getSpecificTypeConstraints() { return new HashSet<>();}

    @Override
    public Atom inferTypes(){ return this; }

    /**
     * @param sub partial substitution
     * @return list of possible atoms obtained by applying type inference
     */
    public List<Atom> atomOptions(Answer sub){ return Lists.newArrayList(inferTypes());}

    /**
     * @param type to be added to this {@link Atom}
     * @return new {@link Atom} with specified type
     */
    public Atom addType(SchemaConcept type){ return this;}

    /**
     * rewrites the atom to one with user defined name
     * @param parentAtom parent atom that triggers rewrite
     * @return pair of (rewritten atom, unifiers required to unify child with rewritten atom)
     */
    public Atom rewriteToUserDefined(Atom parentAtom){ return this;}

    /**
     * @param parentAtom atom to be unified with
     * @return corresponding unifier
     */
    public abstract Unifier getUnifier(Atom parentAtom);
    /**
     * find the (multi) unifier with parent atom
     * @param parentAtom atom to be unified with
     * @param unifierType type of unifier to be computed
     * @return multiunifier
     */
    public MultiUnifier getMultiUnifier(Atom parentAtom, UnifierComparison unifierType){ return new MultiUnifierImpl(getUnifier(parentAtom));}
}
