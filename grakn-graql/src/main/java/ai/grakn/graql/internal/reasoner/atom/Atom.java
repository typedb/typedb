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
import ai.grakn.concept.Rule;
import ai.grakn.concept.Type;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.reasoner.utils.ReasonerUtils;
import ai.grakn.graql.internal.reasoner.atom.binary.TypeAtom;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;
import ai.grakn.graql.internal.reasoner.atom.predicate.Predicate;
import ai.grakn.graql.internal.reasoner.atom.predicate.ValuePredicate;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueryImpl;
import ai.grakn.graql.internal.reasoner.rule.InferenceRule;
import com.google.common.collect.Sets;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ai.grakn.graql.internal.reasoner.utils.ReasonerUtils.checkTypesCompatible;

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

    private Type type = null;
    protected ConceptId typeId = null;
    private int basePriority = Integer.MAX_VALUE;
    protected Set<InferenceRule> applicableRules = null;

    protected Atom(VarPatternAdmin pattern, ReasonerQuery par) { super(pattern, par);}
    protected Atom(Atom a) {
        super(a);
        this.type = a.type;
        this.typeId = a.typeId;
        this.applicableRules = a.applicableRules;
    }

    @Override
    public boolean isAtom(){ return true;}

    /**
     * @return true if the atom corresponds to a atom
     * */
    public boolean isType(){ return false;}

    /**
     * @return true if the atom corresponds to a non-unary atom
     * */
    public boolean isRelation(){return false;}

    /**
     * @return true if the atom corresponds to a resource atom
     * */
    public boolean isResource(){ return false;}

    /**
     * @return partial substitutions for this atom (NB: instances)
     */
    public Set<IdPredicate> getPartialSubstitutions(){ return new HashSet<>();}

    /**
     * compute base resolution priority of this atom
     * @return priority value
     */
    public int computePriority(){
        return computePriority(getPartialSubstitutions().stream().map(IdPredicate::getVarName).collect(Collectors.toSet()));
    }

    /**
     * compute resolution priority based on provided substitution variables
     * @param subbedVars variables having a substitution
     * @return resolution priority value
     */
    public int computePriority(Set<Var> subbedVars){
        int priority = 0;
        priority += Sets.intersection(getVarNames(), subbedVars).size() * ResolutionStrategy.PARTIAL_SUBSTITUTION;
        priority += isRuleResolvable()? ResolutionStrategy.RULE_RESOLVABLE_ATOM : 0;
        priority += isRecursive()? ResolutionStrategy.RECURSIVE_ATOM : 0;

        priority += getTypeConstraints().size() * ResolutionStrategy.GUARD;
        Set<Var> otherVars = getParentQuery().getAtoms().stream()
                .filter(a -> a != this)
                .flatMap(at -> at.getVarNames().stream())
                .collect(Collectors.toSet());
        priority += Sets.intersection(getVarNames(), otherVars).size() * ResolutionStrategy.BOUND_VARIABLE;
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

    public abstract boolean isRuleApplicable(InferenceRule child);

    /**
     * @return set of potentially applicable rules - does shallow (fast) check for applicability
     */
    private Set<Rule> getPotentialRules(){
        Type type = getType();
        return type != null ?
                type.subTypes().stream().flatMap(t -> t.getRulesOfConclusion().stream()).collect(Collectors.toSet()) :
                ReasonerUtils.getRules(graph());
    }

    /**
     * @return set of applicable rules - does detailed (slow) check for applicability
     */
    public Set<InferenceRule> getApplicableRules() {
        if (applicableRules == null) {
            applicableRules = getPotentialRules().stream()
                    .map(rule -> new InferenceRule(rule, graph()))
                    .filter(this::isRuleApplicable)
                    .collect(Collectors.toSet());
        }
        return applicableRules;
    }

    @Override
    public boolean isRuleResolvable() {
        return !getApplicableRules().isEmpty();
    }

    @Override
    public boolean isRecursive(){
        if (isResource() || getType() == null) return false;
        Type type = getType();
        return getApplicableRules().stream()
                .filter(rule -> rule.getBody().selectAtoms().stream()
                        .filter(at -> Objects.nonNull(at.getType()))
                        .filter(at -> checkTypesCompatible(type, at.getType())).findFirst().isPresent())
                .filter(this::isRuleApplicable)
                .findFirst().isPresent();
    }

    /**
     * @return true if the atom can constitute a head of a rule
     */
    public boolean isAllowedToFormRuleHead(){ return false; }

    /**
     * @return true if the atom requires materialisation in order to be referenced
     */
    public boolean requiresMaterialisation(){ return false; }

    /**
     * @return corresponding type if any
     */
    public Type getType(){
        if (type == null && typeId != null) {
            type = getParentQuery().graph().getConcept(typeId).asType();
        }
        return type;
    }

    /**
     * @return type id of the corresponding type if any
     */
    public ConceptId getTypeId(){ return typeId;}

    /**
     * @return value variable name
     */
    public Var getValueVariable() {
        throw new IllegalArgumentException("getValueVariable called on Atom object " + getPattern());
    }

    /**
     * @return set of predicates relevant to this atom
     */
    public Set<Predicate> getPredicates() {
        Set<Predicate> predicates = new HashSet<>();
        predicates.addAll(getValuePredicates());
        predicates.addAll(getIdPredicates());
        return predicates;
    }

    /**
     * @return set of id predicates relevant to this atom
     */
    public Set<IdPredicate> getIdPredicates() {
        return ((ReasonerQueryImpl) getParentQuery()).getIdPredicates().stream()
                .filter(atom -> containsVar(atom.getVarName()))
                .collect(Collectors.toSet());
    }

    /**
     * @return set of value predicates relevant to this atom
     */
    public Set<ValuePredicate> getValuePredicates(){
        return ((ReasonerQueryImpl) getParentQuery()).getValuePredicates().stream()
                .filter(atom -> atom.getVarName().equals(getValueVariable()))
                .collect(Collectors.toSet());
    }

    /**
     * @return set of types relevant to this atom
     */
    public Set<TypeAtom> getTypeConstraints(){
        Set<TypeAtom> relevantTypes = new HashSet<>();
        //ids from indirect types
        ((ReasonerQueryImpl) getParentQuery()).getTypeConstraints().stream()
                .filter(atom -> atom != this)
                .filter(atom -> containsVar(atom.getVarName()))
                .forEach(relevantTypes::add);
        return relevantTypes;
    }

    /**
     * @return neighbours of this atoms, i.e. atoms connected to this atom via shared variable
     */
    public Stream<Atom> getNeighbours(){
        return getParentQuery().getAtoms().stream()
                .filter(Atomic::isAtom).map(at -> (Atom) at)
                .filter(at -> at != this)
                .filter(at -> !Sets.intersection(this.getVarNames(), at.getVarNames()).isEmpty());
    }

    /**
     * @return set of constraints of this atom (predicates + types) that are not selectable
     */
    public Set<Atomic> getNonSelectableConstraints() {
        Set<Atom> types = getTypeConstraints().stream()
                .filter(at -> !at.isSelectable())
                .collect(Collectors.toSet());
        return Sets.union(types, getPredicates());
    }

    public Set<IdPredicate> getUnmappedIdPredicates(){ return new HashSet<>();}
    public Set<TypeAtom> getUnmappedTypeConstraints(){ return new HashSet<>();}
    public Set<TypeAtom> getMappedTypeConstraints() { return new HashSet<>();}
    public Set<Unifier> getPermutationUnifiers(Atom headAtom){ return new HashSet<>();}

    /**
     * infers types (type, role types) fo the atom if applicable/possible
     */
    public void inferTypes(){}

    /**
     * rewrites the atom to one with user defined name
     * @return pair of (rewritten atom, unifiers required to unify child with rewritten atom)
     */
    public Atom rewriteToUserDefined(){ return this;}

    /**
     * find unifier with parent atom
     * @param parentAtom atom to be unified with
     * @return unifier
     */
    public abstract Unifier getUnifier(Atomic parentAtom);
}
