/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
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
import ai.grakn.concept.SchemaConcept;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.MultiUnifier;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.admin.UnifierComparison;
import ai.grakn.graql.admin.VarProperty;
import ai.grakn.graql.internal.pattern.property.DirectIsaProperty;
import ai.grakn.graql.internal.query.QueryAnswer;
import ai.grakn.graql.internal.reasoner.MultiUnifierImpl;
import ai.grakn.graql.internal.reasoner.atom.binary.OntologicalAtom;
import ai.grakn.graql.internal.reasoner.atom.binary.RelationshipAtom;
import ai.grakn.graql.internal.reasoner.atom.binary.ResourceAtom;
import ai.grakn.graql.internal.reasoner.atom.binary.TypeAtom;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;
import ai.grakn.graql.internal.reasoner.atom.predicate.Predicate;
import ai.grakn.graql.internal.reasoner.rule.InferenceRule;
import ai.grakn.graql.internal.reasoner.rule.RuleUtils;
import ai.grakn.util.ErrorMessage;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import javax.annotation.Nullable;
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

    //NB: protected to be able to assign it when copying
    protected Set<InferenceRule> applicableRules = null;

    public RelationshipAtom toRelationshipAtom(){
        throw GraqlQueryException.illegalAtomConversion(this);
    }

    @Override
    public boolean isAtom(){ return true;}

    public boolean isRuleResolvable() {
        return getApplicableRules().findFirst().isPresent();
    }

    public boolean isRecursive(){
        if (isResource() || getSchemaConcept() == null) return false;
        SchemaConcept schemaConcept = getSchemaConcept();
        return getApplicableRules()
                .filter(rule -> rule.getBody().selectAtoms().stream()
                        .filter(at -> Objects.nonNull(at.getSchemaConcept()))
                        .anyMatch(at -> typesCompatible(schemaConcept, at.getSchemaConcept())))
                .anyMatch(this::isRuleApplicable);
    }

    /**
     * @return true if the atom is ground (all variables are bound)
     */
    public boolean isGround(){
        Set<Var> varNames = getVarNames();
        return Stream.concat(
                getPredicates(),
                getInnerPredicates())
                .map(AtomicBase::getVarName)
                .allMatch(varNames::contains);
    }

    /**
     * @return true if this atom is bounded - via substitution/specific resource or schema
     */
    public boolean isBounded(){
        return isResource() && ((ResourceAtom) this).isSpecific()
                || this instanceof OntologicalAtom
                || isGround();
    }

    /**
     * @return true if this atom is disconnected (doesn't have neighbours)
     */
    public boolean isDisconnected(){
        return isSelectable()
            && getParentQuery().getAtoms(Atom.class)
                .filter(Atomic::isSelectable)
                .filter(at -> !at.equals(this))
                .allMatch(at -> Sets.intersection(at.getVarNames(), this.getVarNames()).isEmpty());
    }

    /**
     * @return true if this atom requires direct schema lookups
     */
    public boolean requiresSchema(){
        return getSchemaConcept() == null || this instanceof OntologicalAtom;
    }

    public abstract Class<? extends VarProperty> getVarPropertyClass();

    @Override
    public Set<String> validateAsRuleHead(Rule rule){
        Set<String> errors = new HashSet<>();
        Set<Atomic> parentAtoms = getParentQuery().getAtoms(Atomic.class).filter(at -> !at.equals(this)).collect(Collectors.toSet());
        Set<Var> varNames = Sets.difference(
                getVarNames(),
                this.getInnerPredicates().map(Atomic::getVarName).collect(Collectors.toSet())
        );
        boolean unboundVariables = varNames.stream()
                .anyMatch(var -> parentAtoms.stream().noneMatch(at -> at.getVarNames().contains(var)));
        if (unboundVariables) {
            errors.add(ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_ATOM_WITH_UNBOUND_VARIABLE.getMessage(rule.getThen(), rule.getLabel()));
        }

        SchemaConcept schemaConcept = getSchemaConcept();
        if (schemaConcept == null){
            errors.add(ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_ATOM_WITH_AMBIGUOUS_SCHEMA_CONCEPT.getMessage(rule.getThen(), rule.getLabel()));
        } else if (schemaConcept.isImplicit()){
            errors.add(ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_ATOM_WITH_IMPLICIT_SCHEMA_CONCEPT.getMessage(rule.getThen(), rule.getLabel()));
        }
        return errors;
    }

    /**
     * @return var properties this atom (its pattern) contains
     */
    public Stream<VarProperty> getVarProperties(){
        return getCombinedPattern().admin().varPatterns().stream().flatMap(vp -> vp.getProperties(getVarPropertyClass()));
    }

    /**
     * @return partial substitutions for this atom (NB: instances)
     */
    public Stream<IdPredicate> getPartialSubstitutions(){ return Stream.empty();}

    /**
     * @return set of variables that need to be have their roles expanded
     */
    public Set<Var> getRoleExpansionVariables(){ return new HashSet<>();}

    private boolean isRuleApplicable(InferenceRule child){
        return isRuleApplicableViaAtom(child.getRuleConclusionAtom());
    }

    protected abstract boolean isRuleApplicableViaAtom(Atom headAtom);

    /**
     * @return set of potentially applicable rules - does shallow (fast) check for applicability
     */
    protected Stream<Rule> getPotentialRules(){
        boolean isDirect = getPattern().admin().getProperties(DirectIsaProperty.class).findFirst().isPresent();
        return getPossibleTypes().stream()
                .flatMap(type -> RuleUtils.getRulesWithType(type, isDirect, tx()))
                .distinct();
    }

    /**
     * @return set of applicable rules - does detailed (slow) check for applicability
     */
    public Stream<InferenceRule> getApplicableRules() {
        if (applicableRules == null) {
            applicableRules = new HashSet<>();
            getPotentialRules()
                    .map(rule -> new InferenceRule(rule, tx()))
                    .filter(this::isRuleApplicable)
                    .map(r -> r.rewrite(this))
                    .forEach(applicableRules::add);
        }
        return applicableRules.stream();
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
     * @param var variable of interest
     * @return id predicate referring to prescribed variable
     */
    @Nullable
    public IdPredicate getIdPredicate(Var var){
        return getPredicate(var, IdPredicate.class);
    }

    /**
     * @param var variable the predicate refers to
     * @param type predicate type
     * @param <T> predicate type generic
     * @return specific predicate referring to provided variable
     */
    @Nullable
    public <T extends Predicate> T getPredicate(Var var, Class<T> type){
        return getPredicates(type).filter(p -> p.getVarName().equals(var)).findFirst().orElse(null);
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
                .filter(atom -> atom != this)
                .filter(atom -> !Sets.intersection(this.getVarNames(), atom.getVarNames()).isEmpty());
    }

    /**
     * @return set of constraints of this atom (predicates + types) that are not selectable
     */
    public Stream<Atomic> getNonSelectableConstraints() {
        return Stream.concat(
                Stream.concat(
                        getPredicates(),
                        getPredicates().flatMap(AtomicBase::getPredicates)
                ),
                getTypeConstraints().filter(at -> !at.isSelectable())
        );
    }

    /**
     * @return set of type atoms that describe specific role players or resource owner
     */
    public Set<TypeAtom> getSpecificTypeConstraints() { return new HashSet<>();}

    @Override
    public Atom inferTypes(){ return inferTypes(new QueryAnswer()); }

    @Override
    public Atom inferTypes(Answer sub){ return this; }

    /**
     * @return list of types this atom can take
     */
    public ImmutableList<SchemaConcept> getPossibleTypes(){ return ImmutableList.of(getSchemaConcept());}

    /**
     * @param sub partial substitution
     * @return list of possible atoms obtained by applying type inference
     */
    public List<Atom> atomOptions(Answer sub){ return Lists.newArrayList(inferTypes(sub));}

    /**
     * @param type to be added to this {@link Atom}
     * @return new {@link Atom} with specified type
     */
    public Atom addType(SchemaConcept type){ return this;}

    public Stream<Answer> materialise(){ return Stream.empty();}

    public abstract Atom rewriteWithTypeVariable();

    /**
     * rewrites the atom to user-defined type variable
     * @param parentAtom parent atom that triggers rewrite
     * @return rewritten atom
     */
    protected Atom rewriteWithTypeVariable(Atom parentAtom){
        if (!parentAtom.getPredicateVariable().isUserDefinedName()) return this;
        return rewriteWithTypeVariable();
    }

    /**
     * rewrites the atom to one with user defined relation variable
     * @return rewritten atom
     */
    public Atom rewriteWithRelationVariable(){ return this;}

    /**
     * rewrites the atom to one with suitably user-defined names depending on provided parent
     * @param parentAtom parent atom that triggers rewrite
     * @return rewritten atom
     */
    public abstract Atom rewriteToUserDefined(Atom parentAtom);

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
