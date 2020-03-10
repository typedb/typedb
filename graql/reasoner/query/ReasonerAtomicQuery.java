/*
 * Copyright (C) 2020 Grakn Labs
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
 *
 */

package grakn.core.graql.reasoner.query;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import grakn.common.util.Pair;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.kb.graql.executor.TraversalExecutor;
import grakn.core.graql.reasoner.CacheCasting;
import grakn.core.graql.reasoner.ReasoningContext;
import grakn.core.graql.reasoner.atom.Atom;
import grakn.core.graql.reasoner.atom.AtomicUtil;
import grakn.core.graql.reasoner.atom.PropertyAtomicFactory;
import grakn.core.graql.reasoner.atom.binary.TypeAtom;
import grakn.core.graql.reasoner.atom.predicate.VariablePredicate;
import grakn.core.graql.reasoner.cache.SemanticDifference;
import grakn.core.graql.reasoner.rule.RuleUtils;
import grakn.core.graql.reasoner.state.AnswerPropagatorState;
import grakn.core.graql.reasoner.state.AnswerState;
import grakn.core.graql.reasoner.state.AtomicState;
import grakn.core.graql.reasoner.state.AtomicStateProducer;
import grakn.core.graql.reasoner.state.CacheCompletionState;
import grakn.core.graql.reasoner.state.ResolutionState;
import grakn.core.graql.reasoner.state.VariableComparisonState;
import grakn.core.graql.reasoner.unifier.MultiUnifierImpl;
import grakn.core.graql.reasoner.unifier.UnifierType;
import grakn.core.graql.reasoner.utils.ReasonerUtils;
import grakn.core.kb.graql.planning.gremlin.TraversalPlanFactory;
import grakn.core.kb.graql.reasoner.atom.Atomic;
import grakn.core.kb.graql.reasoner.cache.QueryCache;
import grakn.core.kb.graql.reasoner.query.ReasonerQuery;
import grakn.core.kb.graql.reasoner.unifier.MultiUnifier;
import grakn.core.kb.graql.reasoner.unifier.Unifier;
import graql.lang.pattern.Conjunction;
import graql.lang.statement.Statement;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Base reasoner atomic query. An atomic query is a query constrained to having at most one rule-resolvable atom
 * together with its accompanying constraints (predicates and types).
 */
public class ReasonerAtomicQuery extends ReasonerQueryImpl {

    private final Atom atom;

    /**
     * BUILDER constructor should only be used in the ReasonerQueryFactory because it utilises
     * the setAtomSet method to work around an ordering constraint
     */
    ReasonerAtomicQuery(Conjunction<Statement> pattern, PropertyAtomicFactory propertyAtomicFactory,
                        TraversalPlanFactory traversalPlanFactory, TraversalExecutor traversalExecutor,
                        ReasoningContext ctx) {
        super(pattern, propertyAtomicFactory, traversalPlanFactory, traversalExecutor, ctx);
        this.atom = Iterables.getOnlyElement(selectAtoms()::iterator);
    }

    /**
     * Copy constructor
     * @param query
     */
    ReasonerAtomicQuery(ReasonerQueryImpl query) {
        super(query);
        this.atom = Iterables.getOnlyElement(selectAtoms()::iterator);
    }

    private ReasonerAtomicQuery(List<Atom> atomsToPropagate,  TraversalPlanFactory traversalPlanFactory, TraversalExecutor traversalExecutor, ReasoningContext ctx) {
        super(atomsToPropagate, traversalPlanFactory, traversalExecutor, ctx);
        this.atom = Iterables.getOnlyElement(selectAtoms()::iterator);
    }

    ReasonerAtomicQuery(Atom atomToPropagate, TraversalPlanFactory traversalPlanFactory, TraversalExecutor traversalExecutor, ReasoningContext ctx) {
        this(Collections.singletonList(atomToPropagate), traversalPlanFactory, traversalExecutor, ctx);
    }

    ReasonerAtomicQuery(Set<Atomic> atomsToCopy, TraversalPlanFactory traversalPlanFactory, TraversalExecutor traversalExecutor, ReasoningContext ctx) {
        super(atomsToCopy, traversalPlanFactory, traversalExecutor, ctx);
        this.atom = Iterables.getOnlyElement(selectAtoms()::iterator);
    }

    @Override
    public ReasonerAtomicQuery copy(){ return new ReasonerAtomicQuery(this);}

    @Override
    public ReasonerAtomicQuery withSubstitution(ConceptMap sub){
        Set<Atomic> union = Sets.union(getAtoms(), AtomicUtil.answerToPredicates(sub, this));
        return new ReasonerAtomicQuery(union,  traversalPlanFactory, traversalExecutor, context());
    }

    @Override
    public ReasonerAtomicQuery inferTypes() {
        return new ReasonerAtomicQuery(getAtoms().stream().map(Atomic::inferTypes).collect(Collectors.toSet()), traversalPlanFactory, traversalExecutor, context());
    }

    @Override
    public String toString(){ return getAtoms(Atom.class).map(Atomic::toString).collect(Collectors.joining(", "));}

    @Override
    public boolean isAtomic(){ return true;}

    public boolean hasUniqueAnswer(){ return getAtom().hasUniqueAnswer();}

    /**
     * @param parent query to compare with
     * @return true if this query is subsumed by the provided query
     */
    public boolean isSubsumedBy(ReasonerAtomicQuery parent){
        return isSubsumedBy(parent, UnifierType.SUBSUMPTIVE);
    }

    /**
     * @param parent query to compare with
     * @return true if this query is structurally subsumed by the provided query
     */
    public boolean isSubsumedStructurallyBy(ReasonerAtomicQuery parent){
        return isSubsumedBy(parent, UnifierType.STRUCTURAL_SUBSUMPTIVE);
    }

    /**
     * Determines whether the subsumption relation between this (C) and provided query (P) holds,
     * i. e. determines if:
     *
     * C <= P
     *
     * is true meaning that P is more general than C and their respective answer sets meet:
     *
     * answers(C) subsetOf answers(P)
     *
     * i. e. the set of answers of C is a subset of the set of answers of P
     *
     * @param parent query to compare with
     * @param unifierType unifier type specifying subsumption type
     * @return true if this query is subsumed by the provided query
     */
    private boolean isSubsumedBy(ReasonerAtomicQuery parent, UnifierType unifierType){
        MultiUnifier multiUnifier = this.getMultiUnifier(parent, unifierType);
        if (multiUnifier.isEmpty()) return false;
        MultiUnifier inverse = multiUnifier.inverse();

        //check whether propagated answers would be complete
        boolean propagatedAnswersComplete = !inverse.isEmpty() &&
                inverse.stream().allMatch(u -> u.values().containsAll(this.getVarNames()));
        return propagatedAnswersComplete
                && !parent.getAtoms(VariablePredicate.class).findFirst().isPresent()
                && !this.getAtoms(VariablePredicate.class).findFirst().isPresent();
    }

    /**
     * @return the atom constituting this atomic query
     */
    public Atom getAtom() {
        return atom;
    }

    /**
     * @throws IllegalArgumentException if passed a ReasonerQuery that is not a ReasonerAtomicQuery.
     */
    @Override
    public MultiUnifier getMultiUnifier(ReasonerQuery p, UnifierType unifierType){
        if (p == this) return MultiUnifierImpl.trivial();
        Preconditions.checkArgument(p instanceof ReasonerAtomicQuery);

        //NB: this is a defensive check and potentially expensive
        if (unifierType.equivalence() != null && !unifierType.equivalence().equivalent(p, this)) return MultiUnifierImpl.nonExistent();

        ReasonerAtomicQuery parent = (ReasonerAtomicQuery) p;
        MultiUnifier multiUnifier = this.getAtom().getMultiUnifier(parent.getAtom(), unifierType);

        Set<TypeAtom> childTypes = this.getAtom().getTypeConstraints().collect(Collectors.toSet());
        if (multiUnifier.isEmpty() || childTypes.isEmpty()) return multiUnifier;

        //get corresponding type unifiers
        Set<TypeAtom> parentTypes = parent.getAtom().getTypeConstraints().collect(Collectors.toSet());

        Set<Unifier> unifiers = multiUnifier.unifiers().stream()
                .map(unifier -> ReasonerUtils.typeUnifier(childTypes, parentTypes, unifier, unifierType))
                .collect(Collectors.toSet());
        return new MultiUnifierImpl(unifiers);
    }

    /**
     * Calculates:
     * - unifier between this (parent, source) and child (target) query
     * - semantic difference between this (parent, source) and child (target) query
     * - child is more specific than parent ( child <= parent)
     * @param child query
     * @return pair of: a parent->child unifier and a parent->child semantic difference between
     */
    public Set<Pair<Unifier, SemanticDifference>> getMultiUnifierWithSemanticDiff(ReasonerAtomicQuery child){
        MultiUnifier unifier = child.getMultiUnifier(this, UnifierType.STRUCTURAL_SUBSUMPTIVE);
        return unifier.stream()
                .map(childParentUnifier -> {
                    Unifier inverse = childParentUnifier.inverse();
                    return new Pair<>(inverse, this.getAtom().computeSemanticDifference(child.getAtom(), inverse));
                })
                .collect(Collectors.toSet());
    }

    /**
     * materialise  this query with the accompanying answer - persist to kb
     * @param answer to be materialised
     * @return stream of materialised answers
     */
    public Stream<ConceptMap> materialise(ConceptMap answer) {
        return this.withSubstitution(answer)
                .getAtom()
                .materialise()
                .map(ans -> ans.explain(answer.explanation(), this.getPattern()));
    }

    @Override
    public ResolutionState resolutionState(ConceptMap sub, Unifier u, AnswerPropagatorState parent, Set<ReasonerAtomicQuery> subGoals){
        if (getAtom().getSchemaConcept() == null) return new AtomicStateProducer(this, sub, u, parent, subGoals);
        return !containsVariablePredicates()?
                new AtomicState(this, sub, u, parent, subGoals, context()) :
                new VariableComparisonState(this, sub, u, parent, subGoals);
    }

    @Override
    protected Stream<ReasonerQueryImpl> getQueryStream(ConceptMap sub){
        return getAtom().atomOptions(sub).stream().map(atom -> new ReasonerAtomicQuery(atom, traversalPlanFactory, traversalExecutor, context()));
    }

    @Override
    public Iterator<ResolutionState> innerStateIterator(AnswerPropagatorState parent, Set<ReasonerAtomicQuery> visitedSubGoals) {
        QueryCache queryCache = context().queryCache();
        Pair<Stream<ConceptMap>, MultiUnifier> cacheEntry = CacheCasting.queryCacheCast(queryCache).getAnswerStreamWithUnifier(this);
        Iterator<AnswerState> dbIterator = cacheEntry.first()
                .map(a -> a.explain(a.explanation(), this.getPattern()))
                .map(ans -> new AnswerState(ans, parent.getUnifier(), parent))
                .iterator();

        Iterator<ResolutionState> dbCompletionIterator =
                Iterators.singletonIterator(new CacheCompletionState(queryCache, this, new ConceptMap(), null));

        boolean visited = visitedSubGoals.contains(this);
        //if this is ground and exists in the db then do not resolve further
        boolean doNotResolveFurther = visited
                || CacheCasting.queryCacheCast(queryCache).isComplete(this)
                || (this.isGround() && dbIterator.hasNext());
        Iterator<ResolutionState> subGoalIterator = !doNotResolveFurther?
                ruleStateIterator(parent, visitedSubGoals) :
                Collections.emptyIterator();

        if (!visited) visitedSubGoals.add(this);
        return Iterators.concat(dbIterator, dbCompletionIterator, subGoalIterator);
    }

    /**
     * Constructs an iterator of RuleStates this query can generate - rule states correspond to rules that can be applied to this query.
     * NB: we need this iterator to be fully lazy, hence we need to ensure that the base stream doesn't have any stateful ops.
     * @param parent parent state
     * @param visitedSubGoals set of visited sub goals
     * @return ruleState iterator corresponding to rules that are matchable with this query
     */
    private Iterator<ResolutionState> ruleStateIterator(AnswerPropagatorState parent, Set<ReasonerAtomicQuery> visitedSubGoals) {
        return RuleUtils
                .stratifyRules(getAtom().getApplicableRules().collect(Collectors.toSet()))
                .flatMap(r -> r.getMultiUnifier(getAtom()).stream().map(unifier -> new Pair<>(r, unifier)))
                .map(rulePair -> rulePair.first().subGoal(this.getAtom(), rulePair.second(), parent, visitedSubGoals))
                .iterator();
    }
}
