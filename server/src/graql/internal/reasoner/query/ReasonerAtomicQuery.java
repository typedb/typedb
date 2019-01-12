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

package grakn.core.graql.internal.reasoner.query;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import grakn.core.graql.admin.Atomic;
import grakn.core.graql.admin.MultiUnifier;
import grakn.core.graql.admin.ReasonerQuery;
import grakn.core.graql.admin.Unifier;
import grakn.core.graql.answer.ConceptMap;
import grakn.core.graql.internal.reasoner.atom.Atom;
import grakn.core.graql.internal.reasoner.atom.binary.TypeAtom;
import grakn.core.graql.internal.reasoner.atom.predicate.NeqPredicate;
import grakn.core.graql.internal.reasoner.cache.MultilevelSemanticCache;
import grakn.core.graql.internal.reasoner.cache.SemanticDifference;
import grakn.core.graql.internal.reasoner.state.AnswerState;
import grakn.core.graql.internal.reasoner.state.AtomicStateProducer;
import grakn.core.graql.internal.reasoner.state.CacheCompletionState;
import grakn.core.graql.internal.reasoner.state.QueryStateBase;
import grakn.core.graql.internal.reasoner.state.ResolutionState;
import grakn.core.graql.internal.reasoner.unifier.MultiUnifierImpl;
import grakn.core.graql.internal.reasoner.unifier.UnifierType;
import grakn.core.graql.internal.reasoner.utils.Pair;
import grakn.core.graql.query.pattern.Conjunction;
import grakn.core.graql.query.pattern.statement.Statement;
import grakn.core.server.session.TransactionOLTP;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static grakn.core.graql.internal.reasoner.utils.ReasonerUtils.typeUnifier;

/**
 *
 * <p>
 * Base reasoner atomic query. An atomic query is a query constrained to having at most one rule-resolvable atom
 * together with its accompanying constraints (predicates and types).
 * </p>
 *
 *
 */
@SuppressFBWarnings("EQ_DOESNT_OVERRIDE_EQUALS")
public class ReasonerAtomicQuery extends ReasonerQueryImpl {

    private final Atom atom;

    ReasonerAtomicQuery(Conjunction<Statement> pattern, TransactionOLTP tx) {
        super(pattern, tx);
        this.atom = Iterables.getOnlyElement(selectAtoms()::iterator);
    }

    ReasonerAtomicQuery(ReasonerQueryImpl query) {
        super(query);
        this.atom = Iterables.getOnlyElement(selectAtoms()::iterator);
    }

    ReasonerAtomicQuery(Atom at) {
        super(at);
        this.atom = Iterables.getOnlyElement(selectAtoms()::iterator);
    }

    ReasonerAtomicQuery(Set<Atomic> atoms, TransactionOLTP tx){
        super(atoms, tx);
        this.atom = Iterables.getOnlyElement(selectAtoms()::iterator);
    }

    @Override
    public ReasonerQuery copy(){ return new ReasonerAtomicQuery(this);}

    @Override
    public ReasonerAtomicQuery withSubstitution(ConceptMap sub){
        return new ReasonerAtomicQuery(Sets.union(this.getAtoms(), sub.toPredicates(this)), this.tx());
    }

    @Override
    public ReasonerAtomicQuery inferTypes() {
        return new ReasonerAtomicQuery(getAtoms().stream().map(Atomic::inferTypes).collect(Collectors.toSet()), tx());
    }

    @Override
    public ReasonerAtomicQuery positive(){
        return new ReasonerAtomicQuery(
                getAtoms().stream()
                        .filter(at -> !(at instanceof NeqPredicate))
                        .filter(at -> !Sets.intersection(at.getVarNames(), getAtom().getVarNames()).isEmpty())
                        .collect(Collectors.toSet()),
                tx());
    }

    @Override
    public String toString(){
        return getAtoms(Atom.class).map(Atomic::toString).collect(Collectors.joining(", "));
    }

    @Override
    public boolean isAtomic(){ return true;}

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
     * @return true if this query subsumes the provided query
     */
    public boolean subsumes(ReasonerAtomicQuery parent){
        MultiUnifier multiUnifier = this.getMultiUnifier(parent, UnifierType.SUBSUMPTIVE);
        if (multiUnifier.isEmpty()) return false;
        MultiUnifier inverse = multiUnifier.inverse();
        return//check whether propagated answers would be complete
                !inverse.isEmpty() &&
                        inverse.stream().allMatch(u -> u.values().containsAll(this.getVarNames()))
                        && !parent.getAtoms(NeqPredicate.class).findFirst().isPresent()
                        && !this.getAtoms(NeqPredicate.class).findFirst().isPresent();
    }

    /**
     * @return the atom constituting this atomic query
     */
    public Atom getAtom() {
        return atom;
    }

    /**
     * @throws IllegalArgumentException if passed a {@link ReasonerQuery} that is not a {@link ReasonerAtomicQuery}.
     */
    @Override
    public MultiUnifier getMultiUnifier(ReasonerQuery p, UnifierType unifierType){
        if (p == this) return MultiUnifierImpl.trivial();
        Preconditions.checkArgument(p instanceof ReasonerAtomicQuery);
        if (unifierType.equivalence() != null && !unifierType.equivalence().equivalent(p, this)) return MultiUnifierImpl.nonExistent();

        ReasonerAtomicQuery parent = (ReasonerAtomicQuery) p;
        MultiUnifier multiUnifier = this.getAtom().getMultiUnifier(parent.getAtom(), unifierType);

        Set<TypeAtom> childTypes = this.getAtom().getTypeConstraints().collect(Collectors.toSet());
        if (multiUnifier.isEmpty() || childTypes.isEmpty()) return multiUnifier;

        //get corresponding type unifiers
        Set<TypeAtom> parentTypes = parent.getAtom().getTypeConstraints().collect(Collectors.toSet());

        Set<Unifier> unifiers = multiUnifier.unifiers().stream()
                .map(unifier -> typeUnifier(childTypes, parentTypes, unifier, unifierType))
                .collect(Collectors.toSet());
        return new MultiUnifierImpl(unifiers);
    }

    /**
     * Calculates:
     * - subsumptive unifier between this (child) and parent query
     * - semantic difference between this (child and parent query
     * @param parent parent query
     * @return pair of subsumptive unifier and semantic difference between this and parent query
     */
    public Set<Pair<Unifier, SemanticDifference>> getMultiUnifierWithSemanticDiff(ReasonerAtomicQuery parent){
        return this.getMultiUnifier(parent, UnifierType.SUBSUMPTIVE).stream()
                .map(u -> new Pair<>(u, this.getAtom().semanticDifference(parent.getAtom(), u)))
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
                .map(ans -> ans.explain(answer.explanation()));
    }

    @Override
    public ResolutionState subGoal(ConceptMap sub, Unifier u, QueryStateBase parent, Set<ReasonerAtomicQuery> subGoals, MultilevelSemanticCache cache){
        return new AtomicStateProducer(this, sub, u, parent, subGoals, cache);
    }

    @Override
    protected Stream<ReasonerQueryImpl> getQueryStream(ConceptMap sub){
        Atom atom = getAtom();
        return atom.getSchemaConcept() == null?
                atom.atomOptions(sub).stream().map(ReasonerAtomicQuery::new) :
                Stream.of(this);
    }

    @Override
    public Iterator<ResolutionState> queryStateIterator(QueryStateBase parent, Set<ReasonerAtomicQuery> visitedSubGoals, MultilevelSemanticCache cache) {
        Pair<Stream<ConceptMap>, MultiUnifier> cacheEntry = cache.getAnswerStreamWithUnifier(this);
        Iterator<AnswerState> dbIterator = cacheEntry.getKey()
                .map(a -> a.explain(a.explanation().setQuery(this)))
                .map(ans -> new AnswerState(ans, parent.getUnifier(), parent))
                .iterator();

        Iterator<ResolutionState> dbCompletionIterator =
                Iterators.singletonIterator(new CacheCompletionState(this, new ConceptMap(), null, cache));

        Iterator<ResolutionState> subGoalIterator;
        //if this is ground and exists in the db then do not resolve further
        if(visitedSubGoals.contains(this)
                || (this.isGround() && dbIterator.hasNext())){
            subGoalIterator = Collections.emptyIterator();
        } else {
            visitedSubGoals.add(this);
            subGoalIterator = cache.ruleCache().getRuleStream(this.getAtom())
                    .map(rulePair -> rulePair.getKey().subGoal(this.getAtom(), rulePair.getValue(), parent, visitedSubGoals, cache))
                    .iterator();
        }
        return Iterators.concat(dbIterator, dbCompletionIterator, subGoalIterator);
    }
}
