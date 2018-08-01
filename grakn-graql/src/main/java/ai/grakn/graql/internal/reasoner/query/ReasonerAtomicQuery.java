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
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.graql.internal.reasoner.query;

import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.answer.ConceptMap;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.MultiUnifier;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.admin.UnifierComparison;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.reasoner.MultiUnifierImpl;
import ai.grakn.graql.internal.reasoner.UnifierImpl;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.atom.binary.TypeAtom;
import ai.grakn.graql.internal.reasoner.atom.predicate.NeqPredicate;
import ai.grakn.graql.internal.reasoner.cache.QueryCache;
import ai.grakn.graql.internal.reasoner.rule.InferenceRule;
import ai.grakn.graql.internal.reasoner.state.AnswerState;
import ai.grakn.graql.internal.reasoner.state.AtomicStateProducer;
import ai.grakn.graql.internal.reasoner.state.QueryStateBase;
import ai.grakn.graql.internal.reasoner.state.ResolutionState;
import ai.grakn.graql.internal.reasoner.utils.Pair;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ai.grakn.graql.internal.reasoner.utils.ReasonerUtils.typeUnifier;

/**
 *
 * <p>
 * Base reasoner atomic query. An atomic query is a query constrained to having at most one rule-resolvable atom
 * together with its accompanying constraints (predicates and types).
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
@SuppressFBWarnings("EQ_DOESNT_OVERRIDE_EQUALS")
public class ReasonerAtomicQuery extends ReasonerQueryImpl {

    private final Atom atom;

    ReasonerAtomicQuery(Conjunction<VarPatternAdmin> pattern, EmbeddedGraknTx<?> tx) {
        super(pattern, tx);
        atom = selectAtoms().stream().findFirst().orElse(null);
    }

    ReasonerAtomicQuery(ReasonerQueryImpl query) {
        super(query);
        atom = selectAtoms().stream().findFirst().orElse(null);
    }

    ReasonerAtomicQuery(Atom at) {
        super(at);
        atom = selectAtoms().stream().findFirst().orElse(null);
    }

    ReasonerAtomicQuery(Set<Atomic> atoms, EmbeddedGraknTx<?> tx){
        super(atoms, tx);
        atom = selectAtoms().stream().findFirst().orElse(null);
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
     * @return the atom constituting this atomic query
     */
    public Atom getAtom() {
        return atom;
    }

    @Override
    public Set<Atom> selectAtoms() {
        Set<Atom> selectedAtoms = super.selectAtoms();
        if (selectedAtoms.size() != 1) {
            throw GraqlQueryException.nonAtomicQuery(this);
        }
        return selectedAtoms;
    }

    /**
     * @throws IllegalArgumentException if passed a {@link ReasonerQuery} that is not a {@link ReasonerAtomicQuery}.
     */
    @Override
    public MultiUnifier getMultiUnifier(ReasonerQuery p, UnifierComparison unifierType){
        if (p == this) return new MultiUnifierImpl();
        Preconditions.checkArgument(p instanceof ReasonerAtomicQuery);
        ReasonerAtomicQuery parent = (ReasonerAtomicQuery) p;
        MultiUnifier multiUnifier = this.getAtom().getMultiUnifier(parent.getAtom(), unifierType);

        Set<TypeAtom> childTypes = this.getAtom().getTypeConstraints().collect(Collectors.toSet());
        if (childTypes.isEmpty()) return multiUnifier;

        //get corresponding type unifiers
        Set<TypeAtom> parentTypes = parent.getAtom().getTypeConstraints().collect(Collectors.toSet());
        if (multiUnifier.isEmpty()) return new MultiUnifierImpl(typeUnifier(childTypes, parentTypes, new UnifierImpl()));

        Set<Unifier> unifiers = multiUnifier.unifiers().stream()
                .map(unifier -> typeUnifier(childTypes, parentTypes, unifier))
                .collect(Collectors.toSet());
        return new MultiUnifierImpl(unifiers);
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
    public ResolutionState subGoal(ConceptMap sub, Unifier u, QueryStateBase parent, Set<ReasonerAtomicQuery> subGoals, QueryCache<ReasonerAtomicQuery> cache){
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
    public Iterator<ResolutionState> queryStateIterator(QueryStateBase parent, Set<ReasonerAtomicQuery> visitedSubGoals, QueryCache<ReasonerAtomicQuery> cache) {
        Pair<Stream<ConceptMap>, MultiUnifier> cacheEntry = cache.getAnswerStreamWithUnifier(this);
        Iterator<AnswerState> dbIterator = cacheEntry.getKey()
                .map(a -> a.explain(a.explanation().setQuery(this)))
                .map(ans -> new AnswerState(ans, parent.getUnifier(), parent))
                .iterator();

        Iterator<ResolutionState> subGoalIterator;
        //if this is ground and exists in the db then do not resolve further
        if(visitedSubGoals.contains(this)
                || (this.isGround() && dbIterator.hasNext())){
            subGoalIterator = Collections.emptyIterator();
        } else {
            visitedSubGoals.add(this);
            subGoalIterator = this.getRuleStream()
                    .map(rulePair -> rulePair.getKey().subGoal(this.getAtom(), rulePair.getValue(), parent, visitedSubGoals, cache))
                    .iterator();
        }
        return Iterators.concat(dbIterator, subGoalIterator);
    }

    /**
     * @return stream of all rules applicable to this atomic query including permuted cases when the role types are meta roles
     */
    private Stream<Pair<InferenceRule, Unifier>> getRuleStream(){
        return getAtom().getApplicableRules()
                .flatMap(r -> r.getMultiUnifier(getAtom()).stream().map(unifier -> new Pair<>(r, unifier)))
                .sorted(Comparator.comparing(rt -> -rt.getKey().resolutionPriority()));
    }

}
