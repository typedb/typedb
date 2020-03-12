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

package grakn.core.graql.reasoner.cache;

import com.google.common.base.Equivalence;
import com.google.common.base.Preconditions;
import grakn.common.util.Pair;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.graql.reasoner.query.ReasonerAtomicQuery;
import grakn.core.graql.reasoner.unifier.UnifierType;
import grakn.core.kb.graql.executor.ExecutorFactory;
import grakn.core.kb.graql.executor.TraversalExecutor;
import grakn.core.kb.graql.planning.gremlin.TraversalPlanFactory;
import grakn.core.kb.graql.reasoner.cache.CacheEntry;
import grakn.core.kb.graql.reasoner.unifier.MultiUnifier;
import grakn.core.kb.graql.reasoner.unifier.Unifier;
import graql.lang.statement.Variable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

/**
 *
 * Implementation of SemanticCache using ReasonerQueryEquivalence#StructuralEquivalence
 * for query equivalence checks and IndexedAnswerSets for storing query answer sets.
 *
 */
public class MultilevelSemanticCache extends SemanticCache<Equivalence.Wrapper<ReasonerAtomicQuery>, IndexedAnswerSet> {

    private static final Logger LOG = LoggerFactory.getLogger(MultilevelSemanticCache.class);

    public MultilevelSemanticCache(TraversalPlanFactory traversalPlanFactory, TraversalExecutor traversalExecutor) {
        super(traversalPlanFactory, traversalExecutor);
    }

    @Override public UnifierType unifierType() { return UnifierType.STRUCTURAL;}

    @Override
    CacheEntry<ReasonerAtomicQuery, IndexedAnswerSet> createEntry(ReasonerAtomicQuery query, Set<ConceptMap> answers) {
        IndexedAnswerSet answerSet = answers.isEmpty() ?
                IndexedAnswerSet.create(query.index()) :
                IndexedAnswerSet.create(answers, query.index());
        return new CacheEntry<>(query, answerSet);
    }

    @Override
    Equivalence.Wrapper<ReasonerAtomicQuery> queryToKey(ReasonerAtomicQuery query) {
        return unifierType().equivalence().wrap(query);
    }

    @Override
    ReasonerAtomicQuery keyToQuery(Equivalence.Wrapper<ReasonerAtomicQuery> key) {
        return key.get();
    }

    @Override
    boolean propagateAnswers(CacheEntry<ReasonerAtomicQuery, IndexedAnswerSet> parentEntry,
                                    CacheEntry<ReasonerAtomicQuery, IndexedAnswerSet> childEntry,
                                    boolean propagateInferred) {
        ReasonerAtomicQuery parent = parentEntry.query();
        ReasonerAtomicQuery child = childEntry.query();
        IndexedAnswerSet parentAnswers = parentEntry.cachedElement();
        IndexedAnswerSet childAnswers = childEntry.cachedElement();

        /*
         * propagate answers to child:
         * * calculate constraint difference
         * * apply constraints from the difference
         */
        Set<Pair<Unifier, SemanticDifference>> parentToChildUnifierDelta = parent.getMultiUnifierWithSemanticDiff(child);
        Set<Variable> childVars = child.getVarNames();
        ConceptMap childPartialSub = child.getRoleSubstitution();
        Set<ConceptMap> newAnswers = new HashSet<>();
        
        parentAnswers.getAll().stream()
                .filter(parentAns -> propagateInferred || parentAns.explanation().isLookupExplanation())
                .flatMap(parentAns -> parentToChildUnifierDelta.stream()
                        .map(unifierDelta -> unifierDelta.second().propagateAnswer(parentAns, childPartialSub, childVars, unifierDelta.first()))
                )
                .filter(ans -> !ans.isEmpty())
                .peek(ans -> validateAnswer(ans, child, childVars))
                .filter(childAnswers::add)
                .forEach(newAnswers::add);

        LOG.trace("Parent {} answers propagated to child {}: {}", parent, child, newAnswers);

        return !newAnswers.isEmpty();
    }

    @Override
    Pair<Stream<ConceptMap>, MultiUnifier> entryToAnswerStreamWithUnifier(ReasonerAtomicQuery query, CacheEntry<ReasonerAtomicQuery, IndexedAnswerSet> entry) {
        ConceptMap answerIndex = query.getAnswerIndex();
        ReasonerAtomicQuery equivalentQuery = entry.query();
        AnswerSet answers = entry.cachedElement();
        MultiUnifier multiUnifier = equivalentQuery.getMultiUnifier(query, unifierType());
        Preconditions.checkState(!multiUnifier.isEmpty());

        return new Pair<>(
                multiUnifier.inverse()
                        .apply(answerIndex)
                        .flatMap(index -> answers.get(index).stream())
                        .flatMap(multiUnifier::apply)
                .map(ans -> ans.withPattern(query.getPattern())),
                multiUnifier
        );
    }

    @Override
    public boolean answersQuery(ReasonerAtomicQuery query) {
        CacheEntry<ReasonerAtomicQuery, IndexedAnswerSet> entry = getEntry(query);
        if (entry == null) return false;
        ReasonerAtomicQuery cacheQuery = entry.query();
        IndexedAnswerSet answerSet = entry.cachedElement();
        Set<Variable> cacheIndex = cacheQuery.getAnswerIndex().vars();
        MultiUnifier queryToCacheUnifier = query.getMultiUnifier(cacheQuery, unifierType());

        return queryToCacheUnifier.apply(query.getAnswerIndex())
                .anyMatch(sub ->
                        answerSet.get(sub.project(cacheIndex)).stream()
                                .anyMatch(ans -> ans.containsAll(sub)));
    }
}

