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

package grakn.core.graql.reasoner.cache;

import com.google.common.base.Equivalence;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.graql.exception.GraqlQueryException;
import grakn.core.graql.reasoner.query.ReasonerAtomicQuery;
import grakn.core.graql.reasoner.query.ReasonerQueryEquivalence;
import grakn.core.graql.reasoner.unifier.MultiUnifier;
import grakn.core.graql.reasoner.unifier.Unifier;
import grakn.core.graql.reasoner.unifier.UnifierType;
import grakn.core.graql.reasoner.utils.Pair;
import graql.lang.statement.Variable;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toSet;

/**
 *
 * Implementation of {@link SemanticCache} using {@link ReasonerQueryEquivalence#StructuralEquivalence}
 * for query equivalence checks and {@link IndexedAnswerSet}s for storing query answer sets.
 *
 */
public class MultilevelSemanticCache extends SemanticCache<Equivalence.Wrapper<ReasonerAtomicQuery>, IndexedAnswerSet> {

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
    protected boolean answersQuery(ReasonerAtomicQuery query) {
        CacheEntry<ReasonerAtomicQuery, IndexedAnswerSet> entry = getEntry(query);
        if (entry == null) return false;
        ReasonerAtomicQuery cacheQuery = entry.query();
        IndexedAnswerSet answerSet = entry.cachedElement();
        Set<Variable> cacheIndex = cacheQuery.getAnswerIndex().vars();
        MultiUnifier queryToCacheUnifier = query.getMultiUnifier(cacheQuery, semanticUnifier());

        return queryToCacheUnifier.apply(query.getAnswerIndex())
                .anyMatch(sub ->
                        answerSet.get(sub.project(cacheIndex)).stream()
                                .anyMatch(ans -> ans.containsAll(sub)));
    }

    @Override
    protected boolean propagateAnswers(CacheEntry<ReasonerAtomicQuery, IndexedAnswerSet> parentEntry,
                                    CacheEntry<ReasonerAtomicQuery, IndexedAnswerSet> childEntry,
                                    boolean inferred) {
        ReasonerAtomicQuery parent = parentEntry.query();
        ReasonerAtomicQuery child = childEntry.query();
        IndexedAnswerSet parentAnswers = parentEntry.cachedElement();
        IndexedAnswerSet childAnswers = childEntry.cachedElement();

        Set<Pair<Unifier, SemanticDifference>> parentToChildUnifierDelta =
                child.getMultiUnifierWithSemanticDiff(parent).stream()
                .map(unifierDelta -> new Pair<>(unifierDelta.getKey().inverse(), unifierDelta.getValue()))
                .collect(toSet());

        /*
         * propagate answers to child:
         * * calculate constraint difference
         * * apply constraints from the difference
         */
        Set<Variable> childVars = child.getVarNames();
        ConceptMap partialSub = child.getRoleSubstitution();
        Set<ConceptMap> newAnswers = new HashSet<>();

        parentAnswers.getAll().stream()
                .filter(ans -> inferred || ans.explanation().isLookupExplanation())
                .flatMap(ans -> parentToChildUnifierDelta.stream()
                                .map(unifierDelta -> unifierDelta.getValue()
                                        .applyToAnswer(ans, partialSub, childVars, unifierDelta.getKey()))
                )
                .filter(ans -> !ans.isEmpty())
                .peek(ans -> {
                    if (!ans.vars().containsAll(childVars)){
                        throw GraqlQueryException.invalidQueryCacheEntry(child, ans);
                    }
                })
                .filter(childAnswers::add)
                .forEach(newAnswers::add);

        return !newAnswers.isEmpty();
    }

    @Override
    protected Stream<ConceptMap> entryToAnswerStream(CacheEntry<ReasonerAtomicQuery, IndexedAnswerSet> entry) {
        return entry.cachedElement().get(entry.query().getAnswerIndex()).stream();
    }

    @Override
    protected Pair<Stream<ConceptMap>, MultiUnifier> entryToAnswerStreamWithUnifier(ReasonerAtomicQuery query, CacheEntry<ReasonerAtomicQuery, IndexedAnswerSet> entry) {
        ConceptMap answerIndex = query.getAnswerIndex();
        ReasonerAtomicQuery equivalentQuery = entry.query();
        AnswerSet answers = entry.cachedElement();
        MultiUnifier multiUnifier = equivalentQuery.getMultiUnifier(query, unifierType());

        return new Pair<>(
                multiUnifier.inverse()
                        .apply(answerIndex)
                        .flatMap(index -> answers.get(index).stream())
                        .flatMap(multiUnifier::apply),
                multiUnifier
        );
    }
}

