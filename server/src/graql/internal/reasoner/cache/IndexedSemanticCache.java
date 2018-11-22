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

package grakn.core.graql.internal.reasoner.cache;

import com.google.common.base.Equivalence;
import grakn.core.graql.Var;
import grakn.core.graql.admin.MultiUnifier;
import grakn.core.graql.admin.Unifier;
import grakn.core.graql.answer.ConceptMap;
import grakn.core.graql.internal.reasoner.query.ReasonerAtomicQuery;
import grakn.core.graql.internal.reasoner.unifier.UnifierType;
import grakn.core.graql.internal.reasoner.utils.Pair;
import grakn.core.server.exception.GraqlQueryException;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.stream.Collectors.toSet;

/**
 *
 * Implementation of {@link SemanticCache} using {@link grakn.core.graql.internal.reasoner.query.ReasonerQueryEquivalence#StructuralEquivalence}
 * for query equivalence checks and {@link IndexedAnswerSet}s for storing query answer sets.
 *
 */
public class IndexedSemanticCache extends SemanticCache<Equivalence.Wrapper<ReasonerAtomicQuery>, IndexedAnswerSet> {

    private static final Logger LOG = LoggerFactory.getLogger(IndexedSemanticCache.class);

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
    protected void propagateAnswers(ReasonerAtomicQuery target,
                                    CacheEntry<ReasonerAtomicQuery, IndexedAnswerSet> parentEntry,
                                    CacheEntry<ReasonerAtomicQuery, IndexedAnswerSet> childEntry,
                                    boolean inferred) {
        ReasonerAtomicQuery parent = parentEntry.query();
        ReasonerAtomicQuery child = childEntry.query();
        IndexedAnswerSet parentAnswers = parentEntry.cachedElement();
        IndexedAnswerSet childAnswers = childEntry.cachedElement();
        ConceptMap baseAnswerIndex = target.getAnswerIndex();

        LOG.trace("Propagating answers \nfrom: " + parent + "\nto: " + target);

        MultiUnifier targetToParentUnifier = target.getMultiUnifier(parent, semanticUnifier());
        Set<Pair<Unifier, SemanticDifference>> parentToChildUnifierDelta =
                child.getMultiUnifierWithSemanticDiff(parent).stream()
                .map(unifierDelta -> new Pair<>(unifierDelta.getKey().inverse(), unifierDelta.getValue()))
                .collect(toSet());

        Set<ConceptMap> parentAnswersToPropagate = baseAnswerIndex.unify(targetToParentUnifier)
                .flatMap(sub ->
                        parentAnswers.getAll().stream()
                                .filter(ans -> inferred || ans.explanation().isLookupExplanation()
                                ))
                .collect(toSet());

        Set<ConceptMap> newAnswers = new HashSet<>();

        /*
         * propagate answers to child:
         * * calculate constraint difference
         * * apply constraints from the difference
         */
        Set<Var> childVars = child.getVarNames();
        parentToChildUnifierDelta.stream()
                .flatMap(unifierDelta -> parentAnswersToPropagate.stream()
                        .map(ans -> ans.unify(unifierDelta.getKey()))
                        .filter(ans -> unifierDelta.getValue().satisfiedBy(ans)))
                .filter(ans -> !ans.isEmpty())
                .map(ans -> ans.merge(child.getRoleSubstitution()))
                .map(ans -> ans.project(childVars))
                .peek(ans -> {
                    if (!ans.vars().containsAll(childVars)){
                        throw GraqlQueryException.invalidQueryCacheEntry(child, ans);
                    }
                })
                .filter(childAnswers::add)
                .forEach(newAnswers::add);

        LOG.trace(newAnswers.size() + " new answers propagated: " + newAnswers);
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
                answerIndex.unify(multiUnifier.inverse())
                        .flatMap(index -> answers.get(index).stream())
                        .flatMap(ans -> ans.unify(multiUnifier)),
                multiUnifier
        );
    }
}

