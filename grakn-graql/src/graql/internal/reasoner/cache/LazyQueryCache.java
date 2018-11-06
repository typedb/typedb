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

package ai.grakn.graql.internal.reasoner.cache;

import ai.grakn.graql.answer.ConceptMap;
import ai.grakn.graql.admin.MultiUnifier;
import ai.grakn.graql.internal.reasoner.unifier.MultiUnifierImpl;
import ai.grakn.graql.internal.reasoner.explanation.LookupExplanation;
import ai.grakn.graql.internal.reasoner.iterator.LazyAnswerIterator;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueryImpl;
import ai.grakn.graql.internal.reasoner.utils.Pair;

import java.util.stream.Stream;

/**
 *
 * <p>
 * Lazy container class for storing performed query resolutions.
 * NB: For the GET operation, in the case the entry is not found, a RECORD operation is performed.
 * </p>
 *
 * @param <Q> the type of query that is being cached
 *
 * @author Kasper Piskorski
 *
 */
public class LazyQueryCache<Q extends ReasonerQueryImpl> extends QueryCacheBase<Q, LazyAnswerIterator> {

    public LazyQueryCache(){ super();}

    @Override
    public ConceptMap record(Q query, ConceptMap answer) {
        record(query, Stream.of(answer));
        return answer;
    }

    @Override
    public LazyAnswerIterator record(Q query, LazyAnswerIterator answers) {
        CacheEntry<Q, LazyAnswerIterator> match =  this.getEntry(query);
        if (match != null) {
            Q equivalentQuery = match.query();
            Stream<ConceptMap> unifiedStream = answers.unify(query.getMultiUnifier(equivalentQuery)).stream();
            //NB: entry overwrite
            this.putEntry(match.query(), match.cachedElement().merge(unifiedStream));
            return getAnswers(query);
        }
        this.putEntry(query, answers);
        return answers;
    }

    @Override
    public Stream<ConceptMap> record(Q query, Stream<ConceptMap> answers) {
        return recordRetrieveLazy(query, answers).stream();
    }

    /**
     * record answer stream for a specific query and retrieve the updated stream in a lazy iterator
     * @param query to be recorded
     * @param answers answer stream of the query
     * @return lazy iterator of updated answers
     */
    public LazyAnswerIterator recordRetrieveLazy(Q query, Stream<ConceptMap> answers){
        CacheEntry<Q, LazyAnswerIterator> match =  this.getEntry(query);
        if (match!= null) {
            Q equivalentQuery = match.query();
            MultiUnifier multiUnifier = query.getMultiUnifier(equivalentQuery);
            Stream<ConceptMap> unifiedStream = answers.flatMap(a -> a.unify(multiUnifier));
            //NB: entry overwrite
            this.putEntry(match.query(), match.cachedElement().merge(unifiedStream));
            return getAnswers(query);
        }
        LazyAnswerIterator liter = new LazyAnswerIterator(answers);
        this.putEntry(query, liter);
        return liter;
    }

    @Override
    public LazyAnswerIterator getAnswers(Q query) {
        return getAnswersWithUnifier(query).getKey();
    }

    @Override
    public Stream<ConceptMap> getAnswerStream(Q query){
        return getAnswerStreamWithUnifier(query).getKey();
    }

    @Override
    public Pair<LazyAnswerIterator, MultiUnifier> getAnswersWithUnifier(Q query) {
        CacheEntry<Q, LazyAnswerIterator> match =  this.getEntry(query);
        if (match != null) {
            Q equivalentQuery = match.query();
            MultiUnifier multiUnifier = equivalentQuery.getMultiUnifier(query);
            LazyAnswerIterator unified = match.cachedElement().unify(multiUnifier);
            return new Pair<>(unified, multiUnifier);
        }
        Stream<ConceptMap> answerStream = record(
                query,
                query.getQuery().stream().map(a -> a.explain(new LookupExplanation(query)))
        );
        return new Pair<>(new LazyAnswerIterator(answerStream), new MultiUnifierImpl());
    }

    @Override
    public Pair<Stream<ConceptMap>, MultiUnifier> getAnswerStreamWithUnifier(Q query) {
        CacheEntry<Q, LazyAnswerIterator> match =  this.getEntry(query);
        if (match != null) {
            Q equivalentQuery = match.query();
            MultiUnifier multiUnifier = equivalentQuery.getMultiUnifier(query);
            Stream<ConceptMap> unified = match.cachedElement().stream().flatMap(a -> a.unify(multiUnifier));
            return new Pair<>(unified, multiUnifier);
        }

        Stream<ConceptMap> answerStream = record(
                query,
                query.getQuery().stream().map(a -> a.explain(new LookupExplanation(query)))
        );
        return new Pair<>(answerStream, new MultiUnifierImpl());
    }

}
