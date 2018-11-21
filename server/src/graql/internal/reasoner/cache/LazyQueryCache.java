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

import grakn.core.graql.admin.MultiUnifier;
import grakn.core.graql.answer.ConceptMap;
import grakn.core.graql.internal.reasoner.explanation.LookupExplanation;
import grakn.core.graql.internal.reasoner.iterator.LazyAnswerIterator;
import grakn.core.graql.internal.reasoner.query.ReasonerQueryImpl;
import grakn.core.graql.internal.reasoner.unifier.MultiUnifierImpl;
import grakn.core.graql.internal.reasoner.utils.Pair;
import java.util.stream.Stream;
import javax.annotation.Nullable;

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
public class LazyQueryCache<Q extends ReasonerQueryImpl> extends SimpleQueryCacheBase<Q, LazyAnswerIterator> {

    public LazyQueryCache(){ super();}

    @Override
    public CacheEntry<Q, LazyAnswerIterator> record(Q query, ConceptMap answer) {
        return record(query, Stream.of(answer));
    }

    @Override
    public CacheEntry<Q, LazyAnswerIterator> record(Q query, LazyAnswerIterator answers) {
        CacheEntry<Q, LazyAnswerIterator> match =  this.getEntry(query);
        if (match != null) {
            Q equivalentQuery = match.query();
            Stream<ConceptMap> unifiedStream = answers.unify(query.getMultiUnifier(equivalentQuery, unifierType())).stream();
            //NB: entry overwrite
            this.putEntry(match.query(), match.cachedElement().merge(unifiedStream));
            return match;
        }
        return putEntry(query, answers);
    }

    @Override
    public CacheEntry<Q, LazyAnswerIterator> record(Q query, Stream<ConceptMap> answers) {
        return recordRetrieveLazy(query, answers);
    }

    /**
     * record answer stream for a specific query and retrieve the updated stream in a lazy iterator
     * @param query to be recorded
     * @param answers answer stream of the query
     * @return lazy iterator of updated answers
     */
    private CacheEntry<Q, LazyAnswerIterator> recordRetrieveLazy(Q query, Stream<ConceptMap> answers){
        CacheEntry<Q, LazyAnswerIterator> match =  this.getEntry(query);
        if (match!= null) {
            Q equivalentQuery = match.query();
            MultiUnifier multiUnifier = query.getMultiUnifier(equivalentQuery, unifierType());
            Stream<ConceptMap> unifiedStream = answers.flatMap(a -> a.unify(multiUnifier));
            //NB: entry overwrite
            this.putEntry(match.query(), match.cachedElement().merge(unifiedStream));
            return match;
        }
        LazyAnswerIterator liter = new LazyAnswerIterator(answers);
        return putEntry(query, liter);
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
            MultiUnifier multiUnifier = equivalentQuery.getMultiUnifier(query, unifierType());
            LazyAnswerIterator unified = match.cachedElement().unify(multiUnifier);
            return new Pair<>(unified, multiUnifier);
        }
        CacheEntry<Q, LazyAnswerIterator> record = record(
                query,
                query.getQuery().stream().map(a -> a.explain(new LookupExplanation(query)))
        );
        return new Pair<>(new LazyAnswerIterator(record.cachedElement().stream()), new MultiUnifierImpl());
    }

    @Override
    public Pair<Stream<ConceptMap>, MultiUnifier> getAnswerStreamWithUnifier(Q query) {
        CacheEntry<Q, LazyAnswerIterator> match =  this.getEntry(query);
        if (match != null) {
            Q equivalentQuery = match.query();
            MultiUnifier multiUnifier = equivalentQuery.getMultiUnifier(query, unifierType());
            Stream<ConceptMap> unified = match.cachedElement().stream().flatMap(a -> a.unify(multiUnifier));
            return new Pair<>(unified, multiUnifier);
        }

        CacheEntry<Q, LazyAnswerIterator> record = record(
                query,
                query.getQuery().stream().map(a -> a.explain(new LookupExplanation(query)))
        );
        return new Pair<>(record.cachedElement().stream(), new MultiUnifierImpl());
    }

    @Override
    public CacheEntry<Q, LazyAnswerIterator> record(Q query, ConceptMap answer, @Nullable MultiUnifier unifier) {
        //TODO
        return null;
    }

    @Override
    public CacheEntry<Q, LazyAnswerIterator> record(Q query, ConceptMap answer, @Nullable CacheEntry<Q, LazyAnswerIterator> entry, @Nullable MultiUnifier unifier) {
        //TODO
        return null;
    }

    @Override
    public ConceptMap findAnswer(Q query, ConceptMap ans) {
        //TODO
        return null;
    }
}
