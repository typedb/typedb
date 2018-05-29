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

package ai.grakn.graql.internal.reasoner.cache;

import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.MultiUnifier;
import ai.grakn.graql.internal.reasoner.MultiUnifierImpl;
import ai.grakn.graql.internal.reasoner.explanation.LookupExplanation;
import ai.grakn.graql.internal.reasoner.iterator.LazyAnswerIterator;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueryImpl;
import ai.grakn.graql.internal.reasoner.utils.Pair;

import java.util.Set;
import java.util.stream.Collectors;
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
public class LazyQueryCache<Q extends ReasonerQueryImpl> extends Cache<Q, LazyAnswerIterator>{

    public LazyQueryCache(){ super();}

    @Override
    public LazyAnswerIterator record(Q query, LazyAnswerIterator answers) {
        CacheEntry<Q, LazyAnswerIterator> match =  this.getEntry(query);
        if (match != null) {
            Q equivalentQuery = match.query();
            Stream<Answer> unifiedStream = answers.unify(query.getMultiUnifier(equivalentQuery)).stream();
            //NB: entry overwrite
            this.putEntry(match.query(), match.cachedElement().merge(unifiedStream));
            return getAnswerIterator(query);
        }
        this.putEntry(query, answers);
        return answers;
    }

    @Override
    public Stream<Answer> record(Q query, Stream<Answer> answers) {
        return recordRetrieveLazy(query, answers).stream();
    }

    /**
     * record answer stream for a specific query and retrieve the updated stream in a lazy iterator
     * @param query to be recorded
     * @param answers answer stream of the query
     * @return lazy iterator of updated answers
     */
    public LazyAnswerIterator recordRetrieveLazy(Q query, Stream<Answer> answers){
        CacheEntry<Q, LazyAnswerIterator> match =  this.getEntry(query);
        if (match!= null) {
            Q equivalentQuery = match.query();
            MultiUnifier multiUnifier = query.getMultiUnifier(equivalentQuery);
            Stream<Answer> unifiedStream = answers.flatMap(a -> a.unify(multiUnifier));
            //NB: entry overwrite
            this.putEntry(match.query(), match.cachedElement().merge(unifiedStream));
            return getAnswerIterator(query);
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
    public Pair<LazyAnswerIterator, MultiUnifier> getAnswersWithUnifier(Q query) {
        CacheEntry<Q, LazyAnswerIterator> match =  this.getEntry(query);
        if (match != null) {
            Q equivalentQuery = match.query();
            MultiUnifier multiUnifier = equivalentQuery.getMultiUnifier(query);
            LazyAnswerIterator unified = match.cachedElement().unify(multiUnifier);
            return new Pair<>(unified, multiUnifier);
        }
        Stream<Answer> answerStream = record(
                query,
                query.getQuery().stream().map(a -> a.explain(new LookupExplanation(query)))
        );
        return new Pair<>(new LazyAnswerIterator(answerStream), new MultiUnifierImpl());
    }

    @Override
    public Stream<Answer> getAnswerStream(Q query){
        return getAnswerStreamWithUnifier(query).getKey();
    }

    @Override
    public Pair<Stream<Answer>, MultiUnifier> getAnswerStreamWithUnifier(Q query) {
        CacheEntry<Q, LazyAnswerIterator> match =  this.getEntry(query);
        if (match != null) {
            Q equivalentQuery = match.query();
            MultiUnifier multiUnifier = equivalentQuery.getMultiUnifier(query);
            Stream<Answer> unified = match.cachedElement().stream().flatMap(a -> a.unify(multiUnifier));
            return new Pair<>(unified, multiUnifier);
        }

        Stream<Answer> answerStream = record(
                query,
                query.getQuery().stream().map(a -> a.explain(new LookupExplanation(query)))
        );
        return new Pair<>(answerStream, new MultiUnifierImpl());
    }

    public LazyAnswerIterator getAnswerIterator(Q query) {
        return getAnswers(query);
    }

    @Override
    public void remove(Cache<Q, LazyAnswerIterator> c2, Set<Q> queries) {
        c2.getQueries().stream()
                .filter(queries::contains)
                .filter(this::contains)
                .forEach( q -> {
                    CacheEntry<Q, LazyAnswerIterator> match =  this.getEntry(q);
                    Set<Answer> s = match.cachedElement().stream().collect(Collectors.toSet());
                    s.removeAll(c2.getAnswerStream(q).collect(Collectors.toSet()));
                    this.putEntry(match.query(), new LazyAnswerIterator(s.stream()));
                });
    }

}
