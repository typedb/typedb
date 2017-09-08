/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
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

package ai.grakn.graql.internal.reasoner.cache;

import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.internal.reasoner.explanation.LookupExplanation;
import ai.grakn.graql.internal.reasoner.iterator.LazyAnswerIterator;
import ai.grakn.graql.internal.reasoner.UnifierImpl;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueryImpl;
import ai.grakn.graql.internal.reasoner.utils.Pair;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 *
 * <p>
 * Lazy container class for storing performed query resolutions.
 * NB: In case the entry is not found, get methods perform a record operation.
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
        Pair<Q, LazyAnswerIterator> match =  cache.get(query);
        if (match != null) {
            Q equivalentQuery = match.getKey();
            Stream<Answer> unifiedStream = answers.unify(query.getUnifier(equivalentQuery)).stream();
            cache.put(match.getKey(), new Pair<>(match.getKey(), match.getValue().merge(unifiedStream)));
        } else {
            cache.put(query, new Pair<>(query, answers));
        }
        return getAnswerIterator(query);
    }

    @Override
    public Stream<Answer> record(Q query, Stream<Answer> answers) {
        return recordRetrieveLazy(query, answers).stream();
    }

    @Override
    public LazyAnswerIterator recordRetrieveLazy(Q query, Stream<Answer> answers){
        Pair<Q, LazyAnswerIterator> match =  cache.get(query);
        if (match!= null) {
            Q equivalentQuery = match.getKey();
            Unifier u = query.getUnifier(equivalentQuery);
            Stream<Answer> unifiedStream = answers.map(a -> a.unify(u));
            cache.put(match.getKey(), new Pair<>(match.getKey(), match.getValue().merge(unifiedStream)));
        } else {
            cache.put(query, new Pair<>(query, new LazyAnswerIterator(answers)));
        }
        return getAnswerIterator(query);
    }

    @Override
    public LazyAnswerIterator getAnswers(Q query) {
        return getAnswersWithUnifier(query).getKey();
    }

    @Override
    public Pair<LazyAnswerIterator, Unifier> getAnswersWithUnifier(Q query) {
        Pair<Q, LazyAnswerIterator> match =  cache.get(query);
        if (match != null) {
            Q equivalentQuery = match.getKey();
            Unifier unifier = equivalentQuery.getUnifier(query);
            LazyAnswerIterator unified = match.getValue().unify(unifier);
            return new Pair<>(unified, unifier);
        }
        Stream<Answer> answerStream = record(
                query,
                query.getQuery().stream().map(a -> a.explain(new LookupExplanation(query)))
        );
        return new Pair<>(new LazyAnswerIterator(answerStream), new UnifierImpl());
    }

    @Override
    public Stream<Answer> getAnswerStream(Q query){
        return getAnswerStreamWithUnifier(query).getKey();
    }

    @Override
    public Pair<Stream<Answer>, Unifier> getAnswerStreamWithUnifier(Q query) {
        Pair<Q, LazyAnswerIterator> match =  cache.get(query);
        if (match != null) {
            Q equivalentQuery = match.getKey();
            Unifier unifier = equivalentQuery.getUnifier(query);
            Stream<Answer> unified = match.getValue().stream().map(a -> a.unify(unifier));
            return new Pair<>(unified, unifier);
        }

        Stream<Answer> answerStream = record(
                query,
                query.getQuery().stream().map(a -> a.explain(new LookupExplanation(query)))
        );
        return new Pair<>(answerStream, new UnifierImpl());
    }

    @Override
    public LazyAnswerIterator getAnswerIterator(Q query) {
        return getAnswers(query);
    }

    @Override
    public long answerSize(Set<Q> queries){
        return cache.values().stream()
                .filter(p -> queries.contains(p.getKey()))
                .map(v -> v.getValue().size()).mapToLong(Long::longValue).sum();
    }

    @Override
    public void remove(Cache<Q, LazyAnswerIterator> c2, Set<Q> queries) {
        c2.cache.keySet().stream()
                .filter(queries::contains)
                .filter(this::contains)
                .forEach( q -> {
                    Pair<Q, LazyAnswerIterator> match = cache.get(q);
                    Set<Answer> s = match.getValue().stream().collect(Collectors.toSet());
                    s.removeAll(c2.getAnswerStream(q).collect(Collectors.toSet()));
                    cache.put(match.getKey(), new Pair<>(match.getKey(), new LazyAnswerIterator(s.stream())));
                });
    }

    /**
     * force stream consumption and reload cache
     */
    public void reload(){
        Map<Q, Pair<Q, LazyAnswerIterator>> newCache = new HashMap<>();
        cache.entrySet().forEach(entry ->
                newCache.put(entry.getKey(), new Pair<>(
                        entry.getKey(),
                        new LazyAnswerIterator(entry.getValue().getValue().stream().collect(Collectors.toSet()).stream()))));
        cache.clear();
        cache.putAll(newCache);
    }

    public void consume() {
        cache.entrySet().forEach(entry ->
                entry.getValue().getValue().stream().collect(Collectors.toSet()));
    }
}
