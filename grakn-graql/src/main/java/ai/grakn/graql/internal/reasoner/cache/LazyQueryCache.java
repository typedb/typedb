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

import ai.grakn.concept.Concept;
import ai.grakn.graql.VarName;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.internal.reasoner.iterator.LazyAnswerIterator;
import ai.grakn.graql.internal.reasoner.iterator.LazyIterator;
import ai.grakn.graql.internal.reasoner.query.QueryAnswerStream;
import ai.grakn.graql.internal.reasoner.query.QueryAnswers;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javafx.util.Pair;

import static ai.grakn.graql.internal.reasoner.query.QueryAnswerStream.varFilterFunction;

/**
 *
 * <p>
 * Lazy container class for storing performed query resolutions.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public class LazyQueryCache<Q extends ReasonerQuery> extends Cache<Q, LazyAnswerIterator>{


    public LazyQueryCache(){ super();}

    @Override
    public LazyAnswerIterator record(Q query, LazyAnswerIterator answers) {
        Pair<Q, LazyAnswerIterator> match =  cache.get(query);
        if (match != null) {
            Stream<Map<VarName, Concept>> unifiedStream = answers.unify(getRecordUnifiers(query)).stream();
            cache.put(match.getKey(), new Pair<>(match.getKey(), match.getValue().merge(unifiedStream)));
        } else {
            cache.put(query, new Pair<>(query, answers));
        }
        return getAnswerIterator(query);
    }

    @Override
    public Stream<Map<VarName, Concept>> record(Q query, Stream<Map<VarName, Concept>> answers) {
        return recordRetrieveLazy(query, answers).stream();
    }

    /**
     * updates the cache by the specified query
     * @param query query to be added/updated
     * @param answers answers to the query
     */
    @Override
    public LazyAnswerIterator recordRetrieveLazy(Q query, Stream<Map<VarName, Concept>> answers){
        Pair<Q, LazyAnswerIterator> match =  cache.get(query);
        if (match!= null) {
            Stream<Map<VarName, Concept>> unifiedStream = QueryAnswerStream.unify(answers, getRecordUnifiers(query));
            cache.put(match.getKey(), new Pair<>(match.getKey(), match.getValue().merge(unifiedStream)));
        } else {
            cache.put(query, new Pair<>(query, new LazyAnswerIterator(answers)));
        }
        return getAnswerIterator(query);
    }

    /**
     * retrieve cached answers for provided query
     * @param query for which to retrieve answers
     * @return unified cached answers
     */
    @Override
    public LazyAnswerIterator getAnswers(Q query) {
        Pair<Q, LazyAnswerIterator> match =  cache.get(query);
        if (match != null) {
            return match.getValue().unify(getRetrieveUnifiers(query));
        }
        else return new LazyAnswerIterator(Stream.empty());
    }

    @Override
    public Stream<Map<VarName, Concept>> getAnswerStream(Q query){
        Pair<Q, LazyAnswerIterator> match =  cache.get(query);
        if (match != null) {
            Map<VarName, VarName> unifiers = getRetrieveUnifiers(query);
            return match.getValue().stream().map(a -> QueryAnswers.unify(a, unifiers));
        }
        else return Stream.empty();
    }

    @Override
    public LazyAnswerIterator getAnswerIterator(Q query) {
        return getAnswers(query);
    }

    @Override
    public Stream<Map<VarName, Concept>> getLimitedAnswerStream(Q query, LazyIterator<Map<VarName, Concept>> subIter, Set<VarName> subVars){
        Set<Concept> concepts = subIter.stream()
                .flatMap(a -> varFilterFunction.apply(a, subVars))
                .map(Map::values).flatMap(Collection::stream).collect(Collectors.toSet());
        return getAnswerStream(query).filter(ans -> {
            for (VarName var : subVars)
                if (!concepts.contains(ans.get(var))) return false;
            return true;
        }
        );
    }

    @Override
    public Map<Pair<VarName, Concept>, Set<Map<VarName, Concept>>> getInverseAnswerMap(Q query, Set<VarName> vars){
        Map<Pair<VarName, Concept>, Set<Map<VarName, Concept>>> inverseAnswerMap = new HashMap<>();
        Set<Map<VarName, Concept>> answers = getAnswers(query).stream().collect(Collectors.toSet());
        answers.forEach(answer -> {
            answer.entrySet().stream()
                    .filter(e -> vars.contains(e.getKey()))
                    .forEach(entry -> {
                Pair<VarName, Concept> key = new Pair<>(entry.getKey(), entry.getValue());
                Set<Map<VarName, Concept>> match = inverseAnswerMap.get(key);
                if (match != null){
                    match.add(answer);
                } else {
                    Set<Map<VarName, Concept>> ans = new HashSet<>();
                    ans.add(answer);
                    inverseAnswerMap.put(key, ans);
                }
            });
        });
        return inverseAnswerMap;
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
                    Set<Map<VarName, Concept>> s = match.getValue().stream().collect(Collectors.toSet());
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
}
