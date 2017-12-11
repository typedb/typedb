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
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.MultiUnifier;
import ai.grakn.graql.internal.reasoner.iterator.LazyIterator;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueryImpl;
import ai.grakn.graql.internal.reasoner.utils.Pair;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 *
 * <p>
 * Generic container class for storing performed query resolutions.
 * </p>
 *
 * @param <Q> the type of query that is being cached
 * @param <T> the type of answer being cached
 *
 * @author Kasper Piskorski
 *
 */
public abstract class Cache<Q extends ReasonerQueryImpl, T extends Iterable<Answer>>{

    private final Map<Q, CacheEntry<Q, T>> cache = new HashMap<>();
    private final StructuralCache<Q> sCache;

    Cache(){
        this.sCache = new StructuralCache<>();
    }

    /**
     * @return structural cache of this cache
     */
    public StructuralCache<Q> structuralCache(){ return sCache;}

    /**
     * @param query for which the entry is to be retrieved
     * @return corresponding cache entry if any or null
     */
    public CacheEntry<Q, T> get(Q query){ return cache.get(query);}

    /**
     * Associates the specified answers with the specified query in this cache adding an (query) -> (answers) entry
     * @param query of the association
     * @param answers of the association
     * @return previous value if any or null
     */
    public CacheEntry<Q, T> put(Q query, T answers){ return cache.put(query, new CacheEntry<>(query, answers));}

    /**
     * Copies all of the mappings from the specified map to this cache
     * @param map with mappings to be copied
     */
    public void putAll(Map<Q, CacheEntry<Q, T>> map){ cache.putAll(map);}

    /**
     * Perform cache union
     * @param c2 union right operand
     */
    public void add(Cache<Q, T> c2){
        c2.cache.keySet().forEach( q -> this.record(q, c2.getAnswers(q)));
    }

    /**
     * Query cache containment check
     * @param query to be checked for containment
     * @return true if cache contains the query
     */
    public boolean contains(Q query){ return cache.containsKey(query);}

    /**
     * @return all queries constituting this cache
     */
    public Set<Q> getQueries(){ return cache.keySet();}

    /**
     * @return all (query) -> (answers) mappings
     */
    public Collection<CacheEntry<Q, T>> entries(){ return cache.values();}

    /**
     * Perform cache difference
     * @param c2 cache which mappings should be removed from this cache
     */
    public void remove(Cache<Q, T> c2){ remove(c2, getQueries());}

    /**
     * Clear the cache
     */
    public void clear(){ cache.clear();}

    /**
     * record answer iterable for a specific query and retrieve the updated answers
     * @param query to be recorded
     * @param answers to this query
     * @return updated answer iterable
     */
    public abstract T record(Q query, T answers);

    /**
     * record answer stream for a specific query and retrieve the updated stream
     * @param query to be recorded
     * @param answers answer stream of the query
     * @return updated answer stream
     */
    public abstract Stream<Answer> record(Q query, Stream<Answer> answers);

    /**
     * record answer stream for a specific query and retrieve the updated stream in a lazy iterator
     * @param query to be recorded
     * @param answers answer stream of the query
     * @return lazy iterator of updated answers
     */
    public abstract LazyIterator<Answer> recordRetrieveLazy(Q query, Stream<Answer> answers);

    /**
     * retrieve cached answers for provided query
     * @param query for which to retrieve answers
     * @return unified cached answers
     */
    public abstract T getAnswers(Q query);
    public abstract Pair<T, MultiUnifier> getAnswersWithUnifier(Q query);
    public abstract Stream<Answer> getAnswerStream(Q query);
    public abstract Pair<Stream<Answer>, MultiUnifier> getAnswerStreamWithUnifier(Q query);
    public abstract LazyIterator<Answer> getAnswerIterator(Q query);

    /**
     * return an inverse answer map which is more suitable for operations involving concept comparison (joins, filtering, etc.)
     * NB: consumes the underlying stream for the specified query
     * @param query for answer are to be retrieved
     * @param vars variable names of interest
     * @return inverse answer map for specified query
     */
    public Map<Pair<Var, Concept>, Set<Answer>> getInverseAnswerMap(Q query, Set<Var> vars){
        Map<Pair<Var, Concept>, Set<Answer>> inverseAnswerMap = new HashMap<>();
        Set<Answer> answers = getAnswerStream(query).collect(Collectors.toSet());
        answers.forEach(answer -> answer.entrySet().stream()
                .filter(e -> vars.contains(e.getKey()))
                .forEach(entry -> {
                    Pair<Var, Concept> key = new Pair<>(entry.getKey(), entry.getValue());
                    Set<Answer> match = inverseAnswerMap.get(key);
                    if (match != null){
                        match.add(answer);
                    } else {
                        Set<Answer> ans = new HashSet<>();
                        ans.add(answer);
                        inverseAnswerMap.put(key, ans);
                    }
                }));
        return inverseAnswerMap;
    }

    /**
     * returns an inverse answer map with all query variables
     * @param query for answer are to be retrieved
     * @return inverse answer map for specified query
     */
    public Map<Pair<Var, Concept>, Set<Answer>> getInverseAnswerMap(Q query){
        return getInverseAnswerMap(query, query.getVarNames());
    }

    /**
     * cache subtraction of specified queries
     * @param c2 subtraction right operand
     * @param queries to which answers shall be subtracted
     */
    public abstract void remove(Cache<Q, T> c2, Set<Q> queries);

    /**
     * @param queries to be checked
     * @return number of answers for the specified query set
     */
    public abstract long answerSize(Set<Q> queries);
}
