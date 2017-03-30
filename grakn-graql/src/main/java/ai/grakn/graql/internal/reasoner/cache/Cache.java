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
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.internal.reasoner.iterator.LazyIterator;
import ai.grakn.graql.internal.reasoner.query.UnifierImpl;
import javafx.util.Pair;

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
public abstract class Cache<Q extends ReasonerQuery, T extends Iterable<Answer>>{

    protected final boolean explanation;
    protected final Map<Q, Pair<Q, T>> cache = new HashMap<>();

    public Cache(){ this.explanation = false;}
    public Cache(boolean explanation){ this.explanation = explanation;}
    public boolean contains(Q query){ return cache.containsKey(query);}
    public Set<Q> getQueries(){ return cache.keySet();}

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

    public abstract T getAnswers(Q query);
    public abstract Stream<Answer> getAnswerStream(Q query);
    public abstract LazyIterator<Answer> getAnswerIterator(Q query);

    /**
     * return an inverse answer map which is more suitable for operations involving concept comparison (joins, filtering, etc.)
     * NB: consumes the underlying stream for the specified query
     * @param query for answer are to be retrieved
     * @param vars variable names of interest
     * @return inverse answer map for specified query
     */
    public Map<Pair<VarName, Concept>, Set<Answer>> getInverseAnswerMap(Q query, Set<VarName> vars){
        Map<Pair<VarName, Concept>, Set<Answer>> inverseAnswerMap = new HashMap<>();
        Set<Answer> answers = getAnswerStream(query).collect(Collectors.toSet());
        answers.forEach(answer -> answer.entrySet().stream()
                .filter(e -> vars.contains(e.getKey()))
                .forEach(entry -> {
                    Pair<VarName, Concept> key = new Pair<>(entry.getKey(), entry.getValue());
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
    public Map<Pair<VarName, Concept>, Set<Answer>> getInverseAnswerMap(Q query){
        return getInverseAnswerMap(query, query.getVarNames());
    }

    Unifier getRecordUnifier(Q toRecord){
        Q equivalentQuery = contains(toRecord)? cache.get(toRecord).getKey() : null;
        if (equivalentQuery != null) return toRecord.getUnifier(equivalentQuery);
        else return new UnifierImpl();
    }

    Unifier getRetrieveUnifier(Q toRetrieve){
        Q equivalentQuery = contains(toRetrieve)? cache.get( toRetrieve).getKey() : null;
        if (equivalentQuery != null) return  equivalentQuery.getUnifier(toRetrieve);
        else return new UnifierImpl();
    }

    /**
     * cache union
     * @param c2 union right operand
     */
    public void add(Cache<Q, T> c2){
        c2.cache.keySet().forEach( q -> this.record(q, c2.getAnswers(q)));
    }

    /**
     * cache subtraction of specified queries
     * @param c2 subtraction right operand
     * @param queries to which answers shall be subtracted
     */
    public abstract void remove(Cache<Q, T> c2, Set<Q> queries);
    public void remove(Cache<Q, T> c2){ remove(c2, getQueries());}

    public void clear(){ cache.clear();}

    public abstract long answerSize(Set<Q> queries);
}
