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
import ai.grakn.graql.internal.reasoner.iterator.LazyIterator;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import javafx.util.Pair;

/**
 *
 * <p>
 * Generic container class for storing performed query resolutions.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public abstract class Cache<Q extends ReasonerQuery, T extends Iterable<Map<VarName, Concept>>>{

    protected final Map<Q, Pair<Q, T>> cache = new HashMap<>();

    public Cache(){ super();}
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
    public abstract Stream<Map<VarName, Concept>> record(Q query, Stream<Map<VarName, Concept>> answers);

    /**
     * record answer stream for a specific query and retrieve the updated stream in a lazy iterator
     * @param query to be recorded
     * @param answers answer stream of the query
     * @return lazy iterator of updated answers
     */
    public abstract LazyIterator<Map<VarName, Concept>> recordRetrieveLazy(Q query, Stream<Map<VarName, Concept>> answers);

    public abstract T getAnswers(Q query);
    public abstract Stream<Map<VarName, Concept>> getAnswerStream(Q query);
    public abstract Stream<Map<VarName, Concept>> getLimitedAnswerStream(Q query, LazyIterator<Map<VarName, Concept>> subIter, Set<VarName> subVars);
    public abstract Map<Pair<VarName, Concept>, Set<Map<VarName, Concept>>> getInverseAnswerMap(Q query, Set<VarName> vars);
    public abstract LazyIterator<Map<VarName, Concept>> getAnswerIterator(Q query);

    Map<VarName, VarName> getRecordUnifiers(Q toRecord){
        Q equivalentQuery = contains(toRecord)? cache.get(toRecord).getKey() : null;
        if (equivalentQuery != null) return toRecord.getUnifiers(equivalentQuery);
        else return new HashMap<>();
    }

    Map<VarName, VarName> getRetrieveUnifiers(Q toRetrieve){
        Q equivalentQuery = contains(toRetrieve)? cache.get( toRetrieve).getKey() : null;
        if (equivalentQuery != null) return  equivalentQuery.getUnifiers(toRetrieve);
        else return new HashMap<>();
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

    public void clear(){ cache.clear();}

    public abstract long answerSize(Set<Q> queries);
}
