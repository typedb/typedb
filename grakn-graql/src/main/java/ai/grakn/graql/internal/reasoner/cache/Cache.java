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
import ai.grakn.graql.internal.reasoner.query.ReasonerQueryImpl;
import ai.grakn.graql.internal.reasoner.utils.Pair;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

/**
 *
 * <p>
 * Generic container class for storing performed query resolutions.
 * A one-to-one mapping is ensured between queries and entries.
 * On retrieval, a relevant entry is identified by means of a query alpha-equivalence check.
 *
 * Defines two basic operations:
 * - GET(Query) - retrieve an entry corresponding to a provided query, if entry doesn't exist return db lookup result of the query.
 * - RECORD(Query) - if the query entry exists, update the entry, otherwise create a new entry. In each case return an up-to-date entry.
 *
 * </p>
 *
 * @param <Q> the type of query that is being cached
 * @param <T> the type of answer being cached
 *
 * @author Kasper Piskorski
 *
 */
public abstract class Cache<Q extends ReasonerQueryImpl, T extends Iterable<ConceptMap>>{

    private final Map<Q, CacheEntry<Q, T>> cache = new HashMap<>();
    private final StructuralCache<Q> sCache;

    Cache(){
        this.sCache = new StructuralCache<>();
    }

    /**
     * @return structural cache of this cache
     */
    StructuralCache<Q> structuralCache(){ return sCache;}

    /**
     * @param query for which the entry is to be retrieved
     * @return corresponding cache entry if any or null
     */
    CacheEntry<Q, T> getEntry(Q query){ return cache.get(query);}

    /**
     * Associates the specified answers with the specified query in this cache adding an (query) -> (answers) entry
     * @param query of the association
     * @param answers of the association
     * @return previous value if any or null
     */
    CacheEntry<Q, T> putEntry(Q query, T answers){ return cache.put(query, new CacheEntry<>(query, answers));}

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
    public abstract Stream<ConceptMap> record(Q query, Stream<ConceptMap> answers);

    /**
     * retrieve (possibly) cached answers for provided query
     * @param query for which to retrieve answers
     * @return unified cached answers
     */
    public abstract T getAnswers(Q query);

    public abstract Pair<T, MultiUnifier> getAnswersWithUnifier(Q query);

    public abstract Stream<ConceptMap> getAnswerStream(Q query);

    public abstract Pair<Stream<ConceptMap>, MultiUnifier> getAnswerStreamWithUnifier(Q query);

    /**
     * cache subtraction of specified queries
     * @param c2 subtraction right operand
     * @param queries to which answers shall be subtracted
     */
    public abstract void remove(Cache<Q, T> c2, Set<Q> queries);

}
