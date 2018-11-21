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
import grakn.core.graql.internal.reasoner.query.ReasonerQueryImpl;
import grakn.core.graql.internal.reasoner.unifier.UnifierType;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import javax.annotation.Nullable;

/**
 *
 * <p>
 * Base class for storing query resolutions based on alpha-equivalence.
 * A one-to-one mapping is ensured between queries and entries.
 * On retrieval, a relevant entry is identified by means of a query alpha-equivalence check.
 *
 * </p>
 *
 * @param <Q> the type of query that is being cached
 * @param <R> the type of answer being cached
 * @param <QE> query cache key type
 * @param <SE> query cache answer container type
 *
 * @author Kasper Piskorski
 *
 */
public abstract class QueryCacheBase<
        Q extends ReasonerQueryImpl,
        R extends Iterable<ConceptMap>,
        QE,
        SE extends Collection<ConceptMap>> implements QueryCache<Q, R, SE>{

    private final Map<QE, CacheEntry<Q, SE>> cache = new HashMap<>();
    private final StructuralCache<Q> sCache = new StructuralCache<>();
    private final RuleCache ruleCache = new RuleCache();

    public static long propagateTime = 0;
    public static long recordTime1 = 0, recordTime2 = 0;
    public static long getTime = 0;
    public static long addEntryTime= 0;
    public static long getEntryTime = 0;
    public static long putEntryTime = 0;
    public static long unifyTime= 0;
    public static long unifierTime = 0;

    public static long recordCalls = 0;
    public static long getCalls = 0;
    public static long unifyCalls = 0;
    public static long getEntryCalls = 0;
    public static long addEntryCalls = 0;
    public static long putEntryCalls = 0;
    public static long cacheHits =0;
    public static long cacheMiss = 0;

    QueryCacheBase(){ }

    abstract UnifierType unifierType();

    abstract QE queryToKey(Q query);
    abstract Q keyToQuery(QE key);

    @Override
    public void clear(){ cache.clear();}

    /**
     * @return structural cache of this cache
     */
    StructuralCache<Q> structuralCache(){ return sCache;}

    public RuleCache ruleCache(){ return ruleCache;}

    @Override
    public CacheEntry<Q, SE> record(Q query, ConceptMap answer) {
        return record(query, answer, null, null);
    }

    @Override
    public CacheEntry<Q, SE> record(Q query, ConceptMap answer, @Nullable MultiUnifier unifier) {
        return record(query, answer, null, unifier);
    }

    @Override
    public R getAnswers(Q query) { return getAnswersWithUnifier(query).getKey(); }

    @Override
    public Stream<ConceptMap> getAnswerStream(Q query) { return getAnswerStreamWithUnifier(query).getKey(); }

    @Override
    public boolean contains(Q query) { return getEntry(query) != null; }

    /**
     * @param query to find unifier for
     * @return unifier that unifies this query with the cache equivalent
     */
    public MultiUnifier getCacheUnifier(Q query){
        CacheEntry<Q, SE> entry = getEntry(query);
        return entry != null? query.getMultiUnifier(entry.query(), unifierType()) : null;
    }

    /**
     * find specific answer to a query in the cache
     * @param query input query
     * @param ans sought specific answer to the query
     * @return found answer if any, otherwise empty answer
     */
    public abstract ConceptMap findAnswer(Q query, ConceptMap ans);

    /**
     * @param query for which the entry is to be retrieved
     * @return corresponding cache entry if any or null
     */
    CacheEntry<Q, SE> getEntry(Q query){
        getEntryCalls++;
        long start = System.currentTimeMillis();
        CacheEntry<Q, SE> cacheEntry = cache.get(queryToKey(query));
        getEntryTime += System.currentTimeMillis() - start;
        return cacheEntry;
    }

    /**
     * Associates the specified answers with the specified query in this cache adding an (query) -> (answers) entry
     * @param query of the association
     * @param answers of the association
     * @return previous value if any or null
     */
    CacheEntry<Q, SE> putEntry(Q query, SE answers){
        return putEntry(new CacheEntry<>(query, answers));
    }

    CacheEntry<Q, SE> putEntry(CacheEntry<Q, SE> cacheEntry) {
        putEntryCalls++;
        long start = System.currentTimeMillis();
        cache.put(queryToKey(cacheEntry.query()), cacheEntry);
        putEntryTime += System.currentTimeMillis() - start;
        return cacheEntry;
    }
}
