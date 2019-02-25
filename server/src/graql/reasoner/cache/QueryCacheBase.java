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

package grakn.core.graql.reasoner.cache;

import grakn.core.concept.answer.ConceptMap;
import grakn.core.graql.reasoner.query.ReasonerQueryImpl;
import grakn.core.graql.reasoner.unifier.MultiUnifier;
import grakn.core.graql.reasoner.unifier.UnifierType;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Base class for storing query resolutions based on alpha-equivalence.
 * A one-to-one mapping is ensured between queries and entries.
 * On retrieval, a relevant entry is identified by means of a query alpha-equivalence check.
 *
 * @param <Q>  the type of query that is being cached
 * @param <R>  the type of answer being cached
 * @param <QE> query cache key type
 * @param <SE> query cache answer container type
 */
public abstract class QueryCacheBase<
        Q extends ReasonerQueryImpl,
        R extends Iterable<ConceptMap>,
        QE,
        SE extends Collection<ConceptMap>> implements QueryCache<Q, R, SE> {

    private final Map<QE, CacheEntry<Q, SE>> cache = new HashMap<>();
    private final StructuralCache<Q> sCache = new StructuralCache<>();

    QueryCacheBase() { }

    abstract UnifierType unifierType();

    abstract QE queryToKey(Q query);

    abstract Q keyToQuery(QE key);

    @Override
    public void clear() { cache.clear();}

    /**
     * @return structural cache of this cache
     */
    StructuralCache<Q> structuralCache() { return sCache;}

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
    public MultiUnifier getCacheUnifier(Q query) {
        CacheEntry<Q, SE> entry = getEntry(query);
        return entry != null ? query.getMultiUnifier(entry.query(), unifierType()) : null;
    }

    /**
     * find specific answer to a query in the cache
     *
     * @param query input query
     * @param ans   sought specific answer to the query
     * @return found answer if any, otherwise empty answer
     */
    public abstract ConceptMap findAnswer(Q query, ConceptMap ans);

    /**
     * @param query for which the entry is to be retrieved
     * @return corresponding cache entry if any or null
     */
    CacheEntry<Q, SE> getEntry(Q query) {
        return cache.get(queryToKey(query));
    }

    /**
     * Associates the specified answers with the specified query in this cache adding an (query) -> (answers) entry
     *
     * @param query   of the association
     * @param answers of the association
     * @return previous value if any or null
     */
    CacheEntry<Q, SE> putEntry(Q query, SE answers) {
        return putEntry(new CacheEntry<>(query, answers));
    }

    CacheEntry<Q, SE> putEntry(CacheEntry<Q, SE> cacheEntry) {
        cache.put(queryToKey(cacheEntry.query()), cacheEntry);
        return cacheEntry;
    }
}
