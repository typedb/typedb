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

import ai.grakn.graql.admin.MultiUnifier;
import ai.grakn.graql.answer.ConceptMap;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueryImpl;
import ai.grakn.graql.internal.reasoner.utils.Pair;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

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
 * @param <S> the type of answer being cached
 *
 * @author Kasper Piskorski
 *
 */
public abstract class QueryCacheBase<Q extends ReasonerQueryImpl, S extends Iterable<ConceptMap>> implements QueryCache<Q, S>{

    private final Map<Q, CacheEntry<Q, S>> cache = new HashMap<>();
    private final StructuralCache<Q> sCache = new StructuralCache<>();
    private final RuleCache ruleCache = new RuleCache();

    QueryCacheBase(){ }

    @Override
    public boolean contains(Q query){ return cache.containsKey(query);}

    @Override
    public Set<Q> getQueries(){ return cache.keySet();}

    @Override
    public void merge(QueryCacheBase<Q, S> c2){
        c2.cache.keySet().forEach( q -> this.record(q, c2.getAnswers(q)));
    }

    @Override
    public void clear(){ cache.clear();}

    /**
     * @return structural cache of this cache
     */
    StructuralCache<Q> structuralCache(){ return sCache;}

    public RuleCache ruleCache(){ return ruleCache;}

    public abstract Pair<S, MultiUnifier> getAnswersWithUnifier(Q query);

    public abstract Pair<Stream<ConceptMap>, MultiUnifier> getAnswerStreamWithUnifier(Q query);

    /**
     * @param query for which the entry is to be retrieved
     * @return corresponding cache entry if any or null
     */
    CacheEntry<Q, S> getEntry(Q query){ return cache.get(query);}

    /**
     * Associates the specified answers with the specified query in this cache adding an (query) -> (answers) entry
     * @param query of the association
     * @param answers of the association
     * @return previous value if any or null
     */
    CacheEntry<Q, S> putEntry(Q query, S answers){ return cache.put(query, new CacheEntry<>(query, answers));}
}
