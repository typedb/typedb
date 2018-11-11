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

import grakn.core.graql.answer.ConceptMap;
import grakn.core.graql.internal.reasoner.query.ReasonerQueryImpl;
import java.util.Set;
import java.util.stream.Stream;


/**
 *
 * <p>
 * Generic interface query caches.
 *
 * Defines two basic operations:
 * - GET(Query)
 * - RECORD(Query, Answer).
 *
 * </p>
 *
 * @param <Q> the type of query that is being cached
 * @param <S> the type of answer being cached
 *
 * @author Kasper Piskorski
 *
 */
public interface QueryCache<Q extends ReasonerQueryImpl, S extends Iterable<ConceptMap>>{

    /**
     * record answer iterable for a specific query and retrieve the updated answers
     * @param query to be recorded
     * @param answers to this query
     * @return updated answer iterable
     */
    S record(Q query, S answers);

    /**
     * record answer stream for a specific query and retrieve the updated stream
     * @param query to be recorded
     * @param answers answer stream of the query
     * @return updated answer stream
     */
    Stream<ConceptMap> record(Q query, Stream<ConceptMap> answers);

    /**
     * record single answer to a specific query
     * @param query of interest
     * @param answer to this query
     * @return recorded answer
     */
    ConceptMap record(Q query, ConceptMap answer);

    /**
     * retrieve (possibly) cached answers for provided query
     * @param query for which to retrieve answers
     * @return unified cached answers
     */
    S getAnswers(Q query);

    Stream<ConceptMap> getAnswerStream(Q query);

    /**
     * Query cache containment check
     * @param query to be checked for containment
     * @return true if cache contains the query
     */
    boolean contains(Q query);

    /**
     * @return all queries contained in this cache
     */
    Set<Q> getQueries();

    /**
     * Perform cache union
     * @param c2 union right operand
     */
    void merge(QueryCacheBase<Q, S> c2);

    /**
     * Clear the cache
     */
    void clear();
}
