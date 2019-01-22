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

import grakn.core.graql.internal.reasoner.unifier.MultiUnifier;
import grakn.core.graql.answer.ConceptMap;
import grakn.core.graql.internal.reasoner.query.ReasonerQueryImpl;
import grakn.core.graql.internal.reasoner.utils.Pair;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.stream.Stream;


/**
 * Generic interface query caches.
 * <p>
 * Defines two basic operations:
 * - GET(Query)
 * - RECORD(Query, Answer).
 *
 * @param <Q>  the type of query that is being cached
 * @param <S>  the type of answer being cached
 * @param <SE> the type of answer being cached
 */
public interface QueryCache<
        Q extends ReasonerQueryImpl,
        S extends Iterable<ConceptMap>,
        SE extends Collection<ConceptMap>> {

    /**
     * record answer iterable for a specific query and retrieve the updated answers
     *
     * @param query   to be recorded
     * @param answers to this query
     * @return updated entry
     */
    CacheEntry<Q, SE> record(Q query, S answers);

    /**
     * record answer stream for a specific query and retrieve the updated stream
     *
     * @param query   to be recorded
     * @param answers answer stream of the query
     * @return updated entry
     */
    CacheEntry<Q, SE> record(Q query, Stream<ConceptMap> answers);

    /**
     * record single answer to a specific query
     *
     * @param query  of interest
     * @param answer to this query
     * @return updated entry
     */
    CacheEntry<Q, SE> record(Q query, ConceptMap answer);

    CacheEntry<Q, SE> record(Q query, ConceptMap answer, @Nullable CacheEntry<Q, SE> entry, @Nullable MultiUnifier unifier);

    CacheEntry<Q, SE> record(Q query, ConceptMap answer, @Nullable MultiUnifier unifier);

    /**
     * retrieve (possibly) cached answers for provided query
     *
     * @param query for which to retrieve answers
     * @return unified cached answers
     */
    S getAnswers(Q query);

    Stream<ConceptMap> getAnswerStream(Q query);

    Pair<S, MultiUnifier> getAnswersWithUnifier(Q query);

    Pair<Stream<ConceptMap>, MultiUnifier> getAnswerStreamWithUnifier(Q query);


    /**
     * Query cache containment check
     *
     * @param query to be checked for containment
     * @return true if cache contains the query
     */
    boolean contains(Q query);

    /**
     * Clear the cache
     */
    void clear();
}
