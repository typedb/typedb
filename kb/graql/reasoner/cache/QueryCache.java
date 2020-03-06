/*
 * Copyright (C) 2020 Grakn Labs
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
 *
 */

package grakn.core.kb.graql.reasoner.cache;

import grakn.common.util.Pair;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.kb.concept.api.Type;
import grakn.core.kb.graql.reasoner.query.ReasonerQuery;
import grakn.core.kb.graql.reasoner.unifier.MultiUnifier;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Set;
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
        Q extends ReasonerQuery,
        S extends Iterable<ConceptMap>,
        SE extends Collection<ConceptMap>> {

    /**
     * record single answer to a specific query
     *
     * @param query  of interest
     * @param answer to this query
     * @return updated entry
     */
    CacheEntry<Q, SE> record(Q query, ConceptMap answer);

    CacheEntry<Q, SE> record(Q query, ConceptMap answer, @Nullable CacheEntry<Q, SE> entry, @Nullable MultiUnifier unifier);

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

    void ackInsertion();

    void ackDeletion(Type type);

    /**
     * Query cache containment check
     *
     * @param query to be checked for containment
     * @return true if cache contains the query
     */
    boolean contains(Q query);

    /**
     * @param query to be checked for answers
     * @return true if cache answers the input query
     */
    boolean answersQuery(Q query);


    Set<Q> queries();

    /**
     * Clear the cache
     */
    void clear();
}
