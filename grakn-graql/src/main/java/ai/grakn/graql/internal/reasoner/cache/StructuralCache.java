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

import ai.grakn.concept.ConceptId;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.internal.gremlin.GraqlTraversal;
import ai.grakn.graql.internal.gremlin.GreedyTraversalPlan;
import ai.grakn.graql.internal.query.match.MatchBase;
import ai.grakn.graql.internal.reasoner.explanation.LookupExplanation;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueryImpl;
import ai.grakn.graql.internal.reasoner.query.ReasonerStructuralQuery;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

/**
 *
 * <p>
 * Container class allowing to store similar graql traversals with similarity measure based on structural query equivalence.
 *
 * On cache hit a concept map between provided query and the one contained in the cache is constructed. Based on that mapping,
 * id predicates of the cached query are transformed.
 *
 * The returned stream is a stream of the transformed cached query unified with the provided query.
 * </p>
 *
 * @param <Q> the type of query that is being cached
 *
 * @author Kasper Piskorski
 *
 */
public class StructuralCache<Q extends ReasonerQueryImpl>{

    private final Map<ReasonerStructuralQuery<Q>, CacheEntry<Q, GraqlTraversal>> structCache;

    public StructuralCache(){
        this.structCache = new HashMap<>();
    }

    /**
     * @param query to be retrieved
     * @return answer stream of provided query
     */
    public Stream<Answer> get(Q query){
        ReasonerStructuralQuery<Q> structQuery = new ReasonerStructuralQuery<>(query);

        CacheEntry<Q, GraqlTraversal> match = structCache.get(structQuery);
        if (match != null){
            Q equivalentQuery = match.query();
            GraqlTraversal traversal = match.cachedElement();
            Unifier unifier = equivalentQuery.getUnifier(query);
            Map<ConceptId, ConceptId> conceptMap = equivalentQuery.getConceptMap(query, unifier);

            ReasonerQueryImpl transformedQuery = equivalentQuery.transformIds(conceptMap);
            return MatchBase.streamWithTraversal(transformedQuery.getPattern(), transformedQuery.tx(), traversal.transform(conceptMap))
                    .map(ans -> ans.unify(unifier))
                    .map(a -> a.explain(new LookupExplanation(query)));
        }

        GraqlTraversal traversal = GreedyTraversalPlan.createTraversal(query.getPattern(), query.tx());
        structCache.put(structQuery, new CacheEntry<>(query, traversal));

        return MatchBase.streamWithTraversal(query.getPattern(), query.tx(), traversal)
                .map(a -> a.explain(new LookupExplanation(query)));
    }
}
