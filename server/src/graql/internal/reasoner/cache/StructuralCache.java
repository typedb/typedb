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

import com.google.common.base.Equivalence;
import grakn.core.graql.admin.MultiUnifier;
import grakn.core.graql.admin.Unifier;
import grakn.core.graql.answer.ConceptMap;
import grakn.core.graql.concept.ConceptId;
import grakn.core.graql.internal.gremlin.GraqlTraversal;
import grakn.core.graql.internal.gremlin.GreedyTraversalPlan;
import grakn.core.graql.internal.match.MatchBase;
import grakn.core.graql.internal.reasoner.explanation.LookupExplanation;
import grakn.core.graql.internal.reasoner.query.ReasonerQueryEquivalence;
import grakn.core.graql.internal.reasoner.query.ReasonerQueryImpl;
import grakn.core.graql.internal.reasoner.unifier.UnifierType;
import grakn.core.graql.query.pattern.Variable;
import grakn.core.server.session.TransactionImpl;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Container class allowing to store similar graql traversals with similarity measure based on structural query equivalence.
 * On cache hit a concept map between provided query and the one contained in the cache is constructed. Based on that mapping,
 * id predicates of the cached query are transformed.
 * The returned stream is a stream of the transformed cached query unified with the provided query.
 *
 * @param <Q> the type of query that is being cached
 */
public class StructuralCache<Q extends ReasonerQueryImpl>{

    private final ReasonerQueryEquivalence equivalence = ReasonerQueryEquivalence.StructuralEquivalence;
    private final Map<Equivalence.Wrapper<Q>, CacheEntry<Q, GraqlTraversal>> structCache;

    StructuralCache(){
        this.structCache = new HashMap<>();
    }

    /**
     * @param query to be retrieved
     * @return answer stream of provided query
     */
    public Stream<ConceptMap> get(Q query){
        Equivalence.Wrapper<Q> structQuery = equivalence.wrap(query);
        TransactionImpl<?> tx = query.tx();

        CacheEntry<Q, GraqlTraversal> match = structCache.get(structQuery);
        if (match != null){
            Q equivalentQuery = match.query();
            GraqlTraversal traversal = match.cachedElement();
            MultiUnifier multiUnifier = equivalentQuery.getMultiUnifier(query, UnifierType.STRUCTURAL);
            Unifier unifier = multiUnifier.getAny();
            Map<Variable, ConceptId> idTransform = equivalentQuery.idTransform(query, unifier);

            ReasonerQueryImpl transformedQuery = equivalentQuery.transformIds(idTransform);

            return MatchBase.streamWithTraversal(transformedQuery.getPattern().variables(), tx, traversal.transform(idTransform))
                    .map(ans -> ans.unify(unifier))
                    .map(a -> a.explain(new LookupExplanation(query)));
        }

        GraqlTraversal traversal = GreedyTraversalPlan.createTraversal(query.getPattern(), tx);
        structCache.put(structQuery, new CacheEntry<>(query, traversal));

        return MatchBase.streamWithTraversal(query.getPattern().variables(), tx, traversal)
                .map(a -> a.explain(new LookupExplanation(query)));
    }
}
