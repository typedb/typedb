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

package grakn.core.graql.reasoner.cache;

import com.google.common.base.Equivalence;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.graql.reasoner.explanation.LookupExplanation;
import grakn.core.graql.reasoner.query.ReasonerQueryEquivalence;
import grakn.core.graql.reasoner.query.ReasonerQueryImpl;
import grakn.core.graql.reasoner.unifier.UnifierType;
import grakn.core.kb.concept.api.ConceptId;
import grakn.core.kb.graql.executor.ExecutorFactory;
import grakn.core.kb.graql.executor.TraversalExecutor;
import grakn.core.kb.graql.planning.gremlin.GraqlTraversal;
import grakn.core.kb.graql.planning.gremlin.TraversalPlanFactory;
import grakn.core.kb.graql.reasoner.cache.CacheEntry;
import grakn.core.kb.graql.reasoner.unifier.MultiUnifier;
import grakn.core.kb.graql.reasoner.unifier.Unifier;
import graql.lang.statement.Variable;

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
    private TraversalPlanFactory traversalPlanFactory;
    private TraversalExecutor traversalExecutor;

    StructuralCache(TraversalPlanFactory traversalPlanFactory, TraversalExecutor traversalExecutor){
        this.traversalPlanFactory = traversalPlanFactory;
        this.traversalExecutor = traversalExecutor;
        this.structCache = new HashMap<>();
    }

    /**
     * @param query to be retrieved
     * @return answer stream of provided query
     */
    public Stream<ConceptMap> get(Q query){
        Equivalence.Wrapper<Q> structQuery = equivalence.wrap(query);

        CacheEntry<Q, GraqlTraversal> match = structCache.get(structQuery);
        if (match != null){
            Q equivalentQuery = match.query();
            GraqlTraversal traversal = match.cachedElement();
            MultiUnifier multiUnifier = equivalentQuery.getMultiUnifier(query, UnifierType.STRUCTURAL);
            Unifier unifier = multiUnifier.getAny();
            Map<Variable, ConceptId> idTransform = equivalentQuery.idTransform(query, unifier);

            ReasonerQueryImpl transformedQuery = equivalentQuery.transformIds(idTransform);

            return traversalExecutor.traverse(transformedQuery.getPattern(), traversal.transform(idTransform))
                    .map(unifier::apply)
                    .map(a -> a.explain(new LookupExplanation(), query.getPattern()));
        }

        GraqlTraversal traversal = traversalPlanFactory.createTraversal(query.getPattern());
        structCache.put(structQuery, new CacheEntry<>(query, traversal));

        return traversalExecutor.traverse(query.getPattern(), traversal)
                .map(a -> a.explain(new LookupExplanation(), query.getPattern()));
    }

    public void clear(){
        structCache.clear();
    }
}
