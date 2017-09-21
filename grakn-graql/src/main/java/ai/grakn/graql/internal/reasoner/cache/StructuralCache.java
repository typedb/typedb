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
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.internal.gremlin.GraqlTraversal;
import ai.grakn.graql.internal.gremlin.GreedyTraversalPlan;
import ai.grakn.graql.internal.reasoner.UnifierImpl;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueryImpl;
import ai.grakn.graql.internal.reasoner.query.ReasonerStructuralQuery;
import ai.grakn.graql.internal.reasoner.rule.InferenceRule;
import ai.grakn.graql.internal.reasoner.utils.Pair;
import com.google.common.collect.Iterators;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;


/**
 *
 * <p>
 * TODO
 * </p>
 *
 * @param <Q> query type
 *
 * @author Kasper Piskorski
 *
 */
public class StructuralCache<Q extends ReasonerQueryImpl>{

    private final Map<ReasonerStructuralQuery<Q>, Pair<ReasonerStructuralQuery<Q>, StructuralCacheEntry<Q>>> structCache;

    public StructuralCache(){
        this.structCache = new HashMap<>();
    }

    public Pair<StructuralCacheEntry<Q>, Pair<Unifier, Map<ConceptId, ConceptId>>> get(Q query){
        ReasonerStructuralQuery<Q> structQuery = new ReasonerStructuralQuery<>(query);

        Pair<ReasonerStructuralQuery<Q>, StructuralCacheEntry<Q>> match = structCache.get(structQuery);
        if (match != null){
            Q equivalentQuery = match.getKey().query();
            GraqlTraversal traversal = match.getValue().traversal();
            Unifier unifier = equivalentQuery.getUnifier(query);
            Map<ConceptId, ConceptId> conceptMap = new HashMap<>();
            equivalentQuery.getAtoms(IdPredicate.class)
                    .forEach(p -> {
                        Collection<Var> vars = unifier.get(p.getVarName());
                        Var var = !vars.isEmpty()? Iterators.getOnlyElement(vars.iterator()) : p.getVarName();
                        IdPredicate p2 = query.getIdPredicate(var);
                        if ( p2 != null){
                            conceptMap.put(p.getPredicate(), p2.getPredicate());
                        }
                    });
            return new Pair<>(
                    new StructuralCacheEntry<>(equivalentQuery, traversal.transform(conceptMap)),
                    new Pair<>(unifier, conceptMap)
            );
        }

        StructuralCacheEntry<Q> newEntry = new StructuralCacheEntry<>(
                query,
                GreedyTraversalPlan.createTraversal(query.getPattern(), query.tx())
        );
        structCache.put(structQuery, new Pair<>(structQuery, newEntry));
        return new Pair<>(newEntry, new Pair<>(new UnifierImpl(), new HashMap<>()));
    }
}
