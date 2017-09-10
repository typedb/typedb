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

import ai.grakn.GraknTx;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.internal.gremlin.GraqlTraversal;
import ai.grakn.graql.internal.gremlin.GreedyTraversalPlan;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.query.QueryAnswers;
import ai.grakn.graql.internal.reasoner.query.ReasonerAtomicQuery;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueries;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueryImpl;
import ai.grakn.graql.internal.reasoner.query.ReasonerStructuralQuery;
import ai.grakn.graql.internal.reasoner.rule.InferenceRule;
import ai.grakn.graql.internal.reasoner.rule.RuleUtil;
import ai.grakn.graql.internal.reasoner.utils.Pair;
import java.util.Collections;
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
 * @author Kasper Piskorski
 *
 */
public class StructuralCache<Q extends ReasonerQueryImpl>{

    private final Map<ReasonerStructuralQuery<Q>, Pair<ReasonerStructuralQuery<Q>, StructuralCacheEntry>> structCache;

    public StructuralCache(){
        this.structCache = new HashMap<>();
    }

    public StructuralCacheEntry get(Q query){
        ReasonerStructuralQuery<Q> structQuery = new ReasonerStructuralQuery<>(query);

        Pair<ReasonerStructuralQuery<Q>, StructuralCacheEntry> match = structCache.get(structQuery);
        if (match != null){
            Q equivalentQuery = match.getKey().query();
            GraqlTraversal traversal = match.getValue().getTraversal();
            Unifier unifier = equivalentQuery.getUnifier(query);
            return new StructuralCacheEntry(traversal.transform(query, unifier), match.getValue().getRules());
        }

        StructuralCacheEntry newEntry = new StructuralCacheEntry(
                GreedyTraversalPlan.createTraversal(query.getPattern(), query.tx()),
                query.getAtoms(Atom.class).flatMap(Atom::getApplicableRules).collect(Collectors.toSet())
        );
        structCache.put(structQuery, new Pair<>(structQuery, newEntry));
        return newEntry;
    }

    public Set<InferenceRule> getApplicableRules(Q q){
        ReasonerStructuralQuery<Q> structQuery = new ReasonerStructuralQuery<>(q);

        Pair<ReasonerStructuralQuery<Q>, StructuralCacheEntry> match = structCache.get(structQuery);
        if (match != null) return match.getValue().getRules();

        return Collections.emptySet();
    }

    public boolean isRuleResolvable(Q q){
        return !getApplicableRules(q).isEmpty();
    }

    /*
    public boolean isRuleResolvable(Atom atom) {
        return isRuleResolvable(ReasonerQueries.create(Collections.singletonList(atom), atom.getParentQuery().tx()));
    }
    */
}
