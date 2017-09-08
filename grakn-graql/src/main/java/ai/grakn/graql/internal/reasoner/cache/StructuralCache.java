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
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.query.ReasonerAtomicQuery;
import ai.grakn.graql.internal.reasoner.query.ReasonerStructuralQuery;
import ai.grakn.graql.internal.reasoner.rule.InferenceRule;
import ai.grakn.graql.internal.reasoner.rule.RuleUtil;
import ai.grakn.graql.internal.reasoner.utils.Pair;
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
public class StructuralCache{

    private final Map<ReasonerStructuralQuery<ReasonerAtomicQuery>, Pair<ReasonerStructuralQuery<ReasonerAtomicQuery>, StructuralCacheEntry>> structCache;

    public StructuralCache(){
        this.structCache = new HashMap<>();
    }

    public Set<InferenceRule> getApplicableRules(ReasonerAtomicQuery q){
        ReasonerStructuralQuery<ReasonerAtomicQuery> structQuery = new ReasonerStructuralQuery<>(q);
        Pair<ReasonerStructuralQuery<ReasonerAtomicQuery>, StructuralCacheEntry> match = structCache.get(structQuery);
        if (match != null) return match.getValue().getRules();

        Atom atom = q.getAtom();
        GraknTx tx = q.tx();
        Set<InferenceRule> applicableRules = RuleUtil.getRulesWithType(atom.getSchemaConcept(), tx)
                .map(rule -> new InferenceRule(rule, tx))
                .filter(atom::isRuleApplicable)
                .map(r -> r.rewriteToUserDefined(atom))
                .collect(Collectors.toSet());

        structCache.put(structQuery, new Pair<>(structQuery, new StructuralCacheEntry(applicableRules)));
        return applicableRules;
    }
}
