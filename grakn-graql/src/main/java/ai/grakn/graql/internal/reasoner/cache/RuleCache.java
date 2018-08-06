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

package ai.grakn.graql.internal.reasoner.cache;

import ai.grakn.concept.Rule;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.rule.InferenceRule;
import ai.grakn.graql.internal.reasoner.utils.Pair;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Introduces rule cache that wraps around the atom matching rule retrieval to ensure resolution of fruitless rules is not pursued.
 *
 * @author Kasper Piskorski
 *
 */
public class RuleCache {
    private final Set<Rule> fruitlessRules = new HashSet<>();
    private final Set<Rule> checkedRules = new HashSet<>();

    /**
     * @param atom of interest
     * @return stream of rules applicable to this atom
     */
    public Stream<InferenceRule> getApplicableRules(Atom atom){
        return atom.getApplicableRules()
                .filter( r -> {
                    if (fruitlessRules.contains(r.getRule())) return false;
                    if (r.getBody().isRuleResolvable() || checkedRules.contains(r.getRule())) return true;
                    boolean fruitless = !r.getBody().getQuery().stream().findFirst().isPresent();
                    if (fruitless) {
                        fruitlessRules.add(r.getRule());
                        return false;
                    }
                    checkedRules.add(r.getRule());
                    return true;
                });
    }

    /**
     * @param atom of interest
     * @return stream of all rules applicable to this atom including permuted cases when the role types are meta roles
     */
    public Stream<Pair<InferenceRule, Unifier>> getRuleStream(Atom atom){
        return getApplicableRules(atom)
                .flatMap(r -> r.getMultiUnifier(atom).stream().map(unifier -> new Pair<>(r, unifier)))
                .sorted(Comparator.comparing(rt -> -rt.getKey().resolutionPriority()));
    }
}
