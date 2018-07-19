/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.graql.internal.reasoner.cache;

import ai.grakn.concept.Rule;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.rule.InferenceRule;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

/**
 * TODO
 */
public class RuleCache {
    private final Set<Rule> fruitlessRules = new HashSet<>();
    private final Set<Rule> checkedRules = new HashSet<>();

    public Stream<InferenceRule> getApplicableRules(Atom atom){
        return atom.getApplicableRules()
                .filter( r -> {
                    if (fruitlessRules.contains(r.getRule())) return false;
                    if (checkedRules.contains(r.getRule())) return true;
                    if (r.getBody().isRuleResolvable()) return true;
                    boolean fruitless = !r.getBody().getQuery().stream().findFirst().isPresent();
                    if (fruitless) {
                        fruitlessRules.add(r.getRule());
                        return false;
                    }
                    checkedRules.add(r.getRule());
                    return true;
                });

    }
}
