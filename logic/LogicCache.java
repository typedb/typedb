/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.logic;

import com.vaticle.typedb.core.common.cache.CommonCache;
import com.vaticle.typedb.core.common.parameters.Label;
import com.vaticle.typedb.core.logic.resolvable.Concludable;
import com.vaticle.typedb.core.logic.resolvable.Unifier;
import com.vaticle.typedb.core.traversal.GraphTraversal;
import com.vaticle.typedb.core.traversal.common.Identifier;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class LogicCache {

    private final CommonCache<String, Rule> ruleCache;
    private final CommonCache<Concludable, Map<Rule, Set<Unifier>>> unifiers;
    private final CommonCache<GraphTraversal.Type, Optional<Map<Identifier.Variable.Retrievable, Set<Label>>>> typeInferenceCache;
    private final CommonCache<GraphTraversal.Type, Boolean> queryCoherenceCache;

    public LogicCache() {
        this.ruleCache = new CommonCache<>();
        this.unifiers = new CommonCache<>();
        this.typeInferenceCache = new CommonCache<>();
        this.queryCoherenceCache = new CommonCache<>();
    }

    public LogicCache(int size, int timeOutMinutes) {
        this.ruleCache = new CommonCache<>(size, timeOutMinutes);
        this.unifiers = new CommonCache<>(size, timeOutMinutes);
        this.typeInferenceCache = new CommonCache<>(size, timeOutMinutes);
        this.queryCoherenceCache = new CommonCache<>(size, timeOutMinutes);
    }

    public CommonCache<GraphTraversal.Type, Optional<Map<Identifier.Variable.Retrievable, Set<Label>>>> typeInference() {
        return typeInferenceCache;
    }

    public CommonCache<GraphTraversal.Type, Boolean> queryCoherence() {
        return queryCoherenceCache;
    }

    CommonCache<String, Rule> rule() {
        return ruleCache;
    }

    CommonCache<Concludable, Map<Rule, Set<Unifier>>> unifiers(){
        return unifiers;
    }
}
