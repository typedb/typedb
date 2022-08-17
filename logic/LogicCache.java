/*
 * Copyright (C) 2022 Vaticle
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

    private final CommonCache<GraphTraversal.Type, Optional<Map<Identifier.Variable.Retrievable, Set<Label>>>> typeInferenceCache;
    private final CommonCache<String, Rule> ruleCache;
    private final CommonCache<Concludable, Map<Rule, Set<Unifier>>> unifiers;

    public LogicCache() {
        this.ruleCache = new CommonCache<>();
        this.typeInferenceCache = new CommonCache<>();
        this.unifiers = new CommonCache<>();
    }

    public LogicCache(int size, int timeOutMinutes) {
        this.ruleCache = new CommonCache<>(size, timeOutMinutes);
        this.typeInferenceCache = new CommonCache<>(size, timeOutMinutes);
        this.unifiers = new CommonCache<>(size, timeOutMinutes);
    }

    public CommonCache<GraphTraversal.Type, Optional<Map<Identifier.Variable.Retrievable, Set<Label>>>> inference() {
        return typeInferenceCache;
    }

    CommonCache<String, Rule> rule() {
        return ruleCache;
    }

    CommonCache<Concludable, Map<Rule, Set<Unifier>>> unifiers(){
        return unifiers;
    }
}
