/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.traversal;

import com.vaticle.typedb.common.collection.Pair;
import com.vaticle.typedb.core.common.cache.CommonCache;
import com.vaticle.typedb.core.traversal.common.Modifiers;
import com.vaticle.typedb.core.traversal.planner.Planner;
import com.vaticle.typedb.core.traversal.structure.Structure;

import java.util.function.Function;

public class TraversalCache {

    private final CommonCache<Pair<Structure, Modifiers>, Planner> activePlanners;
    private final CommonCache<Pair<Structure, Modifiers>, Planner> optimalPlanners;

    public TraversalCache() {
        activePlanners = new CommonCache<>(30);
        optimalPlanners = new CommonCache<>(10_000);
    }

    public Planner getPlanner(Structure structure, Modifiers modifiers, Function<Pair<Structure, Modifiers>, Planner> constructor) {
        Pair<Structure, Modifiers> key = new Pair<>(structure, modifiers);
        Planner planner = optimalPlanners.getIfPresent(key);
        if (planner != null) return planner;
        return activePlanners.get(key, constructor);
    }

    public void mayUpdatePlanner(Structure structure, Modifiers modifiers, Planner planner) {
        Pair<Structure, Modifiers> key = new Pair<>(structure, modifiers);
        if (planner.isOptimal() && optimalPlanners.getIfPresent(key) == null) {
            optimalPlanners.put(key, planner);
            activePlanners.invalidate(key);
        } else if (!planner.isOptimal() && activePlanners.getIfPresent(key) == null) {
            activePlanners.put(key, planner);
            optimalPlanners.invalidate(key);
        }
    }
}
