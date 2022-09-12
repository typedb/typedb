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
