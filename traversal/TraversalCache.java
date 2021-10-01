/*
 * Copyright (C) 2021 Vaticle
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

import com.vaticle.typedb.core.common.cache.CommonCache;
import com.vaticle.typedb.core.traversal.planner.Planner;
import com.vaticle.typedb.core.traversal.structure.Structure;

import java.util.Map;
import java.util.function.Function;

public class TraversalCache {

    private final CommonCache<Structure, Planner> activePlanners;
    private final CommonCache<Structure, Planner> optimalPlanners;

    public TraversalCache() {
        activePlanners = new CommonCache<>(30);
        optimalPlanners = new CommonCache<>(10_000);
    }

    public Planner getPlanner(Structure structure, Function<Structure, Planner> constructor) {
        Planner planner = optimalPlanners.getIfPresent(structure);
        if (planner != null) return planner;
        return activePlanners.get(structure, constructor);
    }

    public void mayUpdatePlanners(Map<Structure, Planner> planners) {
        planners.forEach((structure, planner) -> {
            if (planner.isOptimal() && optimalPlanners.getIfPresent(structure) == null) {
                optimalPlanners.put(structure, planner);
                activePlanners.invalidate(structure);
            } else if (!planner.isOptimal() && activePlanners.getIfPresent(structure) == null) {
                activePlanners.put(structure, planner);
                optimalPlanners.invalidate(structure);
            }
        });
    }
}
