/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2019 Grakn Labs Ltd
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

package grakn.core.server.statistics;

import grakn.core.concept.Label;

import grakn.core.server.kb.Schema;
import java.util.HashMap;

/**
 * A transaction-bound tracker of created and deleted types. A simple implementation that increments and decrements
 * and doesn't need to be thread safe as transactions are currently bound to a single thread.
 *
 * Written into `KeyspaceStatistics` on commit
 */
public class UncomittedStatisticsDelta {

    private HashMap<Label, Long> instanceDeltas;
    public UncomittedStatisticsDelta() {
        instanceDeltas = new HashMap<>();
    }

    long delta(Label label) {
        return instanceDeltas.getOrDefault(label, 0L);
    }

    public void increment(Label label) {
        Long currentCount = instanceDeltas.getOrDefault(label, 0L);
        instanceDeltas.put(label, currentCount + 1);
    }

    public void decrement(Label label) {
        Long currentCount = instanceDeltas.getOrDefault(label, 0L);
        instanceDeltas.put(label, currentCount - 1);
    }

    HashMap<Label, Long> instanceDeltas() {
        return instanceDeltas;
    }

    /**
     * Updates the concept count for Thing. It overwrites the entry to be equal to the sum of contributions in the
     * delta map.
     */
    void updateThingCount(){
        // precompute the delta for all Thing's and update the delta map
        long thingDelta = instanceDeltas.values().stream()
                .reduce(Long::sum)
                .orElse(0L);
        if (thingDelta != 0) {
            Label thingLabel = Schema.MetaSchema.THING.getLabel();
            instanceDeltas.put(thingLabel, thingDelta);
        }
    }
}
