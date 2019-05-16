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

package grakn.core.server.statistics;

import java.util.HashMap;

public class UncomittedStatisticsDelta {

    private HashMap<String, Long> instanceDeltas;
    public UncomittedStatisticsDelta() {
        instanceDeltas = new HashMap<>();
    }

    public long delta(String label) {
        return instanceDeltas.getOrDefault(label, 0L);
    }

    public void increment(String label) {
        Long currentCount = instanceDeltas.getOrDefault(label, 0L);
        instanceDeltas.put(label, currentCount + 1);
    }

    public void decrement(String label) {
        Long currentCount = instanceDeltas.getOrDefault(label, 0L);
        instanceDeltas.put(label, currentCount - 1);
    }
}
