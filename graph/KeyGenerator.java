/*
 * Copyright (C) 2020 Grakn Labs
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

package hypergraph.graph;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class KeyGenerator {

    private final Map<Schema.Vertex.Type.Root, AtomicInteger> typeKeys;
    private final Map<Schema.Vertex.Type, AtomicLong> thingKeys;

    public KeyGenerator() {
        typeKeys = new ConcurrentHashMap<>();
        thingKeys = new ConcurrentHashMap<>();
    }

    public short forType(Schema.Vertex.Type.Root root) {
        if (typeKeys.containsKey(root)) {
            return (short) typeKeys.get(root).getAndIncrement();
        } else {
            AtomicInteger zero = new AtomicInteger(0);
            typeKeys.put(root, zero);
            return (short) zero.getAndIncrement();
        }
    }

    public long forThing(Schema.Vertex.Type type) {
        if (thingKeys.containsKey(type)) {
            return thingKeys.get(type).getAndIncrement();
        } else {
            AtomicLong zero = new AtomicLong(0);
            thingKeys.put(type, zero);
            return zero.getAndIncrement();
        }
    }
}
