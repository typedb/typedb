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

import hypergraph.graph.vertex.ThingVertex;
import hypergraph.graph.vertex.TypeVertex;
import hypergraph.graph.vertex.Vertex;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class Buffer {

    private final KeyGenerator keyGenerator;
    private final Set<TypeVertex> types;
    private final Set<ThingVertex> things;

    Buffer() {
        keyGenerator = new KeyGenerator(Schema.Key.BUFFERED);
        types = Collections.synchronizedSet(new HashSet<>());
        things = Collections.synchronizedSet(new HashSet<>());
    }

    KeyGenerator keyGenerator() {
        return keyGenerator;
    }

    void add(Vertex vertex) {
        if (vertex instanceof TypeVertex) {
            add((TypeVertex) vertex);
        } else if (vertex instanceof ThingVertex) {
            add((ThingVertex) vertex);
        }
    }

    void add(TypeVertex vertex) {
        types.add(vertex);
    }

    void add(ThingVertex vertex) {
        things.add(vertex);
    }

    Set<TypeVertex> typeVertices() {
        return types;
    }

    Set<ThingVertex> thingVertices() {
        return things;
    }

}
