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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class Buffer {

    private final KeyGenerator keyGenerator;
    private final Map<String, TypeVertex> typeIndex;
    private final Set<TypeVertex> typeVertices;
    private final Set<ThingVertex> thingVertices;

    Buffer() {
        keyGenerator = new KeyGenerator(Schema.Key.BUFFERED);
        typeIndex = new ConcurrentHashMap<>();
        typeVertices = new HashSet<>();
        thingVertices = new HashSet<>();
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
        typeVertices.add(vertex);
        typeIndex.put(vertex.label(), vertex);
    }

    void add(ThingVertex vertex) {
        thingVertices.add(vertex);
    }

    TypeVertex getTypeVertex(String label) {
        return typeIndex.get(label);
    }


    Set<TypeVertex> typeVertices() {
        return typeVertices;
    }

    Set<ThingVertex> thingVertices() {
        return thingVertices;
    }

}
