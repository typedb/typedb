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

import hypergraph.graph.edge.Edge;
import hypergraph.graph.vertex.Vertex;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

public class Buffer {

    private final KeyGenerator keyGenerator;
    private final Map<Vertex, Set<Edge>> adjacencyList;

    Buffer() {
        keyGenerator = new KeyGenerator(Schema.Key.BUFFERED);
        adjacencyList = new ConcurrentHashMap<>();
    }

    KeyGenerator keyGenerator() {
        return keyGenerator;
    }

    void put(Vertex vertex) {
        adjacencyList.putIfAbsent(vertex, Collections.synchronizedSet(new HashSet<>()));
    }

    void put(Edge edge) {
        adjacencyList.get(edge.from()).add(edge);
    }

    Stream<Vertex> vertices() {
        return adjacencyList.keySet().parallelStream();
    }

}
