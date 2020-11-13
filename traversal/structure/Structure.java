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

package grakn.core.traversal.structure;

import grakn.core.graph.util.Encoding;
import grakn.core.traversal.Identifier;
import graql.lang.common.GraqlToken;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class Structure {

    private final Map<Identifier, StructureVertex> vertices;
    private final Set<StructureEdge> edges;
    private int generatedIdentifierCount;
    private List<Structure> patterns;

    public Structure() {
        vertices = new HashMap<>();
        edges = new HashSet<>();
        generatedIdentifierCount = 0;
    }

    public StructureVertex vertex(Identifier identifier) {
        return vertices.computeIfAbsent(identifier, i -> new StructureVertex(i, this));
    }

    public Identifier.Generated newIdentifier() {
        return Identifier.Generated.of(generatedIdentifierCount++);
    }

    public void edge(Identifier from, Identifier to) {
        edge(new StructureEdge.Type.Equal(), from, to, false, new String[]{});
    }

    public void edge(Encoding.Edge encoding, Identifier from, Identifier to) {
        edge(new StructureEdge.Type.Encoded(encoding), from, to, false, new String[]{});
    }

    public void edge(Encoding.Edge encoding, Identifier from, Identifier to, boolean isTransitive) {
        edge(new StructureEdge.Type.Encoded(encoding), from, to, isTransitive, new String[]{});
    }

    public void edge(Encoding.Edge encoding, Identifier from, Identifier to, String[] labels) {
        edge(new StructureEdge.Type.Encoded(encoding), from, to, false, labels);
    }

    public void edge(GraqlToken.Comparator.Equality comparator, Identifier from, Identifier to) {
        edge(new StructureEdge.Type.Comparator(comparator), from, to, false, new String[]{});
    }

    private void edge(StructureEdge.Type type, Identifier from, Identifier to, boolean isTransitive, String[] labels) {
        StructureVertex fromVertex = vertex(from);
        StructureVertex toVertex = vertex(to);
        StructureEdge edge = new StructureEdge(type, fromVertex, toVertex, isTransitive, labels);
        edges.add(edge);
        fromVertex.out(edge);
        toVertex.in(edge);
    }

    public List<Structure> graphs() {
        if (patterns == null) {
            patterns = new ArrayList<>();
            while (!vertices.isEmpty()) {
                Structure newPattern = new Structure();
                splitGraph(vertices.values().iterator().next(), newPattern);
                patterns.add(newPattern);
            }
        }
        return patterns;
    }

    private void splitGraph(StructureVertex vertex, Structure newPattern) {
        if (!vertices.containsKey(vertex.identifier())) return;

        this.vertices.remove(vertex.identifier());
        newPattern.vertices.put(vertex.identifier(), vertex);
        vertex.outs().forEach(outgoing -> {
            if (this.edges.contains(outgoing)) {
                this.edges.remove(outgoing);
                newPattern.edges.add(outgoing);
                splitGraph(outgoing.to(), newPattern);
            }
        });
        vertex.ins().forEach(incoming -> {
            if (this.edges.contains(incoming)) {
                this.edges.remove(incoming);
                newPattern.edges.add(incoming);
                splitGraph(incoming.from(), newPattern);
            }
        });
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        else if (o == null || getClass() != o.getClass()) return false;

        Structure that = (Structure) o;
        return (this.vertices.equals(that.vertices) && this.edges.equals(that.edges));
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.vertices, this.edges);
    }
}
