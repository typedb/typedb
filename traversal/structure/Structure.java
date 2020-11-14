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
import java.util.Collection;
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
    private List<Structure> structures;

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

    public Collection<StructureVertex> vertices() {
        return vertices.values();
    }

    public void edge(Identifier from, Identifier to) {
        edge(new StructureEdge.Property.Equal(), from, to);
    }

    public void edge(Encoding.Edge encoding, Identifier from, Identifier to) {
        edge(new StructureEdge.Property.Type(encoding), from, to);
    }

    public void edge(Encoding.Edge encoding, Identifier from, Identifier to, boolean isTransitive) {
        edge(new StructureEdge.Property.Type(encoding, isTransitive), from, to);
    }

    public void edge(Encoding.Edge encoding, Identifier from, Identifier to, String[] labels) {
        edge(new StructureEdge.Property.Type(encoding, labels), from, to);
    }

    public void edge(GraqlToken.Comparator.Equality comparator, Identifier from, Identifier to) {
        edge(new StructureEdge.Property.Comparator(comparator), from, to);
    }

    private void edge(StructureEdge.Property type, Identifier from, Identifier to) {
        StructureVertex fromVertex = vertex(from);
        StructureVertex toVertex = vertex(to);
        StructureEdge edge = new StructureEdge(type, fromVertex, toVertex);
        edges.add(edge);
        fromVertex.out(edge);
        toVertex.in(edge);
    }

    public List<Structure> asGraphs() {
        if (structures == null) {
            structures = new ArrayList<>();
            while (!vertices.isEmpty()) {
                Structure newPattern = new Structure();
                splitGraph(vertices.values().iterator().next(), newPattern);
                structures.add(newPattern);
            }
        }
        return structures;
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
