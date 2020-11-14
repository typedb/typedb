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

import grakn.core.traversal.Identifier;
import grakn.core.traversal.property.EdgeProperty;

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

    public StructureVertex thingVertex(Identifier identifier) {
        return vertices.computeIfAbsent(identifier, i -> new StructureVertex.Thing(i, this));
    }

    public StructureVertex typeVertex(Identifier identifier) {
        return vertices.computeIfAbsent(identifier, i -> new StructureVertex.Type(i, this));
    }

    public Identifier.Generated newIdentifier() {
        return Identifier.Generated.of(generatedIdentifierCount++);
    }

    public Collection<StructureVertex> vertices() {
        return vertices.values();
    }

    public void edge(EdgeProperty property, StructureVertex from, StructureVertex to) {
        StructureEdge edge = new StructureEdge(property, from, to);
        edges.add(edge);
        from.out(edge);
        to.in(edge);
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
