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

import grakn.core.common.parameters.Label;
import grakn.core.graph.util.Encoding;
import grakn.core.traversal.common.Identifier;
import grakn.core.traversal.common.Predicate;
import grakn.core.traversal.graph.TraversalVertex;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class Structure {

    // TODO: create vertex properties first, then the vertex itself, then edges
    //       that way, we can make properties to be 'final' objects that are
    //       included in equality and hashCode of vertices
    final Map<Identifier.Variable, TraversalVertex.Properties> properties;
    private final Map<Identifier, StructureVertex<?>> vertices;
    private final Set<StructureEdge<?, ?>> edges;
    private int generatedIdentifierCount;
    private List<Structure> structures;

    public Structure() {
        vertices = new HashMap<>();
        properties = new HashMap<>();
        edges = new HashSet<>();
        generatedIdentifierCount = 0;
    }

    public StructureVertex.Thing thingVertex(Identifier identifier) {
        return vertices.computeIfAbsent(identifier, id -> {
            StructureVertex.Thing v = new StructureVertex.Thing(this, id);
            // TODO: remove this with this.properties
            if (id.isVariable()) properties.put(id.asVariable(), v.props());
            return v;
        }).asThing();
    }

    public StructureVertex.Type typeVertex(Identifier identifier) {
        return vertices.computeIfAbsent(identifier, id -> {
            StructureVertex.Type v = new StructureVertex.Type(this, id);
            // TODO: remove this with this.properties
            if (id.isVariable()) properties.put(id.asVariable(), v.props());
            return v;
        }).asType();
    }

    public Identifier.Scoped newIdentifier(Identifier.Variable scope) {
        return Identifier.Scoped.of(scope, generatedIdentifierCount++);
    }

    public Collection<StructureVertex<?>> vertices() {
        return vertices.values();
    }

    public Set<StructureEdge<?, ?>> edges() {
        return edges;
    }

    public void equalEdge(StructureVertex<?> from, StructureVertex<?> to) {
        StructureEdge.Equal edge = new StructureEdge.Equal(from, to);
        edges.add(edge);
        from.out(edge);
        to.in(edge);
    }

    public void predicateEdge(StructureVertex.Thing from, StructureVertex.Thing to, Predicate.Variable predicate) {
        StructureEdge.Predicate edge = new StructureEdge.Predicate(from, to, predicate);
        edges.add(edge);
        from.out(edge);
        to.in(edge);
    }

    public void nativeEdge(StructureVertex<?> from, StructureVertex<?> to, Encoding.Edge encoding) {
        nativeEdge(from, to, encoding, false);
    }

    public void nativeEdge(StructureVertex<?> from, StructureVertex<?> to, Encoding.Edge encoding, boolean isTransitive) {
        StructureEdge.Native<?, ?> edge = new StructureEdge.Native<>(from, to, encoding, isTransitive);
        edges.add(edge);
        from.out(edge);
        to.in(edge);
    }

    public void optimisedEdge(StructureVertex.Thing from, StructureVertex.Thing to, Encoding.Edge.Thing encoding) {
        optimisedEdge(from, to, encoding, new HashSet<>());
    }

    public void optimisedEdge(StructureVertex.Thing from, StructureVertex.Thing to, Encoding.Edge encoding, Set<Label> types) {
        StructureEdge.Native.Optimised edge = new StructureEdge.Native.Optimised(from, to, encoding, types);
        edges.add(edge);
        from.out(edge);
        to.in(edge);
    }

    public List<Structure> asGraphs() {
        if (structures == null) {
            structures = new ArrayList<>();
            while (!vertices.isEmpty()) {
                Structure newStructure = new Structure();
                splitGraph(vertices.values().iterator().next(), newStructure);
                if (newStructure.vertices().size() > 1 ||
                        newStructure.vertices().iterator().next().id().isNamedReference()) {
                    structures.add(newStructure);
                }
            }
        }
        return structures;
    }

    private void splitGraph(StructureVertex<?> vertex, Structure newStructure) {
        if (!vertices.containsKey(vertex.id())) return;

        this.vertices.remove(vertex.id());
        newStructure.vertices.put(vertex.id(), vertex);
        // TODO: remove this with this.properties
        if (vertex.id().isVariable() && this.properties.containsKey(vertex.id().asVariable())) {
            TraversalVertex.Properties props = this.properties.remove(vertex.id().asVariable());
            newStructure.properties.put(vertex.id().asVariable(), props);
        }
        List<StructureVertex<?>> adjacents = new ArrayList<>();
        vertex.outs().forEach(outgoing -> {
            if (this.edges.contains(outgoing)) {
                this.edges.remove(outgoing);
                newStructure.edges.add(outgoing);
                adjacents.add(outgoing.to());
            }
        });
        vertex.ins().forEach(incoming -> {
            if (this.edges.contains(incoming)) {
                this.edges.remove(incoming);
                newStructure.edges.add(incoming);
                adjacents.add(incoming.from());
            }
        });
        adjacents.forEach(v -> splitGraph(v, newStructure));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        else if (o == null || getClass() != o.getClass()) return false;

        Structure that = (Structure) o;
        return (this.vertices.equals(that.vertices) &&
                this.properties.equals(that.properties) &&
                this.edges.equals(that.edges));
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.vertices, this.properties, this.edges);
    }
}
