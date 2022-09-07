/*
 * Copyright (C) 2022 Vaticle
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

package com.vaticle.typedb.core.traversal.structure;

import com.vaticle.typedb.core.common.parameters.Label;
import com.vaticle.typedb.core.graph.common.Encoding;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typedb.core.traversal.graph.TraversalVertex;
import com.vaticle.typedb.core.traversal.predicate.Predicate;

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
    private List<Structure> structures;

    public Structure() {
        vertices = new HashMap<>();
        properties = new HashMap<>();
        edges = new HashSet<>();
    }

    public StructureVertex.Thing thingVertex(Identifier identifier) {
        return vertices.computeIfAbsent(identifier, id -> {
            StructureVertex.Thing v = new StructureVertex.Thing(id);
            // TODO: remove this with this.properties
            if (id.isVariable()) properties.put(id.asVariable(), v.props());
            return v;
        }).asThing();
    }

    public StructureVertex.Type typeVertex(Identifier identifier) {
        return vertices.computeIfAbsent(identifier, id -> {
            StructureVertex.Type v = new StructureVertex.Type(id);
            // TODO: remove this with this.properties
            if (id.isVariable()) properties.put(id.asVariable(), v.props());
            return v;
        }).asType();
    }

    public Collection<StructureVertex<?>> vertices() {
        return vertices.values();
    }

    public Set<StructureEdge<?, ?>> edges() {
        return edges;
    }

    public void equalEdge(StructureVertex<?> from, StructureVertex<?> to) {
        StructureEdge.Equal edge = new StructureEdge.Equal(from, to);
        recordEdge(edge);
    }

    public void predicateEdge(StructureVertex.Thing from, StructureVertex.Thing to, Predicate.Variable predicate) {
        StructureEdge.Predicate edge = new StructureEdge.Predicate(from, to, predicate);
        recordEdge(edge);
    }

    public void nativeEdge(StructureVertex<?> from, StructureVertex<?> to, Encoding.Edge encoding) {
        nativeEdge(from, to, encoding, false);
    }

    public void nativeEdge(StructureVertex<?> from, StructureVertex<?> to, Encoding.Edge encoding, boolean isTransitive) {
        StructureEdge.Native<?, ?> edge = new StructureEdge.Native<>(from, to, encoding, isTransitive);
        recordEdge(edge);
    }

    public void rolePlayer(StructureVertex.Thing from, StructureVertex.Thing to, Set<Label> roleTypes, int repetition) {
        StructureEdge.Native.RolePlayer edge = new StructureEdge.Native.RolePlayer(from, to, roleTypes, repetition);
        recordEdge(edge);
    }

    private void recordEdge(StructureEdge<?, ?> edge) {
        edges.add(edge);
        if (edge.from().equals(edge.to())) edge.from().loop(edge);
        else {
            edge.from().out(edge);
            edge.to().in(edge);
        }
    }
    public List<Structure> asGraphs() {
        if (structures == null) {
            structures = new ArrayList<>();
            Set<StructureVertex<?>> verticesToVisit = new HashSet<>(this.vertices.values());
            Set<StructureEdge<?, ?>> edgesToVisit = new HashSet<>(this.edges);
            while (!verticesToVisit.isEmpty()) {
                Structure newStructure = new Structure();
                splitGraph(verticesToVisit.iterator().next(), newStructure, verticesToVisit, edgesToVisit);
                if (newStructure.vertices().size() > 1 || newStructure.vertices().iterator().next().id().isRetrievable()) {
                    structures.add(newStructure);
                }
            }
        }
        return structures;
    }

    private void splitGraph(StructureVertex<?> vertex, Structure newStructure,
                            Set<StructureVertex<?>> verticesToVisit, Set<StructureEdge<?, ?>> edgesToVisit) {
        if (!verticesToVisit.contains(vertex)) return;

        verticesToVisit.remove(vertex);
        newStructure.vertices.put(vertex.id(), vertex);
        TraversalVertex.Properties props;
        if (vertex.id().isVariable() && (props = this.properties.get(vertex.id().asVariable())) != null) {
            newStructure.properties.put(vertex.id().asVariable(), props);
        }
        List<StructureVertex<?>> adjacents = new ArrayList<>();
        vertex.outs().forEach(outgoing -> {
            if (edgesToVisit.contains(outgoing)) {
                edgesToVisit.remove(outgoing);
                newStructure.edges.add(outgoing);
                adjacents.add(outgoing.to());
            }
        });
        vertex.ins().forEach(incoming -> {
            if (edgesToVisit.contains(incoming)) {
                edgesToVisit.remove(incoming);
                newStructure.edges.add(incoming);
                adjacents.add(incoming.from());
            }
        });
        vertex.loops().forEach(loop -> {
            if (edgesToVisit.contains(loop)) {
                edgesToVisit.remove(loop);
                newStructure.edges.add(loop);
            }
        });
        adjacents.forEach(v -> splitGraph(v, newStructure, verticesToVisit, edgesToVisit));
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
