/*
 * Copyright (C) 2021 Vaticle
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

package com.vaticle.typedb.core.traversal.procedure;

import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typedb.core.traversal.structure.Structure;
import com.vaticle.typedb.core.traversal.structure.StructureEdge;
import com.vaticle.typedb.core.traversal.structure.StructureVertex;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

public class CombinationProcedure {

    private final Map<Identifier, ProcedureVertex.Type> vertices;
    private final Map<ProcedureVertex.Type, Set<ProcedureEdge<?, ?>>> forwardEdges;
    private final Map<ProcedureVertex.Type, Set<ProcedureEdge<?, ?>>> reverseEdges;
    private final Map<ProcedureVertex.Type, Set<ProcedureEdge<?, ?>>> loopEdges;
    private ProcedureVertex.Type startVertex;
    private Set<ProcedureVertex.Type> terminals;

    CombinationProcedure() {
        this.vertices = new HashMap<>();
        this.forwardEdges = new HashMap<>();
        this.reverseEdges = new HashMap<>();
        this.loopEdges = new HashMap<>();
    }

    public static CombinationProcedure create(Structure structure) {
        StructureVertex.Type startVertex = structure.vertices().iterator().next().asType();
        CombinationProcedure procedure = new CombinationProcedure();
        procedure.registerBFS(startVertex);
        return procedure;
    }

    public ProcedureVertex.Type startVertex() {
        return startVertex;
    }

    public Collection<ProcedureVertex.Type> vertices() {
        return vertices.values();
    }

    public boolean isTerminal(ProcedureVertex.Type vertex) {
        return terminals().contains(vertex);
    }

    public Set<ProcedureVertex.Type> terminals() {
        if (terminals == null) {
            terminals = new HashSet<>(reverseEdges.keySet());
            terminals.removeAll(forwardEdges.keySet());
        }
        return terminals;
    }

    public Set<ProcedureEdge<?, ?>> forwardEdges(ProcedureVertex.Type vertex) {
        return forwardEdges.computeIfAbsent(vertex, (v) -> new HashSet<>());
    }

    public Set<ProcedureEdge<?, ?>> reverseEdges(ProcedureVertex.Type vertex) {
        return reverseEdges.computeIfAbsent(vertex, (v) -> new HashSet<>());
    }

    public Set<ProcedureEdge<?, ?>> loopEdges(ProcedureVertex.Type vertex) {
        return loopEdges.get(vertex);
    }

    private void registerBFS(StructureVertex.Type start) {
        Set<StructureEdge<?, ?>> visitedEdges = new HashSet<>();
        Queue<StructureVertex.Type> queue = new LinkedList<>();
        queue.add(start);
        StructureVertex.Type vertex;
        while (!queue.isEmpty()) {
            vertex = queue.remove();
            ProcedureVertex.Type procedureVertex;
            if (vertices.isEmpty()) procedureVertex = registerVertex(vertex, true);
            else procedureVertex = registerVertex(vertex);
            queue.addAll(registerOutEdges(procedureVertex, vertex.outs(), visitedEdges));
            queue.addAll(registerInEdges(procedureVertex, vertex.ins(), visitedEdges));
            registerLoopEdges(procedureVertex, vertex.loops(), visitedEdges);
        }
    }

    private ProcedureVertex.Type registerVertex(StructureVertex.Type vertex) {
        return registerVertex(vertex, false);
    }

    private ProcedureVertex.Type registerVertex(StructureVertex.Type vertex, boolean isStart) {
        ProcedureVertex.Type procedureVertex = vertices.computeIfAbsent(vertex.id(), id -> {
            ProcedureVertex.Type v = new ProcedureVertex.Type(id);
            v.props(vertex.props());
            return v;
        });
        if (isStart) startVertex = procedureVertex;
        return procedureVertex;
    }

    private Set<StructureVertex.Type> registerOutEdges(ProcedureVertex.Type from, Set<StructureEdge<?, ?>> outs,
                                                       Set<StructureEdge<?, ?>> visitedEdges) {
        Set<StructureVertex.Type> nextVertices = new HashSet<>();
        outs.forEach(structureEdge -> {
            if (!visitedEdges.contains(structureEdge)) {
                visitedEdges.add(structureEdge);
                ProcedureVertex.Type to = registerVertex(structureEdge.to().asType());
                ProcedureEdge<?, ?> edge = createOut(from, to, structureEdge);
                forwardEdges.computeIfAbsent(from, (v) -> new HashSet<>()).add(edge);
                reverseEdges.computeIfAbsent(to, (v) -> new HashSet<>()).add(edge.reverse());
                nextVertices.add(structureEdge.to().asType());
            }
        });
        return nextVertices;
    }

    private Set<StructureVertex.Type> registerInEdges(ProcedureVertex.Type to, Set<StructureEdge<?, ?>> ins,
                                                      Set<StructureEdge<?, ?>> visitedEdges) {
        Set<StructureVertex.Type> nextVertices = new HashSet<>();
        ins.forEach(structureEdge -> {
            if (!visitedEdges.contains(structureEdge)) {
                visitedEdges.add(structureEdge);
                ProcedureVertex.Type from = registerVertex(structureEdge.from().asType());
                ProcedureEdge<?, ?> edge = createIn(to, from, structureEdge);
                forwardEdges.computeIfAbsent(to, (v) -> new HashSet<>()).add(edge);
                reverseEdges.computeIfAbsent(from, (v) -> new HashSet<>()).add(edge.reverse());
                nextVertices.add(structureEdge.from().asType());
            }
        });
        return nextVertices;
    }

    private void registerLoopEdges(ProcedureVertex.Type from, Set<StructureEdge<?, ?>> loops,
                                   Set<StructureEdge<?, ?>> visitedEdges) {
        loops.forEach(structureEdge -> {
            if (!visitedEdges.contains(structureEdge)) {
                visitedEdges.add(structureEdge);
                ProcedureEdge<?, ?> edge = createLoop(from, structureEdge);
                loopEdges.computeIfAbsent(from, (v) -> new HashSet<>()).add(edge);
            }
        });
    }

    private ProcedureEdge<?, ?> createOut(ProcedureVertex.Type from, ProcedureVertex.Type to,
                                          StructureEdge<?, ?> structureEdge) {
        ProcedureEdge<?, ?> edge = ProcedureEdge.of(from, to, structureEdge, -1, true);
        registerEdge(edge);
        return edge;
    }

    private ProcedureEdge<?, ?> createIn(ProcedureVertex.Type from, ProcedureVertex.Type to,
                                         StructureEdge<?, ?> structureEdge) {
        ProcedureEdge<?, ?> edge = ProcedureEdge.of(from, to, structureEdge, -1, false);
        registerEdge(edge);
        return edge;
    }

    private ProcedureEdge<?, ?> createLoop(ProcedureVertex.Type from, StructureEdge<?, ?> structureEdge) {
        ProcedureEdge<?, ?> edge = ProcedureEdge.of(from, from, structureEdge, -1, true);
        registerEdge(edge);
        return edge;
    }

    private void registerEdge(ProcedureEdge<?, ?> edge) {
        if (edge.from().equals(edge.to())) {
            edge.from().loop(edge);
        } else {
            edge.from().out(edge);
            edge.to().in(edge);
        }
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder();
        str.append("Combination Procedure: {");
        str.append("\n\tvertices:");
        for (ProcedureVertex<?, ?> v : vertices()) {
            str.append("\n\t\t").append(v);
        }
        str.append("\n\tedges:");
        forwardEdges.values().stream().flatMap(Collection::stream).sorted(Comparator.comparing(ProcedureEdge::order))
                .forEachOrdered(edge -> str.append("\n\t\t").append(edge));
        loopEdges.values().stream().flatMap(Collection::stream).sorted(Comparator.comparing(ProcedureEdge::order))
                .forEachOrdered(edge -> str.append("\n\t\t").append(edge));
        str.append("\n}");
        return str.toString();
    }
}
