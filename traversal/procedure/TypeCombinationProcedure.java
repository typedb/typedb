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

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.traversal.TypeTraversal;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typedb.core.traversal.structure.StructureEdge;
import com.vaticle.typedb.core.traversal.structure.StructureVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.UNRECOGNISED_VALUE;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public class TypeCombinationProcedure {

    private static final Logger LOG = LoggerFactory.getLogger(GraphProcedure.class);

    private final Map<Identifier, ProcedureVertex.Type> vertices;
    private final ProcedureEdge.Native.Type[] edges;

    TypeCombinationProcedure(int edgeSize) {
        vertices = new HashMap<>();
        edges = new ProcedureEdge.Native.Type[edgeSize];
    }

    public static TypeCombinationProcedure of(TypeTraversal traversal) {
        TypeCombinationProcedure procedure = new TypeCombinationProcedure(traversal.structure().edges().size());
        Set<StructureEdge<?, ?>> visitedEdges = new HashSet<>();
        procedure.registerDFS(traversal.structure().vertices().iterator().next().asType(), visitedEdges);
        return procedure;
    }

    public ProcedureVertex.Type vertex(Identifier identifier) {
        return vertices.get(identifier);
    }

    public int edgeSize() {
        return edges.length;
    }

    public ProcedureEdge.Native.Type edge(int pos) {
        return edges[pos - 1];
    }

    private ProcedureVertex.Type registerDFS(StructureVertex.Type structureVertex, Set<StructureEdge<?, ?>> visitedEdges) {
        if (vertices.containsKey(structureVertex.id())) return vertices.get(structureVertex.id());
        ProcedureVertex.Type vertex = vertices.computeIfAbsent(structureVertex.id(), id -> new ProcedureVertex.Type(id, vertices.size() == 1));
        vertex.props(structureVertex.props());
        structureVertex.outs().forEach(structureEdge -> {
            if (!visitedEdges.contains(structureEdge)) {
                visitedEdges.add(structureEdge);
                int order = visitedEdges.size();
                ProcedureVertex.Type end = registerDFS(structureEdge.to().asType(), visitedEdges);
                registerOut(vertex, end, structureEdge.asNative(), order);
            }
        });
        structureVertex.ins().forEach(structureEdge -> {
            if (!visitedEdges.contains(structureEdge)) {
                visitedEdges.add(structureEdge);
                int order = visitedEdges.size();
                ProcedureVertex.Type start = registerDFS(structureEdge.from().asType(), visitedEdges);
                registerIn(vertex, start, structureEdge.asNative(), order);
            }
        });
        return vertex;
    }

    private void registerOut(ProcedureVertex.Type from, ProcedureVertex.Type to, StructureEdge.Native<?, ?> structureEdge, int order) {
        // TODO are these all forward, from -> to?
        ProcedureEdge.Native.Type edge;
        switch (structureEdge.encoding().asType()) {
            case SUB:
                edge = new ProcedureEdge.Native.Type.Sub.Forward(from.asType(), to.asType(), order, structureEdge.isTransitive());
                break;
            case OWNS:
                edge =  new ProcedureEdge.Native.Type.Owns.Forward(from, to, order, false);
                break;
            case OWNS_KEY:
                edge = new ProcedureEdge.Native.Type.Owns.Forward(from, to, order, true);
                break;
            case PLAYS:
                edge = new ProcedureEdge.Native.Type.Plays.Forward(from, to, order);
                break;
            case RELATES:
                edge = new ProcedureEdge.Native.Type.Relates.Forward(from, to,order);
                break;
            default:
                throw TypeDBException.of(UNRECOGNISED_VALUE);
        }
        edges[order] = edge;
    }

    private void registerIn(ProcedureVertex.Type from, ProcedureVertex.Type to, StructureEdge.Native<?, ?> structureEdge, int order) {
        // TODO are these all backward, from <- to?
        ProcedureEdge.Native.Type edge;
        switch (structureEdge.encoding().asType()) {
            case SUB:
                edge = new ProcedureEdge.Native.Type.Sub.Backward(from.asType(), to.asType(), order, structureEdge.isTransitive());
                break;
            case OWNS:
                edge =  new ProcedureEdge.Native.Type.Owns.Backward(from, to, order, false);
                break;
            case OWNS_KEY:
                edge = new ProcedureEdge.Native.Type.Owns.Backward(from, to, order, true);
                break;
            case PLAYS:
                edge = new ProcedureEdge.Native.Type.Plays.Backward(from, to, order);
                break;
            case RELATES:
                edge = new ProcedureEdge.Native.Type.Relates.Backward(from, to,order);
                break;
            default:
                throw TypeDBException.of(UNRECOGNISED_VALUE);
        }
        edges[order] = edge;
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder();
        str.append("Type Combination Procedure: {");
        List<ProcedureEdge<?, ?>> procedureEdges = Arrays.asList(edges);
        procedureEdges.sort(Comparator.comparing(ProcedureEdge::order));
        List<ProcedureVertex<?, ?>> procedureVertices = new ArrayList<>(vertices.values());
        procedureVertices.sort(Comparator.comparing(v -> v.id().toString()));

        str.append("\n\tvertices:");
        for (ProcedureVertex<?, ?> v : procedureVertices) {
            str.append("\n\t\t").append(v);
        }
        str.append("\n\tedges:");
        for (ProcedureEdge<?, ?> e : procedureEdges) {
            str.append("\n\t\t").append(e);
        }
        str.append("\n}");
        return str.toString();
    }
}
