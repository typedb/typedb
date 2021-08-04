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
import com.vaticle.typedb.core.graph.common.Encoding;
import com.vaticle.typedb.core.traversal.TypeTraversal;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typedb.core.traversal.planner.PlannerEdge;
import com.vaticle.typedb.core.traversal.structure.StructureEdge;
import com.vaticle.typedb.core.traversal.structure.StructureVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.UNRECOGNISED_VALUE;
import static com.vaticle.typedb.core.graph.common.Encoding.Edge.Type.OWNS;
import static com.vaticle.typedb.core.graph.common.Encoding.Edge.Type.OWNS_KEY;
import static com.vaticle.typedb.core.graph.common.Encoding.Edge.Type.PLAYS;
import static com.vaticle.typedb.core.graph.common.Encoding.Edge.Type.RELATES;
import static com.vaticle.typedb.core.graph.common.Encoding.Edge.Type.SUB;

public class CombinationProcedure {

    private static final Logger LOG = LoggerFactory.getLogger(GraphProcedure.class);

    private final Map<Identifier, ProcedureVertex.Type> vertices;
    private final ProcedureEdge.Native.Type[] edges;

    CombinationProcedure(int edgeSize) {
        vertices = new HashMap<>();
        edges = new ProcedureEdge.Native.Type[edgeSize];
    }

    public static CombinationProcedure of(TypeTraversal traversal) {
        CombinationProcedure procedure = new CombinationProcedure(traversal.structure().edges().size());
        Set<StructureEdge<?, ?>> visitedEdges = new HashSet<>();
        procedure.registerDFS(traversal.structure().vertices().iterator().next().asType(), visitedEdges);
        return procedure;
    }

    private ProcedureVertex.Type registerDFS(StructureVertex.Type structureVertex, Set<StructureEdge<?, ?>> visitedEdges) {
        if (vertices.containsKey(structureVertex.id())) return vertices.get(structureVertex.id());
        ProcedureVertex.Type vertex = vertices.computeIfAbsent(structureVertex.id(), id -> new ProcedureVertex.Type(id, vertices.size() == 1));
        vertex.props(structureVertex.props());
        structureVertex.outs().forEach(structureEdge -> {
            if (!visitedEdges.contains(structureEdge)) {
                visitedEdges.add(structureEdge);
                int order = visitedEdges.size();
                ProcedureVertex.Type toVertex = registerDFS(structureEdge.to().asType(), visitedEdges);
                registerOut(vertex, toVertex, structureEdge.asNative().encoding(), order);
            }
        });
        structureVertex.ins().forEach(structureEdge -> {
            if (!visitedEdges.contains(structureEdge)) {
                visitedEdges.add(structureEdge);
                int order = visitedEdges.size();
                ProcedureVertex.Type fromVertex = registerDFS(structureEdge.from().asType(), visitedEdges);
                registerIn(fromVertex, vertex, structureEdge.asNative().encoding(), order);
            }
        });
        return vertex;
    }

    private void registerOut(ProcedureVertex.Type from, ProcedureVertex.Type to, Encoding.Edge encoding, int order) {
        // TODO are these all forward, from -> to?
        switch (encoding) {
            case SUB:
                return new ProcedureEdge.Native.Type.Sub.Forward(from.asType(), to.asType());
            case OWNS:
                return new ProcedureEdge.Native.Type.Owns.Forward(from, to, order, false);
            case OWNS_KEY:
                return new ProcedureEdge.Native.Type.Owns.Forward(from, to, order, true);
            case PLAYS:
                return new ProcedureEdge.Native.Type.Plays.Forward(from, to, order);
            case RELATES:
                return new ProcedureEdge.Native.Type.Relates.Forward(from, to,order);
            default:
                throw TypeDBException.of(UNRECOGNISED_VALUE);
        }
    }

    private void registerIn(ProcedureVertex.Type from, ProcedureVertex.Type to, Encoding.Edge encoding, int order) {

    }


}
