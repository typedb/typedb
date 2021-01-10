/*
 * Copyright (C) 2021 Grakn Labs
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

package grakn.core.traversal.procedure;

import grakn.core.common.iterator.ResourceIterator;
import grakn.core.common.producer.Producer;
import grakn.core.common.producer.Producers;
import grakn.core.graph.GraphManager;
import grakn.core.graph.vertex.Vertex;
import grakn.core.traversal.Traversal;
import grakn.core.traversal.common.VertexMap;
import grakn.core.traversal.planner.PlannerVertex;
import graql.lang.pattern.variable.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static grakn.common.collection.Collections.map;
import static grakn.common.collection.Collections.pair;

public class VertexProcedure implements Procedure {

    private static final Logger LOG = LoggerFactory.getLogger(VertexProcedure.class);
    private final ProcedureVertex<?, ?> vertex;

    private VertexProcedure(ProcedureVertex<?, ?> vertex) {
        this.vertex = vertex;
    }

    public static VertexProcedure create(PlannerVertex<?> plannerVertex) {
        assert plannerVertex.id().isNamedReference();
        return new VertexProcedure(toProcedure(plannerVertex));
    }

    private static ProcedureVertex<?, ?> toProcedure(PlannerVertex<?> plannerVertex) {
        assert plannerVertex.isStartingVertex();
        ProcedureVertex<?, ?> procedureVertex = plannerVertex.isType()
                ? new ProcedureVertex.Type(plannerVertex.id(), true)
                : new ProcedureVertex.Thing(plannerVertex.id(), true);
        if (procedureVertex.isType()) procedureVertex.asType().props(plannerVertex.asType().props());
        else procedureVertex.asThing().props(plannerVertex.asThing().props());

        plannerVertex.outs().forEach(plannerEdge -> {
            if (plannerEdge.isSelected()) {
                ProcedureEdge<?, ?> procedureEdge = ProcedureEdge.of(procedureVertex, procedureVertex, plannerEdge);
                procedureVertex.out(procedureEdge);
                procedureVertex.in(procedureEdge);
            }
        });

        return procedureVertex;
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder();
        str.append("Vertex Procedure: {");
        List<ProcedureEdge<?, ?>> procedureEdges = new ArrayList<>(vertex.outs());
        procedureEdges.sort(Comparator.comparing(ProcedureEdge::order));

        str.append("\n\tvertex:");
        str.append("\n\t\t").append(vertex);
        str.append("\n\tedges:");
        for (ProcedureEdge<?, ?> e : procedureEdges) {
            str.append("\n\t\t").append(e);
        }
        str.append("\n}");
        return str.toString();
    }

    @Override
    public Producer<VertexMap> producer(GraphManager graphMgr, Traversal.Parameters params, int parallelisation) {
        LOG.debug(params.toString());
        LOG.debug(this.toString());
        return Producers.producer(iterator(graphMgr, params));
    }

    @Override
    public ResourceIterator<VertexMap> iterator(GraphManager graphMgr, Traversal.Parameters params) {
        LOG.debug(params.toString());
        LOG.debug(this.toString());
        Reference ref = vertex.id().asVariable().reference();
        ResourceIterator<? extends Vertex<?, ?>> iterator = vertex.iterator(graphMgr, params);
        for (ProcedureEdge<?, ?> e : vertex.outs()) {
            iterator = iterator.filter(v -> e.isClosure(graphMgr, v, v, params));
        }
        return iterator.map(v -> VertexMap.of(map(pair(ref, v))));
    }
}
