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

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.concurrent.producer.FunctionalProducer;
import com.vaticle.typedb.core.graph.GraphManager;
import com.vaticle.typedb.core.graph.vertex.Vertex;
import com.vaticle.typedb.core.traversal.GraphTraversal;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typedb.core.traversal.common.VertexMap;
import com.vaticle.typedb.core.traversal.planner.PlannerVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

import static com.vaticle.typedb.common.collection.Collections.map;
import static com.vaticle.typedb.common.collection.Collections.pair;
import static com.vaticle.typedb.core.concurrent.producer.Producers.async;

public class VertexProcedure implements Procedure {

    private static final Logger LOG = LoggerFactory.getLogger(VertexProcedure.class);
    private final ProcedureVertex<?, ?> vertex;

    private VertexProcedure(ProcedureVertex<?, ?> vertex) {
        this.vertex = vertex;
    }

    public static VertexProcedure create(PlannerVertex<?> plannerVertex) {
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
    public FunctionalProducer<VertexMap> producer(GraphManager graphMgr, GraphTraversal.Parameters params,
                                                  Set<Identifier.Variable.Retrievable> filter, int parallelisation) {
        LOG.debug(params.toString());
        LOG.debug(this.toString());
        return async(iterator(graphMgr, params, filter));
    }

    @Override
    public FunctionalIterator<VertexMap> iterator(GraphManager graphMgr, GraphTraversal.Parameters params,
                                                  Set<Identifier.Variable.Retrievable> filter) {
        LOG.debug(params.toString());
        LOG.debug(this.toString());
        assert vertex.id().isRetrievable() && filter.contains(vertex.id().asVariable().asRetrievable());
        FunctionalIterator<? extends Vertex<?, ?>> iterator = vertex.iterator(graphMgr, params);
        for (ProcedureEdge<?, ?> e : vertex.outs()) {
            iterator = iterator.filter(v -> e.isClosure(graphMgr, v, v, params));
        }

        return iterator.map(v -> VertexMap.of(map(pair(vertex.id().asVariable().asRetrievable(), v))));
    }

}
