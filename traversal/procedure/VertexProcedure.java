/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.traversal.procedure;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.parameters.Order;
import com.vaticle.typedb.core.concurrent.producer.FunctionalProducer;
import com.vaticle.typedb.core.graph.GraphManager;
import com.vaticle.typedb.core.graph.vertex.Vertex;
import com.vaticle.typedb.core.traversal.Traversal;
import com.vaticle.typedb.core.traversal.common.Modifiers;
import com.vaticle.typedb.core.traversal.common.VertexMap;
import com.vaticle.typedb.core.traversal.structure.StructureEdge;
import com.vaticle.typedb.core.traversal.structure.StructureVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.vaticle.typedb.common.collection.Collections.map;
import static com.vaticle.typedb.common.collection.Collections.pair;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.parameters.Order.Asc.ASC;
import static com.vaticle.typedb.core.concurrent.producer.Producers.async;

public class VertexProcedure implements PermutationProcedure {

    private static final Logger LOG = LoggerFactory.getLogger(VertexProcedure.class);
    private final ProcedureVertex<?, ?> vertex;

    private VertexProcedure(ProcedureVertex<?, ?> vertex) {
        this.vertex = vertex;
    }

    public static VertexProcedure create(StructureVertex<?> structureVertex) {
        ProcedureVertex<?, ?> procedureVertex;
        if (structureVertex.isType()) {
            procedureVertex = new ProcedureVertex.Type(structureVertex.id());
            procedureVertex.asType().props(structureVertex.asType().props());
        } else if (structureVertex.isThing()) {
            procedureVertex = new ProcedureVertex.Thing(structureVertex.id());
            procedureVertex.asThing().props(structureVertex.asThing().props());
        } else if (structureVertex.isValue()) {
            procedureVertex = new ProcedureVertex.Value(structureVertex.id());
            procedureVertex.asValue().props(structureVertex.asValue().props());
        } else throw TypeDBException.of(ILLEGAL_STATE);

        for (StructureEdge<?, ?> structureEdge : structureVertex.loops()) {
            ProcedureEdge<?, ?> edge = ProcedureEdge.of(procedureVertex, procedureVertex, structureEdge, true);
            procedureVertex.loop(edge);
        }
        return new VertexProcedure(procedureVertex);
    }

    @Override
    public FunctionalProducer<VertexMap> producer(GraphManager graphMgr, Traversal.Parameters params,
                                                  Modifiers modifiers, int parallelisation) {
        LOG.trace(params.toString());
        LOG.trace(this.toString());
        return async(iterator(graphMgr, params, modifiers));
    }

    @Override
    public FunctionalIterator<VertexMap> iterator(GraphManager graphMgr, Traversal.Parameters params,
                                                  Modifiers modifiers) {
        LOG.trace(params.toString());
        LOG.trace(this.toString());
        assert vertex.id().isRetrievable() && modifiers.filter().variables().contains(vertex.id().asVariable().asRetrievable());
        Optional<Order> order = modifiers.sorting().order(vertex.id());
        FunctionalIterator<? extends Vertex<?, ?>> iterator = vertex.iterator(graphMgr, params, order.orElse(ASC), order.isPresent());
        for (ProcedureEdge<?, ?> e : vertex.loops()) {
            iterator = iterator.filter(v -> e.isClosure(graphMgr, v, v, params));
        }

        return iterator.map(v -> {
            if (v.isThing() && v.asThing().isAttribute() && v.asThing().asAttribute().isValueSortable()) {
                return VertexMap.of(map(pair(vertex.id().asVariable().asRetrievable(), v.asThing().asAttribute().asValueSortable().toAttribute())));
            } else {
                return VertexMap.of(map(pair(vertex.id().asVariable().asRetrievable(), v)));
            }
        });
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder();
        str.append("Vertex Procedure: {");
        List<ProcedureEdge<?, ?>> procedureEdges = new ArrayList<>(vertex.loops());

        str.append("\n\tvertex:");
        str.append("\n\t\t").append(vertex);
        str.append("\n\tedges:");
        for (ProcedureEdge<?, ?> e : procedureEdges) {
            str.append("\n\t\t").append(e);
        }
        str.append("\n}");
        return str.toString();
    }

}
