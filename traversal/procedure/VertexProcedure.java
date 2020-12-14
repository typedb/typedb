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

package grakn.core.traversal.procedure;

import grakn.core.common.iterator.ResourceIterator;
import grakn.core.common.producer.Producer;
import grakn.core.graph.GraphManager;
import grakn.core.traversal.Traversal;
import grakn.core.traversal.common.VertexMap;
import grakn.core.traversal.producer.VertexProducer;
import grakn.core.traversal.structure.StructureVertex;
import graql.lang.pattern.variable.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static grakn.common.collection.Collections.map;
import static grakn.common.collection.Collections.pair;

public class VertexProcedure implements Procedure {

    private static final Logger LOG = LoggerFactory.getLogger(VertexProcedure.class);
    private final ProcedureVertex<?, ?> vertex;

    private VertexProcedure(StructureVertex<?> structureVertex) {
        vertex = structureVertex.isType()
                ? new ProcedureVertex.Type(structureVertex.id(), true)
                : new ProcedureVertex.Thing(structureVertex.id(), true);
        if (vertex.isType()) vertex.asType().props(structureVertex.asType().props());
        else vertex.asThing().props(structureVertex.asThing().props());
    }

    public static VertexProcedure create(StructureVertex<?> stuctureVertex) {
        return new VertexProcedure(stuctureVertex);
    }

    @Override
    public Producer<VertexMap> producer(GraphManager graphMgr, Traversal.Parameters params, int parallelisation) {
        LOG.debug(toString()); // TODO: remove this
        return new VertexProducer(graphMgr, vertex, params);
    }

    @Override
    public ResourceIterator<VertexMap> iterator(GraphManager graphMgr, Traversal.Parameters params) {
        LOG.debug(toString()); // TODO: remove this
        Reference ref = vertex.id().asVariable().reference();
        return vertex.iterator(graphMgr, params).map(v -> VertexMap.of(map(pair(ref, v))));
    }
}
