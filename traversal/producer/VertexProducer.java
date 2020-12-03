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
 */

package grakn.core.traversal.producer;

import grakn.core.graph.GraphManager;
import grakn.core.graph.vertex.Vertex;
import grakn.core.traversal.procedure.Procedure;
import graql.lang.pattern.variable.Reference;

import java.util.Map;

public class VertexProducer implements TraversalProducer {

    private final GraphManager graphMgr;
    private final Procedure procedure;
    private final int parallelisation;

    public VertexProducer(GraphManager graphMgr, Procedure procedure, int parallelisation) {
        this.graphMgr = graphMgr;
        this.procedure = procedure;
        this.parallelisation = parallelisation;
    }

    @Override
    public void produce(Sink<Map<Reference, Vertex<?, ?>>> sink, int count) {

    }

    @Override
    public void recycle() {

    }
}
