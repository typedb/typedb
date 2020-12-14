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

package grakn.core.traversal;

import grakn.core.common.iterator.ResourceIterator;
import grakn.core.common.producer.Producer;
import grakn.core.graph.GraphManager;
import grakn.core.traversal.common.VertexMap;
import grakn.core.traversal.procedure.GraphProcedure;

public class TraversalEngine {

    private final GraphManager graphMgr;
    private final TraversalCache cache;

    public TraversalEngine(GraphManager graphMgr, TraversalCache cache) {
        this.graphMgr = graphMgr;
        this.cache = cache;
    }

    public GraphManager graph() {
        return graphMgr;
    }

    public Producer<VertexMap> producer(Traversal traversal, int parallelisation) {
        traversal.initialisePlanner(cache);
        return traversal.producer(graphMgr, parallelisation);
    }

    public ResourceIterator<VertexMap> iterator(Traversal traversal) {
        traversal.initialisePlanner(cache);
        return traversal.iterator(graphMgr);
    }

    public ResourceIterator<VertexMap> iterator(GraphProcedure procedure, Traversal.Parameters params) {
        return procedure.iterator(graphMgr, params);
    }
}
