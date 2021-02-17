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

package grakn.core.traversal;

import grakn.core.common.iterator.ResourceIterator;
import grakn.core.common.parameters.Arguments;
import grakn.core.concurrent.producer.Producer;
import grakn.core.graph.GraphManager;
import grakn.core.traversal.common.Identifier;
import grakn.core.traversal.common.VertexMap;
import grakn.core.traversal.procedure.GraphProcedure;

import java.util.Set;

import static grakn.common.collection.Collections.set;

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

    public Producer<VertexMap> producer(Traversal traversal, Arguments.Query.Producer mode, int parallelisation) {
        return producer(traversal, mode, parallelisation, false);
    }

    public Producer<VertexMap> producer(Traversal traversal, Arguments.Query.Producer mode,
                                        int parallelisation, boolean extraPlanningTime) {
        traversal.initialise(cache);
        return traversal.producer(graphMgr, mode, parallelisation, extraPlanningTime);
    }

    public ResourceIterator<VertexMap> iterator(Traversal traversal) {
        return iterator(traversal, false);
    }

    public ResourceIterator<VertexMap> iterator(Traversal traversal, boolean extraPlanningTime) {
        traversal.initialise(cache);
        return traversal.iterator(graphMgr, extraPlanningTime);
    }

    public ResourceIterator<VertexMap> iterator(GraphProcedure procedure, Traversal.Parameters params) {
        return iterator(procedure, params, set());
    }

    public ResourceIterator<VertexMap> iterator(GraphProcedure procedure, Traversal.Parameters params,
                                                Set<Identifier.Variable.Retrievable> filter) {
        return procedure.iterator(graphMgr, params, filter);
    }
}
