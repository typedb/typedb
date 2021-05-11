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

package com.vaticle.typedb.core.traversal;

import com.vaticle.typedb.common.collection.Either;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.concurrent.producer.FunctionalProducer;
import com.vaticle.typedb.core.graph.GraphManager;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typedb.core.traversal.common.VertexMap;
import com.vaticle.typedb.core.traversal.procedure.GraphProcedure;

import java.util.Set;

import static com.vaticle.typedb.common.collection.Collections.set;

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

    public FunctionalProducer<VertexMap> producer(Traversal traversal, Either<Arguments.Query.Producer, Long> context,
                                                  int parallelisation) {
        return producer(traversal, context, parallelisation, false);
    }

    public FunctionalProducer<VertexMap> producer(Traversal traversal, Either<Arguments.Query.Producer, Long> context,
                                                  int parallelisation, boolean extraPlanningTime) {
        traversal.initialise(cache);
        return traversal.producer(graphMgr, context, parallelisation, extraPlanningTime);
    }

    public FunctionalIterator<VertexMap> iterator(Traversal traversal) {
        return iterator(traversal, false);
    }

    public FunctionalIterator<VertexMap> iterator(Traversal traversal, boolean extraPlanningTime) {
        traversal.initialise(cache);
        return traversal.iterator(graphMgr, extraPlanningTime);
    }

    public FunctionalIterator<VertexMap> relations(Traversal traversal) {
        return traversal.relations(graphMgr);
    }

    public FunctionalIterator<VertexMap> iterator(GraphProcedure procedure, Traversal.Parameters params,
                                                  Set<Identifier.Variable.Retrievable> filter) {
        return procedure.iterator(graphMgr, params, filter);
    }

}
