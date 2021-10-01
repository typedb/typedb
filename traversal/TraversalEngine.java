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
import com.vaticle.typedb.core.graph.vertex.TypeVertex;
import com.vaticle.typedb.core.traversal.common.Identifier.Variable.Retrievable;
import com.vaticle.typedb.core.traversal.common.VertexMap;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

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

    public FunctionalProducer<VertexMap> producer(GraphTraversal.Thing traversal, Either<Arguments.Query.Producer, Long> context,
                                                  int parallelisation) {
        traversal.initialise(cache);
        return traversal.permutationProducer(graphMgr, context, parallelisation);
    }

    public FunctionalIterator<VertexMap> iterator(GraphTraversal.Thing traversal) {
        traversal.initialise(cache);
        return traversal.permutationIterator(graphMgr);
    }

    public FunctionalIterator<VertexMap> iterator(GraphTraversal.Type traversal) {
        return traversal.permutationIterator(graphMgr);
    }

    public FunctionalIterator<VertexMap> iterator(RelationTraversal traversal) {
        return traversal.permutationIterator(graphMgr);
    }

    public Optional<Map<Retrievable, Set<TypeVertex>>> combination(GraphTraversal.Type traversal,
                                                                   Set<Retrievable> concreteTypesOnly) {
        return traversal.combination(graphMgr, concreteTypesOnly);
    }
}
