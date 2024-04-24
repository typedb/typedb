/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.traversal;

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
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

    public FunctionalProducer<VertexMap> producer(GraphTraversal.Thing traversal, int parallelisation) {
        traversal.initialise(cache);
        return traversal.permutationProducer(graphMgr, parallelisation);
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
