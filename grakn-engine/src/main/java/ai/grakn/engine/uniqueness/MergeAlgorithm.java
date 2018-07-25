/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.engine.uniqueness;

import ai.grakn.kb.internal.EmbeddedGraknTx;
import ai.grakn.util.Schema;
import com.google.common.collect.Lists;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * TODO
 * @author Ganeshwara Herawan Hananda
 */
public class MergeAlgorithm {
    private static Logger LOG = LoggerFactory.getLogger(MergeAlgorithm.class);

    /**
     * Merges a list of attributes.
     * The attributes being processed will be marked so they cannot be touched by other operations.
     * @param duplicates the list of attributes to be merged
     * @return the merged attribute (TODO)
     */
    public void merge(EmbeddedGraknTx tx, List<Attribute> duplicates) {
        LOG.info("merge started...");

        GraphTraversalSource tinker = tx.getTinkerTraversal();
        GraphTraversal<Vertex, Vertex> t = tinker.V().has(Schema.VertexProperty.INDEX.name(), duplicates.get(0).value());
        Vertex mergeTargetV = t.next();
        List<Vertex> prevMergeTargetV = t.next(1);
        List<String> excludeMergeTarget = duplicates.stream()
                .map(elem -> elem.conceptId().getValue())
                .filter(elem -> !elem.equals(mergeTargetV.value(Schema.VertexProperty.ID.name())))
                .collect(Collectors.toList());
        List<Vertex> duplicatesV = new LinkedList<>();
        duplicatesV.addAll(tinker.V().has(Schema.VertexProperty.ID.name(), P.within(excludeMergeTarget)).toList());
        duplicatesV.addAll(prevMergeTargetV);

        LOG.info("items from queue = '" + duplicates + "', merge target = " + mergeTargetV + ", previous merge target = " + prevMergeTargetV + ", merge duplicates = " + duplicatesV);
        if (duplicatesV.size() >= 1) {
            lock(duplicates);

            for (Vertex dup: duplicatesV) {
                try {
                    dup.vertices(Direction.IN).forEachRemaining(ent -> {
                        Edge edge = tinker.V(dup).inE(Schema.EdgeLabel.ATTRIBUTE.getLabel()).filter(__.outV().is(ent)).next();
                        edge.remove();
                        ent.addEdge(Schema.EdgeLabel.ATTRIBUTE.getLabel(), mergeTargetV);
                    });
                }
                catch (IllegalStateException vertexAlreadyRemovedException) {
                    LOG.warn("Trying to call the method vertices(Direction.IN) on vertex " + dup.id() + " which is already removed.");
                }
                dup.remove();
            }

            unlock(duplicates);
            LOG.info("merge completed.");
        }
    }

    // TODO
    private void lock(List<Attribute> attributes) {
        System.out.println(attributes); // PMD HACK TODO
    }

    // TODO
    private void unlock(List<Attribute> attributes) {
        System.out.println(attributes); // PMD HACK TODO
    }
}
