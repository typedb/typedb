/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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

package ai.grakn.engine.attribute.uniqueness;

import ai.grakn.GraknTxType;
import ai.grakn.engine.attribute.uniqueness.queue.Attributes;
import ai.grakn.engine.factory.EngineGraknTxFactory;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.stream.Collectors;

/**
 * TODO
 * @author Ganeshwara Herawan Hananda
 */
class DeduplicationAlgorithm {
    private static Logger LOG = LoggerFactory.getLogger(DeduplicationAlgorithm.class);

    /**
     * Deduplicate unique through attributes
     * @param attributes
     */
    static void deduplicate(EngineGraknTxFactory txFactory, Attributes attributes) {
        try {
            LOG.info("starting a new batch to process these new attributes: " + attributes);

            // group the attributes into a set of unique (keyspace -> value) pair
            Set<KeyspaceValuePair> uniqueKeyValuePairs = attributes.attributes().stream()
                    .map(attr -> KeyspaceValuePair.create(attr.keyspace(), attr.value())).collect(Collectors.toSet());

            // perform deduplicateSingleAttribute for each (keyspace -> value)
            for (KeyspaceValuePair keyspaceValuePair : uniqueKeyValuePairs) {
                try (EmbeddedGraknTx tx = txFactory.tx(keyspaceValuePair.keyspace(), GraknTxType.WRITE)) {
                    deduplicateSingleAttribute(tx.getTinkerTraversal(), keyspaceValuePair.value());
                    tx.commit();
                }
            }
            LOG.info("new attributes processed.");
        } catch (RuntimeException e) {
            LOG.error("An exception has occurred in the AttributeMergerDaemon. ", e);
            throw e;
        }
    }

    /**
     * Given an attributeValue, find the duplicates and merge them into a single unique attribute
     *
     * @param tinker the {@link GraphTraversalSource} object for accessing the database
     * @param attributeValue the value of attribute with duplicates
     */
    private static void deduplicateSingleAttribute(GraphTraversalSource tinker, String attributeValue) {
        GraphTraversal<Vertex, Vertex> duplicates = tinker.V().has(Schema.VertexProperty.INDEX.name(), attributeValue);
        Vertex mergeTargetV = duplicates.next();
        while (duplicates.hasNext()) {
            Vertex dup = duplicates.next();
            try {
                dup.vertices(Direction.IN).forEachRemaining(ent -> {
                    Edge edge = tinker.V(dup).inE(Schema.EdgeLabel.ATTRIBUTE.getLabel()).filter(__.outV().is(ent)).next();
                    edge.remove();
                    ent.addEdge(Schema.EdgeLabel.ATTRIBUTE.getLabel(), mergeTargetV);
                });
                dup.remove();
            }
            catch (IllegalStateException vertexAlreadyRemovedException) {
                LOG.warn("Trying to call the method vertices(Direction.IN) on vertex " + dup.id() + " which is already removed.");
            }
        }
    }

}
