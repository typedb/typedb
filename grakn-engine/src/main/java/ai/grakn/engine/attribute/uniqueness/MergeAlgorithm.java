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

package ai.grakn.engine.attribute.uniqueness;

import ai.grakn.GraknTxType;
import ai.grakn.engine.attribute.uniqueness.queue.Attributes;
import ai.grakn.engine.attribute.uniqueness.queue.Attribute;
import ai.grakn.factory.EmbeddedGraknSession;
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

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * TODO
 * @author Ganeshwara Herawan Hananda
 */
public class MergeAlgorithm {
    private static Logger LOG = LoggerFactory.getLogger(MergeAlgorithm.class);

    public void merge(Attributes newAttributes) {
        try {
            LOG.info("starting a new batch to process these new attributes: " + newAttributes);
            Set<KeyspaceAndValue> duplicates = newAttributes.attributes().stream()
                    .map(attr -> KeyspaceAndValue.create(attr.keyspace(), attr.value())).collect(Collectors.toSet());
            for (KeyspaceAndValue keyspaceAndValue: duplicates) {
                try (EmbeddedGraknSession s  = EmbeddedGraknSession.create(keyspaceAndValue.keyspace(), "localhost:4567");
                     EmbeddedGraknTx tx = s.transaction(GraknTxType.WRITE)) {
                    merge(tx, keyspaceAndValue.value());
                    tx.commitSubmitNoLogs();
                }
            }
//                    newAttributes.markProcessed(); // TODO: enable after readAttributes is changed to processBatch()
            LOG.info("new attributes processed.");
        } catch (RuntimeException e) {
            LOG.error("An exception has occurred in the AttributeMergerDaemon. ", e);
            throw e;
        }
    }

    /**
     * Merges a list of attributes.
     * The attributes being processed will be marked so they cannot be touched by other operations.
     * @param value the value of attributes to be merged
     * @return the merged attribute (TODO)
     */
    public void merge(EmbeddedGraknTx tx, String value) {
        GraphTraversalSource tinker = tx.getTinkerTraversal();
        GraphTraversal<Vertex, Vertex> duplicates = tinker.V().has(Schema.VertexProperty.INDEX.name(), value);
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

    // TODO
    private void lock(List<Attribute> attributes) {
        System.out.println(attributes); // PMD HACK TODO
    }

    // TODO
    private void unlock(List<Attribute> attributes) {
        System.out.println(attributes); // PMD HACK TODO
    }
}
