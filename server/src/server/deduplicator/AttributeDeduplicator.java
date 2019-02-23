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

package grakn.core.server.deduplicator;

import com.google.common.collect.Lists;
import grakn.core.server.kb.Schema;
import grakn.core.server.Transaction;
import grakn.core.server.session.SessionImpl;
import grakn.core.server.session.SessionStore;
import grakn.core.server.session.TransactionOLTP;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

/**
 *
 * The class containing the actual de-duplication algorithm.
 *
 */
public class AttributeDeduplicator {
    private static Logger LOG = LoggerFactory.getLogger(AttributeDeduplicator.class);
    /**
     * Deduplicate attributes that has the same value. A de-duplication process consists of picking a single attribute
     * in the duplicates as the "merge target", copying every edges from every "other duplicates" to the merge target, and
     * finally deleting that other duplicates.
     *
     * @param sessionStore the factory object for accessing the database
     * @param keyspaceIndexPair the pair containing information about the attribute keyspace and index
     */
    public static void deduplicate(SessionStore sessionStore, KeyspaceIndexPair keyspaceIndexPair) {
        SessionImpl session = sessionStore.session(keyspaceIndexPair.keyspace());
        try (TransactionOLTP tx = session.transaction(Transaction.Type.WRITE)) {
            GraphTraversalSource tinker = tx.getTinkerTraversal();
            GraphTraversal<Vertex, Vertex> duplicates = tinker.V().has(Schema.VertexProperty.INDEX.name(), keyspaceIndexPair.index());
            Vertex mergeTargetV = duplicates.next();
            while (duplicates.hasNext()) {
                Vertex duplicate = duplicates.next();
                try {
                    duplicate.vertices(Direction.IN).forEachRemaining(connectedVertex -> {
                        // merge attribute edge connecting 'duplicate' and 'connectedVertex' to 'mergeTargetV', if exists
                        GraphTraversal<Vertex, Edge> attributeEdge =
                                tinker.V(duplicate).inE(Schema.EdgeLabel.ATTRIBUTE.getLabel()).filter(__.outV().is(connectedVertex));
                        if (attributeEdge.hasNext()) {
                            mergeAttributeEdge(mergeTargetV, connectedVertex, attributeEdge);
                        }

                        // merge role-player edge connecting 'duplicate' and 'connectedVertex' to 'mergeTargetV', if exists
                        GraphTraversal<Vertex, Edge> rolePlayerEdge =
                                tinker.V(duplicate).inE(Schema.EdgeLabel.ROLE_PLAYER.getLabel()).filter(__.outV().is(connectedVertex));
                        if (rolePlayerEdge.hasNext()) {
                            mergeRolePlayerEdge(mergeTargetV, rolePlayerEdge);
                        }
                    });
                    duplicate.remove();
                }
                catch (IllegalStateException vertexAlreadyRemovedException) {
                    LOG.warn("Trying to call the method vertices(Direction.IN) on vertex " + duplicate.id() + " which is already removed.");
                }
            }

            tx.commit();
        } finally {
            session.close();
        }
    }

    private static void mergeRolePlayerEdge(Vertex mergeTargetV, GraphTraversal<Vertex, Edge> rolePlayerEdge) {
        Edge edge = rolePlayerEdge.next();
        Vertex relationshipVertex = edge.outVertex();
        Object[] properties = propertiesToArray(Lists.newArrayList(edge.properties()));
        relationshipVertex.addEdge(Schema.EdgeLabel.ROLE_PLAYER.getLabel(), mergeTargetV, properties);
        edge.remove();
    }

    private static void mergeAttributeEdge(Vertex mergeTargetV, Vertex ent, GraphTraversal<Vertex, Edge> attributeEdge) {
        Edge edge = attributeEdge.next();
        Object[] properties = propertiesToArray(Lists.newArrayList(edge.properties()));
        ent.addEdge(Schema.EdgeLabel.ATTRIBUTE.getLabel(), mergeTargetV, properties);
        edge.remove();
    }

    private static Object[] propertiesToArray(ArrayList<Property<Object>> propertiesAsKeyValue) {
        ArrayList<Object> propertiesAsObj = new ArrayList<>();
        for (Property<Object> property: propertiesAsKeyValue) {
            propertiesAsObj.add(property.key());
            propertiesAsObj.add(property.value());
        }
        return propertiesAsObj.toArray();
    }
}
