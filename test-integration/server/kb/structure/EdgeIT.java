/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2019 Grakn Labs Ltd
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

package grakn.core.server.kb.structure;

import grakn.core.concept.impl.ConceptManagerImpl;
import grakn.core.concept.impl.ConceptObserver;
import grakn.core.concept.structure.EdgeElementImpl;
import grakn.core.concept.structure.ElementFactory;
import grakn.core.core.Schema;
import grakn.core.kb.concept.api.Entity;
import grakn.core.kb.concept.api.EntityType;
import grakn.core.kb.concept.structure.EdgeElement;
import grakn.core.kb.server.AttributeManager;
import grakn.core.kb.server.ShardManager;
import grakn.core.kb.server.Transaction;
import grakn.core.kb.server.cache.KeyspaceSchemaCache;
import grakn.core.kb.server.keyspace.Keyspace;
import grakn.core.kb.server.statistics.KeyspaceStatistics;
import grakn.core.kb.server.statistics.UncomittedStatisticsDelta;
import grakn.core.rule.GraknTestServer;
import grakn.core.server.cache.CacheProviderImpl;
import grakn.core.server.keyspace.KeyspaceImpl;
import grakn.core.server.session.AttributeManagerImpl;
import grakn.core.server.session.JanusGraphFactory;
import grakn.core.server.session.SessionImpl;
import grakn.core.server.session.ShardManagerImpl;
import grakn.core.server.session.TransactionOLTP;
import grakn.core.util.ConceptDowncasting;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.tinkerpop.gremlin.structure.Edge;
import grakn.core.graph.core.JanusGraphTransaction;
import grakn.core.graph.graphdb.database.StandardJanusGraph;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class EdgeIT {

    @ClassRule
    public static final GraknTestServer server = new GraknTestServer();

    private SessionImpl session;
    private Transaction tx;
    private EntityType entityType;
    private Entity entity;
    private EdgeElement edge;

    @Before
    public void setUp(){
        String keyspaceName = "ksp_"+UUID.randomUUID().toString().substring(0, 20).replace("-", "_");
        Keyspace keyspace = new KeyspaceImpl(keyspaceName);

        // obtain components to create sessions and transactions
        JanusGraphFactory janusGraphFactory = server.janusGraphFactory();
        StandardJanusGraph graph = janusGraphFactory.openGraph(keyspace.name());

        // create the session
        AttributeManager attributeManager = new AttributeManagerImpl();

        session = new SessionImpl(keyspace, server.serverConfig(), new KeyspaceSchemaCache(), graph,
                new KeyspaceStatistics(), attributeManager, new ShardManagerImpl(), new ReentrantReadWriteLock());

        // create the transaction
        CacheProviderImpl cacheProvider = new CacheProviderImpl(new KeyspaceSchemaCache());
        UncomittedStatisticsDelta statisticsDelta = new UncomittedStatisticsDelta();

        // janus elements
        JanusGraphTransaction janusGraphTransaction = graph.newThreadBoundTransaction();
        ElementFactory elementFactory = new ElementFactory(janusGraphTransaction);

        // Grakn elements
        ConceptObserver conceptObserver = new ConceptObserver(cacheProvider, statisticsDelta, attributeManager, janusGraphTransaction.toString());
        ConceptManagerImpl conceptManager = new ConceptManagerImpl(elementFactory, cacheProvider.getTransactionCache(), conceptObserver, attributeManager);

        tx = new TransactionOLTP(session, janusGraphTransaction, conceptManager, cacheProvider, statisticsDelta);
        tx.open(Transaction.Type.WRITE);

        // Create Edge
        entityType = tx.putEntityType("My Entity Type");
        entity = entityType.create();

        Edge tinkerEdge = tx.getTinkerTraversal().V().hasId(Schema.elementId(entity.id())).outE().next();
        edge = new EdgeElementImpl(elementFactory, tinkerEdge);
    }

    @After
    public void tearDown(){
        tx.close();
        session.close();
    }

    @Test
    public void checkEqualityBetweenEdgesBasedOnID() {
        Entity entity2 = entityType.create();
        Edge tinkerEdge = tx.getTinkerTraversal().V().hasId(Schema.elementId(entity2.id())).outE().next();
        EdgeElement edge2 = new EdgeElementImpl(null, tinkerEdge);

        assertEquals(edge, edge);
        assertNotEquals(edge, edge2);
    }

    @Test
    public void whenGettingTheSourceOfAnEdge_ReturnTheConceptTheEdgeComesFrom() {
        assertEquals(ConceptDowncasting.concept(entity).vertex(), edge.source());
    }

    @Test
    public void whenGettingTheTargetOfAnEdge_ReturnTheConceptTheEdgePointsTowards() {
        assertEquals(ConceptDowncasting.type(entityType).currentShard().vertex(), edge.target());
    }

    @Test
    public void whenGettingTheLabelOfAnEdge_ReturnExpectedType() {
        assertEquals(Schema.EdgeLabel.ISA.getLabel(), edge.label());
    }
}