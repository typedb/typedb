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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import grakn.core.concept.ConceptId;
import grakn.core.concept.thing.Entity;
import grakn.core.rule.GraknTestServer;
import grakn.core.server.kb.Schema;
import grakn.core.server.kb.concept.ConceptManager;
import grakn.core.server.kb.concept.ElementFactory;
import grakn.core.server.kb.concept.EntityImpl;
import grakn.core.server.kb.concept.EntityTypeImpl;
import grakn.core.server.keyspace.Keyspace;
import grakn.core.server.session.ConceptObserver;
import grakn.core.server.session.JanusGraphFactory;
import grakn.core.server.session.Session;
import grakn.core.server.session.TransactionOLTP;
import grakn.core.server.session.cache.CacheProvider;
import grakn.core.server.session.cache.KeyspaceSchemaCache;
import grakn.core.server.statistics.KeyspaceStatistics;
import grakn.core.server.statistics.UncomittedStatisticsDelta;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class EdgeIT {

    @ClassRule
    public static final GraknTestServer server = new GraknTestServer();

    private Session session;
    private TransactionOLTP tx;
    private EntityTypeImpl entityType;
    private EntityImpl entity;
    private EdgeElement edge;

    @Before
    public void setUp(){
        Keyspace keyspace = Keyspace.of("keyspace");
        final int TIMEOUT_MINUTES_ATTRIBUTES_CACHE = 2;
        final int ATTRIBUTES_CACHE_MAX_SIZE = 10000;

        // obtain components to create sessions and transactions
        JanusGraphFactory janusGraphFactory = server.janusGraphFactory();
        StandardJanusGraph graph = janusGraphFactory.openGraph(keyspace.name());

        // create the session
        Cache<String, ConceptId> attributeCache = CacheBuilder.newBuilder()
                .expireAfterAccess(TIMEOUT_MINUTES_ATTRIBUTES_CACHE, TimeUnit.MINUTES)
                .maximumSize(ATTRIBUTES_CACHE_MAX_SIZE)
                .build();

        session = new Session(keyspace, server.serverConfig(), new KeyspaceSchemaCache(), graph,
                new KeyspaceStatistics(), attributeCache, new ReentrantReadWriteLock());

        // create the transaction
        CacheProvider cacheProvider = new CacheProvider(new KeyspaceSchemaCache());
        UncomittedStatisticsDelta statisticsDelta = new UncomittedStatisticsDelta();
        ConceptObserver conceptObserver = new ConceptObserver(cacheProvider, statisticsDelta);

        // janus elements
        JanusGraphTransaction janusGraphTransaction = graph.newThreadBoundTransaction();
        ElementFactory elementFactory = new ElementFactory(janusGraphTransaction);

        // Grakn elements
        ConceptManager conceptManager = new ConceptManager(elementFactory, cacheProvider.getTransactionCache(), conceptObserver, new ReentrantReadWriteLock());

        tx = new TransactionOLTP(session, janusGraphTransaction, conceptManager, cacheProvider, statisticsDelta);
        tx.open(TransactionOLTP.Type.WRITE);

        // Create Edge
        entityType = (EntityTypeImpl) tx.putEntityType("My Entity Type");
        entity = (EntityImpl) entityType.create();

        Edge tinkerEdge = tx.getTinkerTraversal().V().hasId(Schema.elementId(entity.id())).outE().next();
        edge = new EdgeElement(elementFactory, tinkerEdge);
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
        EdgeElement edge2 = new EdgeElement(null, tinkerEdge);

        assertEquals(edge, edge);
        assertNotEquals(edge, edge2);
    }

    @Test
    public void whenGettingTheSourceOfAnEdge_ReturnTheConceptTheEdgeComesFrom() {
        assertEquals(entity.vertex(), edge.source());
    }

    @Test
    public void whenGettingTheTargetOfAnEdge_ReturnTheConceptTheEdgePointsTowards() {
        assertEquals(entityType.currentShard().vertex(), edge.target());
    }

    @Test
    public void whenGettingTheLabelOfAnEdge_ReturnExpectedType() {
        assertEquals(Schema.EdgeLabel.ISA.getLabel(), edge.label());
    }
}