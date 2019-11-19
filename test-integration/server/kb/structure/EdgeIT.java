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


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 *
 * TODO re-enable this with a proper structure for accessing factories required by tests
 *
 *
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

        session = new SessionImpl(keyspace, server.serverConfig(), new KeyspaceSchemaCache(), graph,
                new KeyspaceStatistics(), attributeCache, new ReentrantReadWriteLock());

        // create the transaction
        CacheProviderImpl cacheProvider = new CacheProviderImpl(new KeyspaceSchemaCache());
        UncomittedStatisticsDelta statisticsDelta = new UncomittedStatisticsDelta();
        ConceptObserverImpl conceptObserver = new ConceptObserverImpl(cacheProvider, statisticsDelta);

        // janus elements
        JanusGraphTransaction janusGraphTransaction = graph.newThreadBoundTransaction();
        ElementFactory elementFactory = new ElementFactory(janusGraphTransaction);

        // Grakn elements
        ConceptManagerImpl conceptManager = new ConceptManagerImpl(elementFactory, cacheProvider.getTransactionCache(), conceptObserver, new ReentrantReadWriteLock());
        TraversalPlanFactory traversalPlanFactory = new TraversalPlanFactoryImpl(conceptManager, session.config().getProperty(ConfigKey.TYPE_SHARD_THRESHOLD), session.keyspaceStatistics());
        ExecutorFactory executorFactory = new ExecutorFactory(conceptManager, null, new KeyspaceStatistics(), traversalPlanFactory);

        tx = new TransactionImpl(session, janusGraphTransaction, conceptManager, cacheProvider, statisticsDelta, executorFactory, traversalPlanFactory);
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
 **/