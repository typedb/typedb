/*
 * Copyright (C) 2020 Grakn Labs
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

package grakn.core.rule;

import grakn.core.common.config.Config;
import grakn.core.graph.graphdb.database.StandardJanusGraph;
import grakn.core.kb.keyspace.AttributeManager;
import grakn.core.kb.keyspace.KeyspaceSchemaCache;
import grakn.core.kb.keyspace.KeyspaceStatistics;
import grakn.core.kb.keyspace.ShardManager;
import grakn.core.kb.server.TransactionProvider;
import grakn.core.kb.server.keyspace.Keyspace;
import grakn.core.keyspace.AttributeManagerImpl;
import grakn.core.keyspace.KeyspaceStatisticsImpl;
import grakn.core.keyspace.ShardManagerImpl;
import grakn.core.server.keyspace.KeyspaceImpl;
import grakn.core.server.session.HadoopGraphFactory;
import grakn.core.server.session.JanusGraphFactory;
import grakn.core.server.session.SessionImpl;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph;

import java.util.UUID;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * For testing, we sometimes ONLY start cassandra, without starting the full Grakn Server
 * This is to enable injecting and retrieving Grakn components we wish to test.
 * This also means we can no longer use the `GraknTestServer` to spawn new sessions.
 * This helper class essentially behave as the test SessionFactory, allowing us to inject different collaborators
 * and also retrieve `TestTransaction` instances in return
 *
 * TODO may be better of as an explicit `TestSessionFactory` or as a Builder pattern
 */
public class SessionUtil {

    /**
     * Re-create a session as a real SessionFactory instance would, but inject a TestTransactionProvider into
     * the SessionImpl.
     * This means tests using this method to create sessions can safely downcast transactions received
     * to `TestTransaction`, which provide access to various pieces of state required in the tests
     *
     * @param mockServerConfig
     * @return
     */
    public static SessionImpl serverlessSessionWithNewKeyspace(Config mockServerConfig, long typeShardThreshold) {
        String newKeyspaceName = "a" + UUID.randomUUID().toString().replaceAll("-", "");
        JanusGraphFactory janusGraphFactory = new JanusGraphFactory(mockServerConfig);
        return serverlessSession(mockServerConfig, janusGraphFactory, newKeyspaceName, typeShardThreshold);
    }

    public static SessionImpl serverlessSessionWithNewKeyspace(Config mockServerConfig) {
        String newKeyspaceName = "a" + UUID.randomUUID().toString().replaceAll("-", "");
        JanusGraphFactory janusGraphFactory = new JanusGraphFactory(mockServerConfig);
        long typeShardThreshold = 250000;
        return serverlessSession(mockServerConfig, janusGraphFactory, newKeyspaceName, typeShardThreshold);
    }

    /**
     * Create a new keyspace, with an injected janusGraphFactory
     */
    public static SessionImpl serverlessSessionWithNewKeyspace(Config mockServerConfig, JanusGraphFactory janusGraphFactory) {
        String newKeyspaceName = "a" + UUID.randomUUID().toString().replaceAll("-", "");
        long typeShardThreshold = 250000;
        return serverlessSession(mockServerConfig, janusGraphFactory, newKeyspaceName, typeShardThreshold);
    }

    /**
     * Create a new keyspace with injected KeyspaceStatistics
     */
    public static SessionImpl serverlessSessionWithNewKeyspace(Config mockServerConfig, KeyspaceStatistics keyspaceStatistics) {
        String newKeyspaceName = "a" + UUID.randomUUID().toString().replaceAll("-", "");
        Keyspace randomKeyspace = new KeyspaceImpl(newKeyspaceName);
        JanusGraphFactory janusGraphFactory = new JanusGraphFactory(mockServerConfig);

        HadoopGraphFactory hadoopGraphFactory = new HadoopGraphFactory(mockServerConfig);
        StandardJanusGraph graph = janusGraphFactory.openGraph(newKeyspaceName);
        KeyspaceSchemaCache cache = new KeyspaceSchemaCache();
        AttributeManager attributeManager = new AttributeManagerImpl();
        ShardManager shardManager = new ShardManagerImpl();
        ReadWriteLock graphLock = new ReentrantReadWriteLock();
        HadoopGraph hadoopGraph = hadoopGraphFactory.getGraph(randomKeyspace);

        long typeShardThreshold = 250000; // TODO decide if this belongs in the mockServerConfig or not
        TransactionProvider transactionProvider = new TestTransactionProvider(graph, hadoopGraph, cache, keyspaceStatistics, attributeManager, graphLock, typeShardThreshold);
        return new SessionImpl(randomKeyspace, transactionProvider, cache, graph, keyspaceStatistics, attributeManager, shardManager);
    }

    /**
     * Open a keyspace with specific name and janus graph factory
     */
    public static SessionImpl serverlessSession(Config mockServerConfig, JanusGraphFactory janusGraphFactory, String keyspaceName, long typeShardThreshold) {
        Keyspace randomKeyspace = new KeyspaceImpl(keyspaceName);
        HadoopGraphFactory hadoopGraphFactory = new HadoopGraphFactory(mockServerConfig);
        StandardJanusGraph graph = janusGraphFactory.openGraph(randomKeyspace.name());
        KeyspaceSchemaCache cache = new KeyspaceSchemaCache();
        KeyspaceStatistics keyspaceStatistics = new KeyspaceStatisticsImpl();
        AttributeManager attributeManager = new AttributeManagerImpl();
        ShardManager shardManager = new ShardManagerImpl();
        ReadWriteLock graphLock = new ReentrantReadWriteLock();
        HadoopGraph hadoopGraph = hadoopGraphFactory.getGraph(randomKeyspace);

        TransactionProvider transactionProvider = new TestTransactionProvider(graph, hadoopGraph, cache, keyspaceStatistics, attributeManager, graphLock, typeShardThreshold);
        return new SessionImpl(randomKeyspace, transactionProvider, cache, graph, keyspaceStatistics, attributeManager, shardManager);
    }

    public static SessionImpl serverlessSession(Config serverConfig, String keyspaceName) {
        JanusGraphFactory janusGraphFactory = new JanusGraphFactory(serverConfig);
        return serverlessSession(serverConfig, janusGraphFactory, keyspaceName, 250000);
    }
}
