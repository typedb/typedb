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
 *
 */

package grakn.core.rule;

import grakn.core.common.config.Config;
import grakn.core.common.config.ConfigKey;
import grakn.core.graph.graphdb.database.StandardJanusGraph;
import grakn.core.kb.server.AttributeManager;
import grakn.core.kb.server.ShardManager;
import grakn.core.kb.server.TransactionProvider;
import grakn.core.kb.server.cache.KeyspaceSchemaCache;
import grakn.core.kb.server.keyspace.Keyspace;
import grakn.core.kb.server.statistics.KeyspaceStatistics;
import grakn.core.server.keyspace.KeyspaceImpl;
import grakn.core.server.session.AttributeManagerImpl;
import grakn.core.server.session.HadoopGraphFactory;
import grakn.core.server.session.JanusGraphFactory;
import grakn.core.server.session.SessionImpl;
import grakn.core.server.session.ShardManagerImpl;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph;

import java.util.UUID;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class SessionUtil {

    /**
     * Re-create a session as a real SessionFactory instance would, but inject a TestTransactionProvider into
     * the SessionImpl.
     * This means tests using this method to create sessions can safely downcast transactions received
     * to `TestTransaction`, which provide access to various pieces of state required in the tests
     * @param config
     * @return
     */
    public static SessionImpl serverlessSessionWithNewKeyspace(Config config) {
        Keyspace randomKeyspace = new KeyspaceImpl("a" + UUID.randomUUID().toString().replaceAll("-", ""));
        JanusGraphFactory janusGraphFactory = new JanusGraphFactory(config);
        HadoopGraphFactory hadoopGraphFactory = new HadoopGraphFactory(config);

        StandardJanusGraph graph = janusGraphFactory.openGraph(randomKeyspace.name());
        KeyspaceSchemaCache cache = new KeyspaceSchemaCache();
        KeyspaceStatistics keyspaceStatistics = new KeyspaceStatistics();
        AttributeManager attributeManager = new AttributeManagerImpl();
        ShardManager shardManager = new ShardManagerImpl();
        ReadWriteLock graphLock = new ReentrantReadWriteLock();
        HadoopGraph hadoopGraph = hadoopGraphFactory.getGraph(randomKeyspace);

        long typeShardThreshold = config.getProperty(ConfigKey.TYPE_SHARD_THRESHOLD);
        TransactionProvider transactionProvider = new TestTransactionProvider(graph, hadoopGraph, cache, keyspaceStatistics, attributeManager, graphLock, typeShardThreshold);
        return new SessionImpl(randomKeyspace, transactionProvider, cache, graph, keyspaceStatistics, attributeManager, shardManager, graphLock);
    }
}
