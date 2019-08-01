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

package grakn.core.server.keyspace;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Row;
import com.google.common.base.Stopwatch;
import com.google.common.cache.CacheBuilder;
import grakn.core.common.config.Config;
import grakn.core.concept.Label;
import grakn.core.concept.thing.Attribute;
import grakn.core.concept.thing.Thing;
import grakn.core.concept.type.AttributeType;
import grakn.core.concept.type.EntityType;
import grakn.core.server.exception.GraknServerException;
import grakn.core.server.exception.InvalidKBException;
import grakn.core.server.session.JanusGraphFactory;
import grakn.core.server.session.SessionImpl;
import grakn.core.server.session.TransactionOLTP;
import grakn.core.server.session.cache.KeyspaceCache;
import grakn.core.server.statistics.KeyspaceStatistics;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * KeyspaceManager used to store all existing keyspaces inside Grakn system keyspace.
 */
public class KeyspaceManager {
    private static final Logger LOG = LoggerFactory.getLogger(KeyspaceManager.class);
    private final Set<KeyspaceImpl> existingKeyspaces;

    public KeyspaceManager(JanusGraphFactory janusGraphFactory, Config config) {
        this.existingKeyspaces = ConcurrentHashMap.newKeySet();
        Cluster storage = Cluster.builder().addContactPoint("localhost").withPort(9042).build();
        LOG.info("listing keyspaces...");
        List<KeyspaceMetadata> keyspaces = storage.connect().getCluster().getMetadata().getKeyspaces();
        for (KeyspaceMetadata keyspace: keyspaces) {
            LOG.info("- " + keyspace.getName());
        }
        LOG.info("keyspaces listed.");
    }

    /**
     * Add new keyspace in graknsystem if it does not exist already.
     *
     * @param keyspace The new KeyspaceImpl we have just created
     */
    // TODO: rewrite
    public void putKeyspace(KeyspaceImpl keyspace) {
        if (!containsKeyspace(keyspace)) {
            existingKeyspaces.add(keyspace);
        }
    }

    public void closeStore() {
    }

    // TODO: rewrite
    private boolean containsKeyspace(KeyspaceImpl keyspace) {
        return existingKeyspaces.contains(keyspace);
    }

    // TODO: rewrite
    public void deleteKeyspace(KeyspaceImpl keyspace) {
        existingKeyspaces.remove(keyspace);
    }

    // TODO: rewrite: describe keyspaces
    public Set<KeyspaceImpl> keyspaces() {
        return existingKeyspaces;
    }
}