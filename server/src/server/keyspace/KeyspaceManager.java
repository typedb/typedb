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

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * KeyspaceManager used to store all existing keyspaces inside Grakn system keyspace.
 */
public class KeyspaceManager {
    private static final Label KEYSPACE_RESOURCE = Label.of("keyspace-name");
    private static final Label KEYSPACE_ENTITY = Label.of("keyspace");
    private final static KeyspaceImpl SYSTEM_KB_KEYSPACE = KeyspaceImpl.of("graknsystem");

    private static final Logger LOG = LoggerFactory.getLogger(KeyspaceManager.class);
    private final Set<KeyspaceImpl> existingKeyspaces;
    private final SessionImpl systemKeyspaceSession;
    private final StandardJanusGraph graph;

    public KeyspaceManager(JanusGraphFactory janusGraphFactory, Config config) {
        KeyspaceCache keyspaceCache = new KeyspaceCache();
        this.graph = janusGraphFactory.openGraph(SYSTEM_KB_KEYSPACE.name());
        this.systemKeyspaceSession = new SessionImpl(SYSTEM_KB_KEYSPACE, config, keyspaceCache, graph, new KeyspaceStatistics(), CacheBuilder.newBuilder().build(), new ReentrantReadWriteLock());
        this.existingKeyspaces = ConcurrentHashMap.newKeySet();
    }

    /**
     * Add new keyspace in graknsystem if it does not exist already.
     *
     * @param keyspace The new KeyspaceImpl we have just created
     */
    public void putKeyspace(KeyspaceImpl keyspace) {
        if (containsKeyspace(keyspace)) return;

        try (TransactionOLTP tx = systemKeyspaceSession.transaction().write()) {
            AttributeType<String> keyspaceName = tx.getSchemaConcept(KEYSPACE_RESOURCE);
            if (keyspaceName == null) {
                throw GraknServerException.initializationException(keyspace);
            }
            Attribute<String> attribute = keyspaceName.create(keyspace.name());
            tx.<EntityType>getSchemaConcept(KEYSPACE_ENTITY).create().has(attribute);
            tx.commit();

            // add to cache
            existingKeyspaces.add(keyspace);
        } catch (InvalidKBException e) {
            throw new RuntimeException("Could not add keyspace [" + keyspace + "] to system graph", e);
        }
    }

    public void closeStore() {
        this.systemKeyspaceSession.close();
        this.graph.close();
    }

    private boolean containsKeyspace(KeyspaceImpl keyspace) {
        //Check local cache
        if (existingKeyspaces.contains(keyspace)) {
            return true;
        }

        try (TransactionOLTP tx = systemKeyspaceSession.transaction().read()) {
            boolean keyspaceExists = (tx.getAttributeType(KEYSPACE_RESOURCE.getValue()).attribute(keyspace) != null);
            if (keyspaceExists) existingKeyspaces.add(keyspace);
            return keyspaceExists;
        }
    }

    public void deleteKeyspace(KeyspaceImpl keyspace) {
        if (keyspace.equals(SYSTEM_KB_KEYSPACE)) {
            throw GraknServerException.create("It is not possible to delete the Grakn system keyspace.");
        }
        deleteReferenceInSystemKeyspace(keyspace);
    }

    private void deleteReferenceInSystemKeyspace(KeyspaceImpl keyspace) {
        try (TransactionOLTP tx = systemKeyspaceSession.transaction().write()) {
            AttributeType<String> keyspaceName = tx.getSchemaConcept(KEYSPACE_RESOURCE);
            Attribute<String> attribute = keyspaceName.attribute(keyspace.name());
            if (attribute == null) {
                throw GraknServerException.create("It is not possible to delete keyspace [" + keyspace.name() + "] as it does not exist.");
            }
            Thing thing = attribute.owner();
            thing.delete();
            attribute.delete();

            existingKeyspaces.remove(keyspace);
            tx.commit();
        }
    }

    public Set<KeyspaceImpl> keyspaces() {
        try (TransactionOLTP graph = systemKeyspaceSession.transaction().write()) {
            AttributeType<String> keyspaceName = graph.getSchemaConcept(KEYSPACE_RESOURCE);

            return graph.<EntityType>getSchemaConcept(KEYSPACE_ENTITY).instances()
                    .flatMap(keyspace -> keyspace.attributes(keyspaceName))
                    .map(name -> (String) name.value())
                    .map(KeyspaceImpl::of)
                    .collect(Collectors.toSet());
        }
    }

    public void loadSystemSchema() {
        Stopwatch timer = Stopwatch.createStarted();
        try (TransactionOLTP tx = systemKeyspaceSession.transaction().write()) {
            if (tx.getSchemaConcept(KEYSPACE_ENTITY) != null) {
                LOG.info("System schema has been previously loaded");
                return;
            }
            LOG.info("Loading schema");
            loadSystemSchema(tx);
            tx.commit();
            LOG.info("Loaded system schema to system keyspace. Took: {}", timer.stop());
        } catch (RuntimeException e) {
            LOG.error("Error while loading system schema in {}. The error was: {}", timer.stop(), e.getMessage(), e);
            throw e;
        }
    }

    /**
     * Loads the system schema using the provided Transaction.
     */
    private void loadSystemSchema(TransactionOLTP tx) {
        AttributeType<String> keyspaceName = tx.putAttributeType("keyspace-name", AttributeType.DataType.STRING);
        tx.putEntityType("keyspace").key(keyspaceName);
    }
}