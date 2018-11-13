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

import grakn.core.server.Transaction;
import grakn.core.server.Keyspace;
import grakn.core.concept.Attribute;
import grakn.core.concept.AttributeType;
import grakn.core.concept.EntityType;
import grakn.core.concept.Label;
import grakn.core.concept.Thing;
import grakn.core.util.GraknConfig;
import grakn.core.server.exception.GraknBackendException;
import grakn.core.server.exception.InvalidKBException;
import grakn.core.server.session.SessionImpl;
import grakn.core.server.session.TransactionFactoryBuilder;
import grakn.core.server.kb.internal.TransactionImpl;
import grakn.core.util.ErrorMessage;
import com.google.common.base.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Default implementation of {@link KeyspaceStore} that uses an {@link SessionImpl} to access a knowledge
 * base and store keyspace information.
 *
 */
public class KeyspaceStoreImpl implements KeyspaceStore {
    private static final Label KEYSPACE_ENTITY = Label.of("keyspace");
    private final static Keyspace SYSTEM_KB_KEYSPACE = Keyspace.of("graknsystem");

    private static final Logger LOG = LoggerFactory.getLogger(KeyspaceStore.class);
    private final Set<Keyspace> existingKeyspaces;
    private final SessionImpl systemKeyspaceSession;
    private final GraknConfig config;

    public KeyspaceStoreImpl(GraknConfig config){
        this.config = config;
        this.systemKeyspaceSession = SessionImpl.createEngineSession(SYSTEM_KB_KEYSPACE, config, TransactionFactoryBuilder.getInstance());
        this.existingKeyspaces = ConcurrentHashMap.newKeySet();
    }

    /**
     * Logs a new {@link Keyspace} to the {@link KeyspaceStore}.
     *
     * @param keyspace The new {@link Keyspace} we have just created
     */
    @Override
    public void addKeyspace(Keyspace keyspace){
        if(containsKeyspace(keyspace)) return;

        try (TransactionImpl<?> tx = systemKeyspaceSession.transaction(Transaction.Type.WRITE)) {
            AttributeType<String> keyspaceName = tx.getSchemaConcept(KEYSPACE_RESOURCE);
            if (keyspaceName == null) {
                throw GraknBackendException.initializationException(keyspace);
            }
            Attribute<String> attribute = keyspaceName.create(keyspace.getValue());
            if (attribute.owner() == null) {
                tx.<EntityType>getSchemaConcept(KEYSPACE_ENTITY).create().has(attribute);
            }
            tx.commit();

            // add to cache
            existingKeyspaces.add(keyspace);
        } catch (InvalidKBException e) {
            throw new RuntimeException("Could not add keyspace [" + keyspace + "] to system graph", e);
        }
    }

    @Override
    public void closeStore() {
        this.systemKeyspaceSession.close();
    }

    @Override
    public boolean containsKeyspace(Keyspace keyspace){
        //Check the local cache to see which keyspaces we already have open
        if(existingKeyspaces.contains(keyspace)){
            return true;
        }

        try (Transaction tx = systemKeyspaceSession.transaction(Transaction.Type.READ)) {
            boolean keyspaceExists = (tx.getAttributeType(KEYSPACE_RESOURCE.getValue()).attribute(keyspace) != null);
            if(keyspaceExists) existingKeyspaces.add(keyspace);
            return keyspaceExists;
        }
    }

    @Override
    public boolean deleteKeyspace(Keyspace keyspace){
        if(keyspace.equals(SYSTEM_KB_KEYSPACE)){
           return false;
        }

        SessionImpl session = SessionImpl.createEngineSession(keyspace, config);
        session.close();
        try(TransactionImpl tx = session.transaction(Transaction.Type.WRITE)){
            tx.closeSession();
            tx.clearGraph();
            tx.txCache().closeTx(ErrorMessage.CLOSED_CLEAR.getMessage());
        }

        return deleteReferenceInSystemKeyspace(keyspace);
    }

    private boolean deleteReferenceInSystemKeyspace(Keyspace keyspace){
        try (TransactionImpl<?> tx = systemKeyspaceSession.transaction(Transaction.Type.WRITE)) {
            AttributeType<String> keyspaceName = tx.getSchemaConcept(KEYSPACE_RESOURCE);
            Attribute<String> attribute = keyspaceName.attribute(keyspace.getValue());

            if(attribute == null) return false;
            Thing thing = attribute.owner();
            if(thing != null) thing.delete();
            attribute.delete();

            existingKeyspaces.remove(keyspace);

            tx.commit();
        }
        return true;
    }

    @Override
    public Set<Keyspace> keyspaces() {
        try (Transaction graph = systemKeyspaceSession.transaction(Transaction.Type.WRITE)) {
            AttributeType<String> keyspaceName = graph.getSchemaConcept(KEYSPACE_RESOURCE);

            return graph.<EntityType>getSchemaConcept(KEYSPACE_ENTITY).instances()
                    .flatMap(keyspace -> keyspace.attributes(keyspaceName))
                    .map(name -> (String) name.value())
                    .map(Keyspace::of)
                    .collect(Collectors.toSet());
        }
    }

    @Override
    public void loadSystemSchema() {
        Stopwatch timer = Stopwatch.createStarted();
        try (TransactionImpl<?> tx = systemKeyspaceSession.transaction(Transaction.Type.WRITE)) {
            if (tx.getSchemaConcept(KEYSPACE_ENTITY) != null) {
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
     * Loads the system schema inside the provided {@link Transaction}.
     *
     * @param tx The tx to contain the system schema
     */
    private void loadSystemSchema(Transaction tx){
        //Keyspace data
        AttributeType<String> keyspaceName = tx.putAttributeType("keyspace-name", AttributeType.DataType.STRING);
        tx.putEntityType("keyspace").key(keyspaceName);
    }
}