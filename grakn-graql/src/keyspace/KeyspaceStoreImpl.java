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

package ai.grakn.keyspace;

import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.concept.Attribute;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Label;
import ai.grakn.concept.Thing;
import ai.grakn.core.server.GraknConfig;
import ai.grakn.core.server.KeyspaceStore;
import ai.grakn.exception.GraknBackendException;
import ai.grakn.exception.InvalidKBException;
import ai.grakn.factory.EmbeddedGraknSession;
import ai.grakn.factory.TxFactoryBuilder;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import ai.grakn.util.ErrorMessage;
import com.google.common.base.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Default implementation of {@link KeyspaceStore} that uses an {@link EmbeddedGraknSession} to access a knowledge
 * base and store keyspace information.
 *
 * @author Felix Chapman
 */
public class KeyspaceStoreImpl implements KeyspaceStore {
    private static final Label KEYSPACE_ENTITY = Label.of("keyspace");
    private final static Keyspace SYSTEM_KB_KEYSPACE = Keyspace.of("graknsystem");

    private static final Logger LOG = LoggerFactory.getLogger(KeyspaceStore.class);
    private final Set<Keyspace> existingKeyspaces;
    private final EmbeddedGraknSession systemKeyspaceSession;
    private final GraknConfig config;

    public KeyspaceStoreImpl(GraknConfig config){
        this.config = config;
        this.systemKeyspaceSession = EmbeddedGraknSession.createEngineSession(SYSTEM_KB_KEYSPACE, config, TxFactoryBuilder.getInstance());
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

        try (EmbeddedGraknTx<?> tx = systemKeyspaceSession.transaction(GraknTxType.WRITE)) {
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

        try (GraknTx tx = systemKeyspaceSession.transaction(GraknTxType.READ)) {
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

        EmbeddedGraknSession session = EmbeddedGraknSession.createEngineSession(keyspace, config);
        session.close();
        try(EmbeddedGraknTx tx = session.transaction(GraknTxType.WRITE)){
            tx.closeSession();
            tx.clearGraph();
            tx.txCache().closeTx(ErrorMessage.CLOSED_CLEAR.getMessage());
        }

        return deleteReferenceInSystemKeyspace(keyspace);
    }

    private boolean deleteReferenceInSystemKeyspace(Keyspace keyspace){
        try (EmbeddedGraknTx<?> tx = systemKeyspaceSession.transaction(GraknTxType.WRITE)) {
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
        try (GraknTx graph = systemKeyspaceSession.transaction(GraknTxType.WRITE)) {
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
        try (EmbeddedGraknTx<?> tx = systemKeyspaceSession.transaction(GraknTxType.WRITE)) {
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
     * Loads the system schema inside the provided {@link GraknTx}.
     *
     * @param tx The tx to contain the system schema
     */
    private void loadSystemSchema(GraknTx tx){
        //Keyspace data
        AttributeType<String> keyspaceName = tx.putAttributeType("keyspace-name", AttributeType.DataType.STRING);
        tx.putEntityType("keyspace").key(keyspaceName);
    }
}