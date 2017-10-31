/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 *
 */

package ai.grakn.engine;

import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.concept.Attribute;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Label;
import ai.grakn.concept.Thing;
import ai.grakn.engine.factory.EngineGraknTxFactory;
import ai.grakn.exception.GraknBackendException;
import ai.grakn.exception.InvalidKBException;
import com.google.common.base.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Default implementation of {@link SystemKeyspace} that uses a {@link EngineGraknTxFactory} to access a knowledge
 * base and store keyspace information.
 *
 * @author Felix Chapman
 */
public class SystemKeyspaceImpl implements SystemKeyspace {
    private static final Label KEYSPACE_ENTITY = Label.of("keyspace");

    private static final Logger LOG = LoggerFactory.getLogger(SystemKeyspace.class);
    private final ConcurrentHashMap<Keyspace, Boolean> openSpaces;
    private final EngineGraknTxFactory factory;

    private SystemKeyspaceImpl(EngineGraknTxFactory factory, boolean loadSystemSchema){
        this.factory = factory;
        this.openSpaces = new ConcurrentHashMap<>();
        if (loadSystemSchema) {
            loadSystemSchema();
        }
    }

    public static SystemKeyspace create(EngineGraknTxFactory factory, boolean loadSystemSchema) {
        return new SystemKeyspaceImpl(factory, loadSystemSchema);
    }

    @Override public void openKeyspace(Keyspace keyspace) {
         if(openSpaces.containsKey(keyspace)){
             return;
         }

        try (GraknTx graph = factory.tx(SYSTEM_KB_KEYSPACE, GraknTxType.WRITE)) {
            AttributeType<String> keyspaceName = graph.getSchemaConcept(KEYSPACE_RESOURCE);
            if (keyspaceName == null) {
                throw GraknBackendException.initializationException(keyspace);
            }
            Attribute<String> attribute = keyspaceName.putAttribute(keyspace.getValue());
            if (attribute.owner() == null) {
                graph.<EntityType>getSchemaConcept(KEYSPACE_ENTITY).addEntity().attribute(attribute);
            }
            graph.admin().commitNoLogs();
        } catch (InvalidKBException e) {
            throw new RuntimeException("Could not add keyspace [" + keyspace + "] to system graph", e);
        }
    }

    @Override public boolean containsKeyspace(Keyspace keyspace){
        try (GraknTx graph = factory.tx(SYSTEM_KB_KEYSPACE, GraknTxType.READ)) {
            return graph.getAttributeType(KEYSPACE_RESOURCE.getValue()).getAttribute(keyspace) != null;
        }
    }

    @Override public boolean deleteKeyspace(Keyspace keyspace){
        if(keyspace.equals(SYSTEM_KB_KEYSPACE)){
           return false;
        }

        try (GraknTx graph = factory.tx(SYSTEM_KB_KEYSPACE, GraknTxType.WRITE)) {
            AttributeType<String> keyspaceName = graph.getSchemaConcept(KEYSPACE_RESOURCE);
            Attribute<String> attribute = keyspaceName.getAttribute(keyspace.getValue());

            if(attribute == null) return false;
            Thing thing = attribute.owner();
            if(thing != null) thing.delete();
            attribute.delete();

            openSpaces.remove(keyspace);

            graph.admin().commitNoLogs();
        }

        return true;
    }

    @Override public Set<Keyspace> keyspaces() {
        try (GraknTx graph = factory.tx(SYSTEM_KB_KEYSPACE, GraknTxType.WRITE)) {
            AttributeType<String> keyspaceName = graph.getSchemaConcept(KEYSPACE_RESOURCE);

            return graph.<EntityType>getSchemaConcept(KEYSPACE_ENTITY).instances()
                    .flatMap(keyspace -> keyspace.attributes(keyspaceName))
                    .map(name -> (String) name.getValue())
                    .map(Keyspace::of)
                    .collect(Collectors.toSet());
        }
    }

    @Override public void loadSystemSchema() {
        Stopwatch timer = Stopwatch.createStarted();
        try (GraknTx tx = factory.tx(SYSTEM_KB_KEYSPACE, GraknTxType.WRITE)) {
            if (tx.getSchemaConcept(KEYSPACE_ENTITY) != null) {
                return;
            }
            LOG.info("Loading schema");
            loadSystemSchema(tx);
            tx.admin().commitNoLogs();
            LOG.info("Loaded system schema to system keyspace. Took: {}", timer.stop());
        } catch (Exception e) {
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