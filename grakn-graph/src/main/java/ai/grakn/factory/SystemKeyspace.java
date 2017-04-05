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
 */

package ai.grakn.factory;

import ai.grakn.GraknGraph;
import ai.grakn.GraknTxType;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.TypeLabel;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.util.GraknVersion;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * <p>
 * Manages the system keyspace.
 * </p>
 * 
 * <p>
 * Used to populate the system ontology the first time the system keyspace
 * is created.
 * </p>
 * 
 * <p>
 * Used to populate the system keyspace with all newly create keyspaces as a
 * user opens them. We have no way to determining whether a keyspace with a
 * given name already exists or not. We maintain the list in our Grakn system
 * keyspace. An element is added to that list when there is an attempt to create
 * a graph from a factory bound to the keyspace name. The list is simply the
 * instances of the system entity type 'keyspace'. Nothing is ever removed from
 * that list. The set of known keyspaces is maintained in a static map so we
 * don't connect to the system keyspace every time a factory produces a new
 * graph. That means that we can't have several different factories (e.g. Titan
 * and in-memory Tinkerpop) at the same time sharing keyspace names. We can't
 * identify the factory builder by engineUrl and config because we don't know
 * what's inside the config, which is residing remotely at the engine!
 * </p>
 * 
 * @author borislav, fppt
 *
 * @param <T>
 */
public class SystemKeyspace<T extends Graph> {
    // This will eventually be configurable and obtained the same way the factory is obtained
    // from engine. For now, we just make sure Engine and Core use the same system keyspace name.
    // If there is a more natural home for this constant, feel free to put it there! (Boris)
    public static final String SYSTEM_GRAPH_NAME = "graknSystem";
    public static final String SYSTEM_ONTOLOGY_FILE = "system.gql";
    public static final TypeLabel KEYSPACE_ENTITY = TypeLabel.of("keyspace");
    public static final TypeLabel KEYSPACE_RESOURCE = TypeLabel.of("keyspace-name");
    public static final TypeLabel SYSTEM_VERSION = TypeLabel.of("system-version");

    protected final Logger LOG = LoggerFactory.getLogger(SystemKeyspace.class);

    private static final ConcurrentHashMap<String, Boolean> openSpaces = new ConcurrentHashMap<>();
    private final InternalFactory<T> factory;

    public SystemKeyspace(InternalFactory<T> factory){
        this.factory = factory;
    }

    /**
     * Notify that we just opened a keyspace with the same engineUrl & config.
     */
    SystemKeyspace<T> keyspaceOpened(String keyspace) {
        openSpaces.computeIfAbsent(keyspace, name -> {
            try (GraknGraph graph = factory.open(GraknTxType.WRITE)) {
                ResourceType<String> keyspaceName = graph.getType(KEYSPACE_RESOURCE);
                Resource<String> resource = keyspaceName.putResource(keyspace);
                if (resource.owner() == null) {
                    graph.<EntityType>getType(KEYSPACE_ENTITY).addEntity().resource(resource);
                }
                graph.admin().commitNoLogs();
            } catch (GraknValidationException e) {
                throw new RuntimeException("Could not add keyspace [" + keyspace + "] to system graph", e);
            }
            return true;
        });
        return this;
    }

    /**
     * Load the system ontology into a newly created system keyspace. Because the ontology
     * only consists of types, the inserts are idempotent and it is safe to load it
     * multiple times.
     */
    void loadSystemOntology() {
        try (GraknGraph graph = factory.open(GraknTxType.WRITE)) {
            if (graph.getType(KEYSPACE_ENTITY) != null) {
                return;
            }
            ClassLoader loader = this.getClass().getClassLoader();
            String query;
            try (BufferedReader buffer = new BufferedReader(new InputStreamReader(loader.getResourceAsStream(SYSTEM_ONTOLOGY_FILE), StandardCharsets.UTF_8))) {
                query = buffer.lines().collect(Collectors.joining("\n"));
            }
            graph.graql().parse(query).execute();
            graph.getResourceType("system-version").putResource(GraknVersion.VERSION);
            graph.admin().commitNoLogs();
            LOG.info("Loaded system ontology to system keyspace.");
        } catch(IOException |GraknValidationException |NullPointerException e) {
            e.printStackTrace(System.err);
            LOG.error("Could not load system ontology. The error was: " + e);
        }
    }
}