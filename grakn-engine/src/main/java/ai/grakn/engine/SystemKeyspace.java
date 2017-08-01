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

package ai.grakn.engine;

import ai.grakn.GraknGraph;
import ai.grakn.GraknTxType;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Label;
import ai.grakn.concept.Thing;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.engine.factory.EngineGraknGraphFactory;
import ai.grakn.exception.GraphOperationException;
import ai.grakn.exception.InvalidGraphException;
import ai.grakn.graph.admin.GraknAdmin;
import ai.grakn.util.GraknVersion;
import com.google.common.base.Stopwatch;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * graph. That means that we can't have several different factories (e.g. Janus
 * and in-memory Tinkerpop) at the same time sharing keyspace names. We can't
 * identify the factory builder by engineUrl and config because we don't know
 * what's inside the config, which is residing remotely at the engine!
 * </p>
 * 
 * @author borislav, fppt
 *
 */
public class SystemKeyspace {
    // This will eventually be configurable and obtained the same way the factory is obtained
    // from engine. For now, we just make sure Engine and Core use the same system keyspace name.
    // If there is a more natural home for this constant, feel free to put it there! (Boris)
    public static final String SYSTEM_GRAPH_NAME = "graknSystem";
    private static final String SYSTEM_VERSION = "system-version";
    public static final Label KEYSPACE_ENTITY = Label.of("keyspace");
    public static final Label KEYSPACE_RESOURCE = Label.of("keyspace-name");

    private static final Logger LOG = LoggerFactory.getLogger(SystemKeyspace.class);
    private final ConcurrentHashMap<String, Boolean> openSpaces;
    private final EngineGraknGraphFactory factory;

    public SystemKeyspace(EngineGraknGraphFactory factory){
        this(factory, true);
    }

    public SystemKeyspace(EngineGraknGraphFactory factory, boolean loadSystemOntology){
        this.factory = factory;
        this.openSpaces = new ConcurrentHashMap<>();
        if (loadSystemOntology) {
            loadSystemOntology();
        }
    }

    /**
     * Notify that we just opened a keyspace with the same engineUrl & config.
     */
     public boolean ensureKeyspaceInitialised(String keyspace) {
         if(openSpaces.containsKey(keyspace)){
             return true;
         }

        try (GraknGraph graph = factory.getGraph(SYSTEM_GRAPH_NAME, GraknTxType.WRITE)) {
            ResourceType<String> keyspaceName = graph.getOntologyConcept(KEYSPACE_RESOURCE);
            Resource<String> resource = keyspaceName.putResource(keyspace);
            if (resource.owner() == null) {
                graph.<EntityType>getOntologyConcept(KEYSPACE_ENTITY).addEntity().resource(resource);
            }
            graph.admin().commitNoLogs();
        } catch (InvalidGraphException e) {
            throw new RuntimeException("Could not add keyspace [" + keyspace + "] to system graph", e);
        }

        return true;
    }

    /**
     * Checks if the keyspace exists in the system. The persisted graph is checked each time because the graph
     * may have been deleted in another JVM.
     *
     * @param keyspace The keyspace which might be in the system
     * @return true if the keyspace is in the system
     */
    public boolean containsKeyspace(String keyspace){
        try (GraknGraph graph = factory.getGraph(SYSTEM_GRAPH_NAME, GraknTxType.READ)) {
            return graph.getResourceType(KEYSPACE_RESOURCE.getValue()).getResource(keyspace) != null;
        }
    }

    /**
     * This is called when a graph is deleted via {@link GraknAdmin#delete()}.
     * This removes the keyspace of the deleted graph from the system graph
     *
     * @param keyspace the keyspace to be removed from the system graph
     */
    public boolean deleteKeyspace(String keyspace){
        if(keyspace.equals(SYSTEM_GRAPH_NAME)){
           return false;
        }

        try (GraknGraph graph = factory.getGraph(SYSTEM_GRAPH_NAME, GraknTxType.WRITE)) {
            ResourceType<String> keyspaceName = graph.getOntologyConcept(KEYSPACE_RESOURCE);
            Resource<String> resource = keyspaceName.getResource(keyspace);

            if(resource == null) return false;
            Thing thing = resource.owner();
            if(thing != null) thing.delete();
            resource.delete();

            openSpaces.remove(keyspace);

            graph.admin().commitNoLogs();
        }

        return true;
    }

    /**
     * Load the system ontology into a newly created system keyspace. Because the ontology
     * only consists of types, the inserts are idempotent and it is safe to load it
     * multiple times.
     */
    public void loadSystemOntology() {
        Stopwatch timer = Stopwatch.createStarted();
        try (GraknGraph graph = factory.getGraph(SYSTEM_GRAPH_NAME, GraknTxType.WRITE)) {
            if (graph.getOntologyConcept(KEYSPACE_ENTITY) != null) {
                checkVersion(graph);
                return;
            }
            LOG.info("No other version found, loading ontology for version {}", GraknVersion.VERSION);
            loadSystemOntology(graph);
            graph.getResourceType(SYSTEM_VERSION).putResource(GraknVersion.VERSION);
            graph.admin().commitNoLogs();
            LOG.info("Loaded system ontology to system keyspace. Took: {}", timer.stop());
        } catch (Exception e) {
            LOG.error("Error while loading system ontology in {}. The error was: {}", timer.stop(), e.getMessage(), e);
            throw e;
        }
    }

    /**
     * Helper method which checks the version persisted in the system keyspace with the version of the running grakn
     * instance
     *
     * @throws ai.grakn.exception.GraphOperationException when the versions do not match
     */
    private void checkVersion(GraknGraph graph){
        Resource existingVersion = graph.getResourceType(SYSTEM_VERSION).instances().iterator().next();
        if(!GraknVersion.VERSION.equals(existingVersion.getValue())) {
            throw GraphOperationException.versionMistmatch(existingVersion);
        } else {
            LOG.info("Found version {}", existingVersion.getValue());
        }
    }

    /**
     * Loads the system ontology inside the provided grakn graph.
     *
     * @param graph The graph to contain the system ontology
     */
    private void loadSystemOntology(GraknGraph graph){
        //Keyspace data
        ResourceType<String> keyspaceName = graph.putResourceType("keyspace-name", ResourceType.DataType.STRING);
        graph.putEntityType("keyspace").key(keyspaceName);

        //User Data
        ResourceType<String> userName = graph.putResourceType("user-name", ResourceType.DataType.STRING);
        ResourceType<String> userPassword = graph.putResourceType("user-password", ResourceType.DataType.STRING);
        ResourceType<String> userPasswordSalt = graph.putResourceType("user-password-salt", ResourceType.DataType.STRING);
        ResourceType<String> userFirstName = graph.putResourceType("user-first-name", ResourceType.DataType.STRING);
        ResourceType<String> userLastName = graph.putResourceType("user-last-name", ResourceType.DataType.STRING);
        ResourceType<String> userEmail = graph.putResourceType("user-email", ResourceType.DataType.STRING);
        ResourceType<Boolean> userIsAdmin = graph.putResourceType("user-is-admin", ResourceType.DataType.BOOLEAN);

        graph.putEntityType("user").key(userName).
                resource(userPassword).
                resource(userPasswordSalt).
                resource(userFirstName).
                resource(userLastName).
                resource(userEmail).
                resource(userIsAdmin);

        //System Version
        graph.putResourceType("system-version", ResourceType.DataType.STRING);
    }
}