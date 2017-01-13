package ai.grakn.factory;

import ai.grakn.GraknGraph;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.TypeName;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.util.GraknVersion;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
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
 * @author borislav
 *
 * @param <M>
 * @param <T>
 */
public class SystemKeyspace<M extends GraknGraph, T extends Graph> {
    // This will eventually be configurable and obtained the same way the factory is obtained
    // from engine. For now, we just make sure Engine and Core use the same system keyspace name.
    // If there is a more natural home for this constant, feel free to put it there! (Boris)
    public static final String SYSTEM_GRAPH_NAME = "graknSystem";
    public static final String SYSTEM_ONTOLOGY_FILE = "system.gql";
    public static final TypeName KEYSPACE_ENTITY = TypeName.of("keyspace");
    public static final TypeName KEYSPACE_RESOURCE = TypeName.of("keyspace-name");
    public static final TypeName SYSTEM_VERSION = TypeName.of("system-version");

    protected final Logger LOG = LoggerFactory.getLogger(SystemKeyspace.class);

    private static final ConcurrentHashMap<String, Boolean> openSpaces = new ConcurrentHashMap<>();
    private final InternalFactory<M, T> factory;

    public SystemKeyspace(InternalFactory<M, T> factory){
        this.factory = factory;
    }

    /**
     * Notify that we just opened a keyspace with the same engineUrl & config.
     */
    public SystemKeyspace<M, T> keyspaceOpened(String keyspace) {
        openSpaces.computeIfAbsent(keyspace, name -> {
            try (GraknGraph graph = factory.getGraph(false)) {
                ResourceType<String> keyspaceName = graph.getType(KEYSPACE_RESOURCE);
                Resource<String> resource = keyspaceName.getResource(keyspace);
                if (resource == null) {
                    resource = keyspaceName.putResource(keyspace);
                }
                if (resource.owner() == null) {
                    graph.<EntityType>getType(KEYSPACE_ENTITY).addEntity().hasResource(resource);
                }
                graph.commit();
            } catch (GraknValidationException e) {
                e.printStackTrace();
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
    public void loadSystemOntology() {
        try (GraknGraph graph = factory.getGraph(false)) {
            if (graph.getType(KEYSPACE_ENTITY) != null) {
                return;
            }
            ClassLoader loader = this.getClass().getClassLoader();
            String query;
            try (BufferedReader buffer = new BufferedReader(new InputStreamReader(loader.getResourceAsStream(SYSTEM_ONTOLOGY_FILE)))) {
                query = buffer.lines().collect(Collectors.joining("\n"));
            }
            LOG.info("System ontology is " + query);
            graph.graql().parse(query).execute();
            graph.getResourceType("system-version").putResource(GraknVersion.VERSION);
            graph.commit();
            LOG.info("Loaded system ontology to system keyspace.");
        }
        catch(IOException|GraknValidationException|NullPointerException e) {
            e.printStackTrace(System.err);
            LOG.error("Could not load system ontology. The error was: " + e);
        }
    }
}