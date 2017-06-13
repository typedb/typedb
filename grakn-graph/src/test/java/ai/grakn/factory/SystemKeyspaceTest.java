package ai.grakn.factory;

import ai.grakn.Grakn;
import ai.grakn.GraknGraph;
import ai.grakn.GraknSession;
import ai.grakn.GraknTxType;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.ResourceType;
import ai.grakn.exception.InvalidGraphException;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.GraknVersion;
import ai.grakn.util.Schema;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SystemKeyspaceTest {

    private final static String TEST_CONFIG = "../conf/test/tinker/grakn.properties";
    private final static Properties TEST_PROPERTIES = new Properties();

    @BeforeClass
    public static void setupProperties(){
        try (InputStream in = new FileInputStream(TEST_CONFIG)){
            TEST_PROPERTIES.load(in);
        } catch (IOException e) {
            throw new RuntimeException(ErrorMessage.INVALID_PATH_TO_CONFIG.getMessage(TEST_CONFIG), e);
        }
    }

    @Test
    public void whenCreatingMultipleGraphs_EnsureKeySpacesAreAddedToSystemGraph() throws InvalidGraphException {
        String [] keyspaces = {"s1", "s2", "s3"};

        Set<GraknGraph> graphs = buildGraphs(keyspaces);
        Set<String> spaces = getSystemKeyspaces();

        for (String keyspace : keyspaces) {
            assertTrue("Keyspace [" + keyspace + "] is missing from system graph", spaces.contains(keyspace));
            assertTrue(SystemKeyspace.containsKeyspace(keyspace));
        }

        graphs.forEach(GraknGraph::close);
    }

    @Test
    public void ensureVersionIsLoadedIntoSystemGraph(){
        try(GraknSession system = Grakn.session(Grakn.IN_MEMORY, SystemKeyspace.SYSTEM_GRAPH_NAME)){
            try(GraknGraph graph = system.open(GraknTxType.WRITE)) {
                assertEquals(GraknVersion.VERSION,
                        graph.getResourceType("system-version").instances().iterator().next().getValue().toString());
            }
        }
    }

    @Test
    public void ensureUserOntologyIsLoadedIntoSystemGraph(){
        GraknGraph graph = Grakn.session(Grakn.IN_MEMORY, SystemKeyspace.SYSTEM_GRAPH_NAME).open(GraknTxType.WRITE);
        graph.showImplicitConcepts(true);

        EntityType user = graph.getEntityType("user");
        ResourceType userName = graph.getResourceType("user-name");
        ResourceType userPassword = graph.getResourceType("user-password");
        ResourceType userFirstName = graph.getResourceType("user-first-name");
        ResourceType userLastName = graph.getResourceType("user-last-name");
        ResourceType userEmail = graph.getResourceType("user-email");
        ResourceType userIsAdmin = graph.getResourceType("user-is-admin");

        //Check Plays
        assertTrue(user.plays().contains(
                graph.getRoleType(Schema.ImplicitType.KEY_OWNER.getLabel(userName.getLabel()).getValue())));
        assertTrue(user.plays().contains(
                graph.getRoleType(Schema.ImplicitType.HAS_OWNER.getLabel(userPassword.getLabel()).getValue())));
        assertTrue(user.plays().contains(
                graph.getRoleType(Schema.ImplicitType.HAS_OWNER.getLabel(userFirstName.getLabel()).getValue())));
        assertTrue(user.plays().contains(
                graph.getRoleType(Schema.ImplicitType.HAS_OWNER.getLabel(userLastName.getLabel()).getValue())));
        assertTrue(user.plays().contains(
                graph.getRoleType(Schema.ImplicitType.HAS_OWNER.getLabel(userEmail.getLabel()).getValue())));
        assertTrue(user.plays().contains(
                graph.getRoleType(Schema.ImplicitType.HAS_OWNER.getLabel(userIsAdmin.getLabel()).getValue())));

        graph.close();
    }

    @Test
    public void whenClearingGraphs_EnsureTheyAreDeletedFromSystemGraph(){
        String [] keyspaces = {"g1", "g2", "g3"};

        //Create graphs to begin with
        Set<GraknGraph> graphs = buildGraphs(keyspaces);
        graphs.forEach(GraknGraph::close);

        //Delete a graph entirely
        GraknGraph deletedGraph = graphs.iterator().next();
        deletedGraph.admin().delete();
        graphs.remove(deletedGraph);

        //Rebuild Graphs Using Keyspaces From Systenm Graph
        Set<String> systemKeyspaces = getSystemKeyspaces();
        Set<GraknGraph> systemGraphs = buildGraphs(systemKeyspaces.toArray(new String[systemKeyspaces.size()]));

        //Check only 2 graphs have been built
        assertEquals(graphs, systemGraphs);
        assertFalse(SystemKeyspace.containsKeyspace(deletedGraph.getKeyspace()));
    }

    @Test
    public void whenInstantiatingSystemGraphInMultipleThreads_InterruptedExceptionIsNotThrown() throws InterruptedException {

        // Dereference the factory in our mocked system keyspace
        SystemKeyspaceMock.dereference();

        int numberThreads = 100;
        ExecutorService executorService = Executors.newFixedThreadPool(numberThreads);

        Collection<Callable<Integer>> threads = new ArrayList<>();
        for(int i = 0; i < numberThreads; i ++){
            int finalI = i;
            threads.add(() -> {

                // Implicitly instantiate system keyspace
                SystemKeyspaceMock.initialise(new TinkerInternalFactory(SystemKeyspace.SYSTEM_GRAPH_NAME, Grakn.IN_MEMORY, TEST_PROPERTIES));

                // Check the system graph exists
                SystemKeyspaceMock.containsKeyspace(SystemKeyspace.SYSTEM_GRAPH_NAME);

                // Close the mock
                SystemKeyspaceMock.close();

                return finalI;
            });
        }

        try {
            // Open system graph concurrently
            Collection<Future<Integer>> futures = executorService.invokeAll(threads);
            for(Future future:futures){
                future.get();
            }
        } catch (InterruptedException | ExecutionException e) {

            // an exception was thrown, fail the test
            fail("Exception was thrown instantiating system keyspace " + getFullStackTrace(e));

            throw new RuntimeException(e);
        } finally {
            // Dereference the factory in our mocked system keyspace
            SystemKeyspaceMock.dereference();
        }
    }

    private Set<GraknGraph> buildGraphs(String ... keyspaces){
        return Arrays.stream(keyspaces).
                map(k -> Grakn.session(Grakn.IN_MEMORY, k).open(GraknTxType.WRITE)).
                collect(Collectors.toSet());
    }

    private Set<String> getSystemKeyspaces(){
        GraknSession system = Grakn.session(Grakn.IN_MEMORY, SystemKeyspace.SYSTEM_GRAPH_NAME);
        try(GraknGraph graph = system.open(GraknTxType.WRITE)) {
            ResourceType<String> keyspaceName = graph.getResourceType("keyspace-name");
            return graph.getEntityType("keyspace").instances().
                    stream().
                    map(e -> e.resources(keyspaceName).iterator().next().getValue().toString()).
                    collect(Collectors.toSet());
        }
    }
}
