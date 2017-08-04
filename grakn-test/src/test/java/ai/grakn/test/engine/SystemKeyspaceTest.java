package ai.grakn.test.engine;

import ai.grakn.Grakn;
import ai.grakn.GraknGraph;
import ai.grakn.GraknTxType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.ResourceType;
import ai.grakn.engine.SystemKeyspace;
import ai.grakn.exception.GraphOperationException;
import ai.grakn.test.EngineContext;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.GraknVersion;
import ai.grakn.util.Schema;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static ai.grakn.engine.SystemKeyspace.SYSTEM_GRAPH_NAME;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SystemKeyspaceTest {

    private final Function<String, GraknGraph> engineFactoryGraphProvider = (k) -> engine.server().factory().getGraph(k, GraknTxType.WRITE);
    private final Function<String, GraknGraph> externalFactoryGraphProvider = (k) -> Grakn.session(engine.uri(), k).open(GraknTxType.WRITE);

    @ClassRule
    public static final EngineContext engine = EngineContext.startInMemoryServer();

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @After
    public void cleanSystemKeySpaceGraph(){
        try (GraknGraph graph = engine.server().factory().getGraph(SYSTEM_GRAPH_NAME, GraknTxType.WRITE)){
            graph.getEntityType("keyspace").instances().forEach(Concept::delete);
            graph.commit();
        }
    }

    @Test
    public void whenOpeningGraphBuiltUsingDifferentVersionOfGrakn_Throw(){
        try {
            String rubbishVersion = "Hippo Version";

            //Insert fake version number
            setVersionInSystemGraph(rubbishVersion);

            expectedException.expect(GraphOperationException.class);
            expectedException.expectMessage(ErrorMessage.VERSION_MISMATCH.getMessage(GraknVersion.VERSION, rubbishVersion));

            //This simulates accessing the system for the first time
            new SystemKeyspace(engine.server().factory());
        } finally {
            // reset real version
            setVersionInSystemGraph(GraknVersion.VERSION);
        }
    }

    @Test
    public void whenCreatingGraphsUsingEngineFactory_EnsureKeySpacesAreAddedToSystemGraph() {
        String [] keyspaces = {"s1", "s2", "s3"};

        Set<GraknGraph> graphs = buildGraphs(engineFactoryGraphProvider, keyspaces);
        Set<String> spaces = getSystemKeyspaces();

        for (String keyspace : keyspaces) {
            assertTrue("Keyspace [" + keyspace + "] is missing from system graph", spaces.contains(keyspace));
            assertTrue(engine.server().factory().systemKeyspace().containsKeyspace(keyspace));
        }

        graphs.forEach(GraknGraph::close);
    }

    @Test
    public void whenCreatingGraphsUsingExternalFactory_EnsureKeySpacesAreAddedToSystemGraph() {
        String [] keyspaces = {"s1", "s2", "s3"};

        Set<GraknGraph> graphs = buildGraphs(externalFactoryGraphProvider, keyspaces);
        Set<String> spaces = getSystemKeyspaces();

        for (String keyspace : keyspaces) {
            assertTrue("Keyspace [" + keyspace + "] is missing from system graph", spaces.contains(keyspace));
            assertTrue(engine.server().factory().systemKeyspace().containsKeyspace(keyspace));
        }

        graphs.forEach(GraknGraph::close);
    }

    @Test
    public void whenConnectingToSystemGraph_EnsureUserOntologyIsLoaded(){
        try(GraknGraph graph = engine.server().factory().getGraph(SYSTEM_GRAPH_NAME, GraknTxType.WRITE)) {

            EntityType user = graph.getEntityType("user");
            ResourceType userName = graph.getResourceType("user-name");
            ResourceType userPassword = graph.getResourceType("user-password");
            ResourceType userFirstName = graph.getResourceType("user-first-name");
            ResourceType userLastName = graph.getResourceType("user-last-name");
            ResourceType userEmail = graph.getResourceType("user-email");
            ResourceType userIsAdmin = graph.getResourceType("user-is-admin");

            //Check Plays
            assertTrue(user.plays().contains(
                    graph.getRole(Schema.ImplicitType.KEY_OWNER.getLabel(userName.getLabel()).getValue())));
            assertTrue(user.plays().contains(
                    graph.getRole(Schema.ImplicitType.HAS_OWNER.getLabel(userPassword.getLabel()).getValue())));
            assertTrue(user.plays().contains(
                    graph.getRole(Schema.ImplicitType.HAS_OWNER.getLabel(userFirstName.getLabel()).getValue())));
            assertTrue(user.plays().contains(
                    graph.getRole(Schema.ImplicitType.HAS_OWNER.getLabel(userLastName.getLabel()).getValue())));
            assertTrue(user.plays().contains(
                    graph.getRole(Schema.ImplicitType.HAS_OWNER.getLabel(userEmail.getLabel()).getValue())));
            assertTrue(user.plays().contains(
                    graph.getRole(Schema.ImplicitType.HAS_OWNER.getLabel(userIsAdmin.getLabel()).getValue())));

            graph.close();
        }
    }

    @Test
    public void whenClearingGraphsUsingExternalFactory_EnsureKeyspacesAreDeletedFromSystemGraph(){
        String[] keyspaces = {"g1", "g2", "g3"};

        //Create graphs to begin with
        Set<GraknGraph> graphs = buildGraphs(externalFactoryGraphProvider, keyspaces);
        graphs.forEach(GraknGraph::close);

        //Delete a graph entirely
        GraknGraph deletedGraph = graphs.iterator().next();
        deletedGraph.admin().delete();
        graphs.remove(deletedGraph);

        // Get system keyspaces
        Set<String> systemKeyspaces = getSystemKeyspaces();

        //Check only 2 graphs have been built
        for(GraknGraph graph:graphs){
            assertTrue("Contains correct keyspace", systemKeyspaces.contains(graph.getKeyspace()));
        }
        assertFalse(engine.server().factory().systemKeyspace().containsKeyspace(deletedGraph.getKeyspace()));
    }

    @Test
    public void whenClearingGraphsUsingEngineFactory_EnsureKeyspacesAreDeletedFromSystemGraph(){
        String[] keyspaces = {"g1", "g2", "g3"};

        //Create graphs to begin with
        Set<GraknGraph> graphs = buildGraphs(engineFactoryGraphProvider, keyspaces);
        graphs.forEach(GraknGraph::close);

        //Delete a graph entirely
        GraknGraph deletedGraph = graphs.iterator().next();
        deletedGraph.admin().delete();
        graphs.remove(deletedGraph);

        // Get system keyspaces
        Set<String> systemKeyspaces = getSystemKeyspaces();

        //Check only 2 graphs have been built
        for(GraknGraph graph:graphs){
            assertTrue("Contains correct keyspace", systemKeyspaces.contains(graph.getKeyspace()));
        }
        assertFalse(engine.server().factory().systemKeyspace().containsKeyspace(deletedGraph.getKeyspace()));
    }

    private void setVersionInSystemGraph(String version){
        String versionResourceType = "system-version";

        //Insert fake version number
        try(GraknGraph graph = engine.server().factory().getGraph(SYSTEM_GRAPH_NAME, GraknTxType.WRITE)){
            //Delete old version number
            graph.getResourceType(versionResourceType).instances().forEach(Concept::delete);
            //Add Fake Version
            graph.getResourceType(versionResourceType).putResource(version);
            graph.commit();
        }
    }

    private Set<GraknGraph> buildGraphs(Function<String, GraknGraph> graphProvider, String ... keyspaces){
        return Arrays.stream(keyspaces)
                .map(graphProvider)
                .collect(Collectors.toSet());
    }

    private Set<String> getSystemKeyspaces(){
        try(GraknGraph graph = engine.server().factory().getGraph(SYSTEM_GRAPH_NAME, GraknTxType.READ)){
            ResourceType<String> keyspaceName = graph.getResourceType("keyspace-name");
            return graph.getEntityType("keyspace").instances().
                    stream().
                    map(e -> e.resources(keyspaceName).iterator().next().getValue().toString()).
                    collect(Collectors.toSet());
        }
    }
}
