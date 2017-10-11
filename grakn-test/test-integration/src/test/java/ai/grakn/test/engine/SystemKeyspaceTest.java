package ai.grakn.test.engine;

import ai.grakn.Grakn;
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.concept.AttributeType;
import ai.grakn.Keyspace;
import ai.grakn.concept.Concept;
import ai.grakn.concept.EntityType;
import ai.grakn.test.EngineContext;
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

import static ai.grakn.engine.SystemKeyspace.SYSTEM_KB_KEYSPACE;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SystemKeyspaceTest {

    private final Function<String, GraknTx> engineFactoryGraphProvider = (k) -> engine.server().factory().tx(k, GraknTxType.WRITE);
    private final Function<String, GraknTx> externalFactoryGraphProvider = (k) -> Grakn.session(engine.uri(), k).open(GraknTxType.WRITE);

    @ClassRule
    public static final EngineContext engine = EngineContext.createWithInMemoryRedis();

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @After
    public void cleanSystemKeySpaceGraph(){
        try (GraknTx graph = engine.server().factory().tx(SYSTEM_KB_KEYSPACE, GraknTxType.WRITE)){
            graph.getEntityType("keyspace").instances().forEach(Concept::delete);
            graph.commit();
        }
    }

    @Test
    public void whenCreatingGraphsUsingEngineFactory_EnsureKeySpacesAreAddedToSystemGraph() {
        String [] keyspaces = {"s1", "s2", "s3"};

        Set<GraknTx> graphs = buildGraphs(engineFactoryGraphProvider, keyspaces);
        Set<String> spaces = getSystemKeyspaces();

        for (String keyspace : keyspaces) {
            assertTrue("Keyspace [" + keyspace + "] is missing from system graph", spaces.contains(keyspace));
            assertTrue(engine.server().factory().systemKeyspace().containsKeyspace(Keyspace.of(keyspace)));
        }

        graphs.forEach(GraknTx::close);
    }

    @Test
    public void whenCreatingGraphsUsingExternalFactory_EnsureKeySpacesAreAddedToSystemGraph() {
        String [] keyspaces = {"s1", "s2", "s3"};

        Set<GraknTx> graphs = buildGraphs(externalFactoryGraphProvider, keyspaces);
        Set<String> spaces = getSystemKeyspaces();

        for (String keyspace : keyspaces) {
            assertTrue("Keyspace [" + keyspace + "] is missing from system graph", spaces.contains(keyspace));
            assertTrue(engine.server().factory().systemKeyspace().containsKeyspace(Keyspace.of(keyspace)));
        }

        graphs.forEach(GraknTx::close);
    }

    @Test
    public void whenConnectingToSystemGraph_EnsureUserSchemaIsLoaded(){
        try(GraknTx graph = engine.server().factory().tx(SYSTEM_KB_KEYSPACE, GraknTxType.WRITE)) {

            EntityType user = graph.getEntityType("user");
            AttributeType userName = graph.getAttributeType("user-name");
            AttributeType userPassword = graph.getAttributeType("user-password");
            AttributeType userFirstName = graph.getAttributeType("user-first-name");
            AttributeType userLastName = graph.getAttributeType("user-last-name");
            AttributeType userEmail = graph.getAttributeType("user-email");
            AttributeType userIsAdmin = graph.getAttributeType("user-is-admin");

            //Check Plays
            assertTrue(user.plays().anyMatch(role -> role.equals(
                    graph.getRole(Schema.ImplicitType.KEY_OWNER.getLabel(userName.getLabel()).getValue()))));
            assertTrue(user.plays().anyMatch(role -> role.equals(
                    graph.getRole(Schema.ImplicitType.HAS_OWNER.getLabel(userPassword.getLabel()).getValue()))));
            assertTrue(user.plays().anyMatch(role -> role.equals(
                    graph.getRole(Schema.ImplicitType.HAS_OWNER.getLabel(userFirstName.getLabel()).getValue()))));
            assertTrue(user.plays().anyMatch(role -> role.equals(
                    graph.getRole(Schema.ImplicitType.HAS_OWNER.getLabel(userLastName.getLabel()).getValue()))));
            assertTrue(user.plays().anyMatch(role -> role.equals(
                    graph.getRole(Schema.ImplicitType.HAS_OWNER.getLabel(userEmail.getLabel()).getValue()))));
            assertTrue(user.plays().anyMatch(role -> role.equals(
                    graph.getRole(Schema.ImplicitType.HAS_OWNER.getLabel(userIsAdmin.getLabel()).getValue()))));

            graph.close();
        }
    }

    @Test
    public void whenClearingGraphsUsingExternalFactory_EnsureKeyspacesAreDeletedFromSystemGraph(){
        String[] keyspaces = {"g1", "g2", "g3"};

        //Create graphs to begin with
        Set<GraknTx> graphs = buildGraphs(externalFactoryGraphProvider, keyspaces);
        graphs.forEach(GraknTx::close);

        //Delete a graph entirely
        GraknTx deletedGraph = graphs.iterator().next();
        deletedGraph.admin().delete();
        graphs.remove(deletedGraph);

        // Get system keyspaces
        Set<String> systemKeyspaces = getSystemKeyspaces();

        //Check only 2 graphs have been built
        for(GraknTx graph:graphs){
            assertTrue("Contains correct keyspace", systemKeyspaces.contains(graph.getKeyspace().getValue()));
        }
        assertFalse(engine.server().factory().systemKeyspace().containsKeyspace(deletedGraph.getKeyspace()));
    }

    @Test
    public void whenClearingGraphsUsingEngineFactory_EnsureKeyspacesAreDeletedFromSystemGraph(){
        String[] keyspaces = {"g1", "g2", "g3"};

        //Create graphs to begin with
        Set<GraknTx> graphs = buildGraphs(engineFactoryGraphProvider, keyspaces);
        graphs.forEach(GraknTx::close);

        //Delete a graph entirely
        GraknTx deletedGraph = graphs.iterator().next();
        deletedGraph.admin().delete();
        graphs.remove(deletedGraph);

        // Get system keyspaces
        Set<String> systemKeyspaces = getSystemKeyspaces();

        //Check only 2 graphs have been built
        for(GraknTx graph:graphs){
            assertTrue("Contains correct keyspace", systemKeyspaces.contains(graph.getKeyspace().getValue()));
        }
        assertFalse(engine.server().factory().systemKeyspace().containsKeyspace(deletedGraph.getKeyspace()));
    }

    private void setVersionInSystemGraph(String version){
        String versionResourceType = "system-version";

        //Insert fake version number
        try(GraknTx graph = engine.server().factory().tx(SYSTEM_KB_KEYSPACE, GraknTxType.WRITE)){
            //Delete old version number
            graph.getAttributeType(versionResourceType).instances().forEach(Concept::delete);
            //Add Fake Version
            graph.getAttributeType(versionResourceType).putAttribute(version);
            graph.commit();
        }
    }

    private Set<GraknTx> buildGraphs(Function<String, GraknTx> graphProvider, String ... keyspaces){
        return Arrays.stream(keyspaces)
                .map(graphProvider)
                .collect(Collectors.toSet());
    }

    private Set<String> getSystemKeyspaces(){
        try(GraknTx graph = engine.server().factory().tx(SYSTEM_KB_KEYSPACE, GraknTxType.READ)){
            AttributeType<String> keyspaceName = graph.getAttributeType("keyspace-name");
            return graph.getEntityType("keyspace").instances().
                    map(e -> e.attributes(keyspaceName).iterator().next().getValue().toString()).
                    collect(Collectors.toSet());
        }
    }
}
