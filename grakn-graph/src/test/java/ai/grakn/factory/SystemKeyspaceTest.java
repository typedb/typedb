package ai.grakn.factory;

import ai.grakn.Grakn;
import ai.grakn.GraknGraph;
import ai.grakn.GraknSession;
import ai.grakn.GraknTxType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.ResourceType;
import ai.grakn.exception.GraphOperationException;
import ai.grakn.exception.InvalidGraphException;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.GraknVersion;
import ai.grakn.util.Schema;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SystemKeyspaceTest {

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Before
    public void cleanSystemKeySpaceGraph(){
        try(GraknSession system = Grakn.session(Grakn.IN_MEMORY, SystemKeyspace.SYSTEM_GRAPH_NAME)) {
            try (GraknGraph graph = system.open(GraknTxType.WRITE)) {
                graph.getEntityType("keyspace").instances().forEach(Concept::delete);
            }
        }
    }

    @Test
    public void whenOpeningGraphBuiltUsingDifferentVersionOfGrakn_Throw(){
        String versionResourceType = "system-version";
        String rubbishVersion = "Hippo Version";
        //Insert fake version number
        try(GraknSession system = Grakn.session(Grakn.IN_MEMORY, SystemKeyspace.SYSTEM_GRAPH_NAME)) {
            try(GraknGraph graph = system.open(GraknTxType.WRITE)){
                //Delete old version number
                graph.getResourceType(versionResourceType).instances().forEach(Concept::delete);
                //Add Fake Version
                graph.getResourceType(versionResourceType).putResource(rubbishVersion);
                graph.commit();
            }
        }

        expectedException.expect(GraphOperationException.class);
        expectedException.expectMessage(ErrorMessage.VERSION_MISMATCH.getMessage(GraknVersion.VERSION, rubbishVersion));

        try(GraknSession session = Grakn.session(Grakn.IN_MEMORY, "mykeyspace")){
            session.open(GraknTxType.WRITE);
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
