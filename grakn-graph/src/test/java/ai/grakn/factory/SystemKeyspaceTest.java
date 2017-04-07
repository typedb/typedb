package ai.grakn.factory;

import ai.grakn.Grakn;
import ai.grakn.GraknGraph;
import ai.grakn.GraknSession;
import ai.grakn.GraknTxType;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.ResourceType;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.util.GraknVersion;
import ai.grakn.util.Schema;
import org.junit.Test;

import java.util.Collection;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SystemKeyspaceTest {

	private final String space1 = "SystemKeyspaceTest.space1".toLowerCase();
	private final String space2 = "SystemKeyspaceTest.space2";
	private final String space3 = "SystemKeyspaceTest.space3";

    @Test
    public void whenCreatingMultipleGraphs_EnsureKeySpacesAreAddedToSystemGraph() throws GraknValidationException {
    	GraknSession f1 = Grakn.session(Grakn.IN_MEMORY, space1);
    	f1.open(GraknTxType.WRITE).close();
    	GraknSession f2 = Grakn.session(Grakn.IN_MEMORY, space2);
    	GraknGraph gf2 = f2.open(GraknTxType.WRITE);
    	GraknSession f3 = Grakn.session(Grakn.IN_MEMORY, space3);
    	GraknGraph gf3 = f3.open(GraknTxType.WRITE);
    	GraknSession system = Grakn.session(Grakn.IN_MEMORY, SystemKeyspace.SYSTEM_GRAPH_NAME);
    	GraknGraph graph = system.open(GraknTxType.WRITE);
    	ResourceType<String> keyspaceName = graph.getResourceType("keyspace-name");
    	Collection<String> spaces = graph.getEntityType("keyspace").instances()
    		.stream().map(e -> 
    			e.resources(keyspaceName).iterator().next().getValue().toString()).collect(Collectors.toList());

        assertTrue("Keyspace [" + space1 + "] is missing from system graph", spaces.contains(space1));
        assertTrue("Keyspace [" + space2 + "] is missing from system graph", spaces.contains(space2.toLowerCase()));
        assertTrue("Keyspace [" + space3 + "] is missing from system graph", spaces.contains(space3.toLowerCase()));

        assertEquals(GraknVersion.VERSION,
                graph.getResourceType("system-version").instances().iterator().next().getValue().toString());

        gf2.close();
    	gf3.close();
    	graph.close();
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

}
