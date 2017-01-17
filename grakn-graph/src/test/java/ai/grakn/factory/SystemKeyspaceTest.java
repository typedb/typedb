package ai.grakn.factory;

import ai.grakn.Grakn;
import ai.grakn.GraknGraph;
import ai.grakn.GraknGraphFactory;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.ResourceType;
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
    public void testCollectKeyspaces() { 
    	GraknGraphFactory f1 = Grakn.factory(Grakn.IN_MEMORY, space1);
    	f1.getGraph().close();
    	GraknGraphFactory f2 = Grakn.factory(Grakn.IN_MEMORY, space2);
    	GraknGraph gf2 = f2.getGraph();
    	GraknGraphFactory f3 = Grakn.factory(Grakn.IN_MEMORY, space3);
    	GraknGraph gf3 = f3.getGraph();
    	GraknGraphFactory system = Grakn.factory(Grakn.IN_MEMORY, SystemKeyspace.SYSTEM_GRAPH_NAME);
    	GraknGraph graph = system.getGraph();
    	ResourceType<String> keyspaceName = graph.getResourceType("keyspace-name");
    	Collection<String> spaces = graph.getEntityType("keyspace").instances()
    		.stream().map(e -> 
    			e.resources(keyspaceName).iterator().next().getValue().toString()).collect(Collectors.toList());
    	assertTrue(spaces.contains(space1));
    	assertTrue(spaces.contains(space2.toLowerCase()));
    	assertTrue(spaces.contains(space3.toLowerCase()));
        assertEquals(GraknVersion.VERSION,
                graph.getResourceType("system-version").instances().iterator().next().getValue().toString());
    	gf2.close();
    	gf3.close();
    	graph.close();
    }

    @Test
    public void testUserOntology(){
        GraknGraph graph = Grakn.factory(Grakn.IN_MEMORY, SystemKeyspace.SYSTEM_GRAPH_NAME).getGraph();
        graph.showImplicitConcepts(true);

        EntityType user = graph.getEntityType("user");
        ResourceType userName = graph.getResourceType("user-name");
        ResourceType userPassword = graph.getResourceType("user-password");
        ResourceType userFirstName = graph.getResourceType("user-first-name");
        ResourceType userLastName = graph.getResourceType("user-last-name");
        ResourceType userEmail = graph.getResourceType("user-email");
        ResourceType userIsAdmin = graph.getResourceType("user-is-admin");

        //Check Plays Roles
        assertTrue(user.playsRoles().contains(
                graph.getRoleType(Schema.Resource.HAS_RESOURCE_OWNER.getName(userName.getName()).getValue())));
        assertTrue(user.playsRoles().contains(
                graph.getRoleType(Schema.Resource.HAS_RESOURCE_OWNER.getName(userPassword.getName()).getValue())));
        assertTrue(user.playsRoles().contains(
                graph.getRoleType(Schema.Resource.HAS_RESOURCE_OWNER.getName(userFirstName.getName()).getValue())));
        assertTrue(user.playsRoles().contains(
                graph.getRoleType(Schema.Resource.HAS_RESOURCE_OWNER.getName(userLastName.getName()).getValue())));
        assertTrue(user.playsRoles().contains(
                graph.getRoleType(Schema.Resource.HAS_RESOURCE_OWNER.getName(userEmail.getName()).getValue())));
        assertTrue(user.playsRoles().contains(
                graph.getRoleType(Schema.Resource.HAS_RESOURCE_OWNER.getName(userIsAdmin.getName()).getValue())));
    }

}
