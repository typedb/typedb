package ai.grakn.factory;

import java.util.Collection;
import java.util.stream.Collectors;

import org.junit.Test;

import ai.grakn.Grakn;
import ai.grakn.GraknGraph;
import ai.grakn.GraknGraphFactory;
import ai.grakn.concept.ResourceType;
import org.junit.Assert;

public class SystemKeyspaceTest {

	private String space1 = "SystemKeyspaceTest.space1".toLowerCase();
	private String space2 = "SystemKeyspaceTest.space2";
	private String space3 = "SystemKeyspaceTest.space3";
	
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
    	Assert.assertTrue(spaces.contains(space1));
    	Assert.assertTrue(spaces.contains(space2.toLowerCase()));
    	Assert.assertTrue(spaces.contains(space3.toLowerCase()));
    	gf2.close();
    	gf3.close();
    	graph.close();
    }
    
}
