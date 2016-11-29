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
    private final static String TEST_CONFIG = "../conf/test/tinker/grakn-tinker.properties";
    private final static String ENGINE_URL = "rubbish";

    @Test
    public void testCollectKeyspaces() { 
    	GraknGraphFactory f1 = Grakn.factory(Grakn.IN_MEMORY, "space1");
    	//FactoryBuilder.getFactory("space1", ENGINE_URL, TEST_CONFIG);
    	f1.getGraph().close();
    	GraknGraphFactory f2 = Grakn.factory(Grakn.IN_MEMORY, "space2"); //FactoryBuilder.getFactory("space2", ENGINE_URL, TEST_CONFIG);
    	GraknGraph gf2 = f2.getGraph();
    	GraknGraphFactory f3 = Grakn.factory(Grakn.IN_MEMORY, "space3"); //FactoryBuilder.getFactory("space3", ENGINE_URL, TEST_CONFIG);
    	GraknGraph gf3 = f3.getGraph();
    	GraknGraphFactory system = Grakn.factory(Grakn.IN_MEMORY, SystemKeyspace.SYSTEM_GRAPH_NAME); //FactoryBuilder.getFactory(SystemKeyspace.SYSTEM_GRAPH_NAME, ENGINE_URL, TEST_CONFIG);
    	GraknGraph graph = system.getGraph();
    	ResourceType<String> keyspaceName = graph.getResourceType("keyspace-name");
    	Collection<String> spaces = graph.getEntityType("keyspace").instances()
    		.stream().map(e -> 
    			e.resources(keyspaceName).iterator().next().getValue().toString()).collect(Collectors.toList());
    	Assert.assertEquals(3, spaces.size());
    	Assert.assertTrue(spaces.contains("space1"));
    	Assert.assertTrue(spaces.contains("space2"));
    	Assert.assertTrue(spaces.contains("space3"));
    	gf2.close();
    	gf3.close();
    	graph.close();
    }
    
}
