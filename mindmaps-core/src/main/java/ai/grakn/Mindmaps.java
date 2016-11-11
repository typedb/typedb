package ai.grakn;

import java.util.HashMap;
import java.util.Map;

/**
 * <p>
 * Main entry point to connect to a Mindmaps knowledge graph. To connect to a knowledge graph, first
 * make sure you have a Mindmaps Engine server running. You can also instantiate an in-memory knowledge graph
 * for testing or experimentation purposes. 
 * </p>
 * <p>
 * To establish a connection, you first need to obtain a {@link MindmapsGraphFactory} by calling
 * the {@link #factory(String, String)} method. A {@link MindmapsGraphFactory} to a given physical 
 * location and specific database instance within that location. Once you've instantiated a factory, 
 * you can obtain as many concurrent graph connection, represented by the {@link MindmapsGraph}
 * interface as you would like.
 * </p>
 */
public class Mindmaps {
    public static final String DEFAULT_URI = "localhost:4567";
    private static final String ENGINE_CONTROLLED_IMPLEMENTATION = "io.mindmaps.factory.MindmapsGraphFactoryPersistent";
    
    public static final String IN_MEMORY = "in-memory";
    private static final String IN_MEMORY_IMPLEMENTATION = "io.mindmaps.factory.MindmapsGraphFactoryInMemory";
    
    private static final Map<String, MindmapsGraphFactory> clients = new HashMap<>();

    private static <F extends MindmapsGraphFactory> F loadImplementation(String className,
    																	 String location,
    																	 String keyspace) {
    	try {
    		@SuppressWarnings("unchecked")
			Class<F> cl = (Class<F>)Class.forName(className);
    		return cl.getConstructor(String.class, String.class).newInstance(keyspace, location);
    	}
    	catch (Exception ex) {
    		throw new RuntimeException(ex);
    	}
    }
    
    /**
     * <p>
     * Obtain the {@link MindmapsGraphFactory} for a given location and keyspace.
     * </p>
     * 
     * @param location The location from which to create the graph. For an in memory graph, 
     * use the {@link #IN_MEMORY} constant defined in this class. For the default, localhost
     * Mindmaps Engine location, use the {@link #DEFAULT_URI} constant provided in this class.
     * @param keyspace THe keyspace, or database name, where the knowledge graph is stored. A given
     * database server will support multiple database instances. You need to explicitly name
     * the instance to be used. In general, if one doesn't exist, it will be created for you. 
     * @return A factory instance that can produce concurrent connection to the knowledge graph.
     */
    public static MindmapsGraphFactory factory(String location, String keyspace) {
        String finalKeyspace = keyspace.toLowerCase();
        String key = location + finalKeyspace;
        String factoryClassname = IN_MEMORY.equals(location) ?IN_MEMORY_IMPLEMENTATION : ENGINE_CONTROLLED_IMPLEMENTATION;
        return clients.computeIfAbsent(key, (k) -> loadImplementation(factoryClassname, location, finalKeyspace));
    }
}