package ai.grakn.engine;

import ai.grakn.engine.util.SimpleURI;
import ai.grakn.test.GraknTestSetup;

import java.io.IOException;
import java.net.ServerSocket;

/**
 * <p>
 * This class instantiates an engine server-side component once, statically until the JVM exists.
 * Any state cleanup needs to be done by tests themselves - more code, more work, but more performant
 * test suite. So all unit tests will share the same components and will be written with that in mind.  
 * </p>
 * 
 * @author borislav
 *
 */
public class EngineTestHelper {
    
    private static volatile GraknEngineConfig config = null;
    
    public static int findAvailablePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();            
        } catch (IOException e) {
            throw new RuntimeException(e);
        }        
    }
    
    /**
     * Create (once only, statically) the GraknEngineConfig as per configuration file and return it.
     */
    public static synchronized GraknEngineConfig config() {
        if (config != null) {
            return config;
        }
        config = GraknEngineConfig.create();
        config.setConfigProperty(GraknEngineConfig.SERVER_PORT_NUMBER, String.valueOf(findAvailablePort()));
        return config;
    }
    
    private static volatile GraknEngineServer server = null;
    
    /**
     * <p>
     * Creates and initializes all components for running a self-contained, embedded engine server.
     * </p>
     * <p>
     * Make sure it's invoked before any test that needs to access engine components. It's an 
     * idempotent operation - it can be invoked many times without repercussions.
     * </p> 
     */
    public static synchronized void engine() {
        if (server != null) {
            return;
        }
        GraknTestSetup.startRedisIfNeeded(new SimpleURI(config().getProperty(GraknEngineConfig.REDIS_HOST)).getPort());
        server = new GraknEngineServer(config());
        server.start();
    }

    /**
     * Similarly to {@link EngineTestHelper#engine()} it creates a test engine with the ability to write graphs to a
     * persistent backend if needed
     */
    public static synchronized void engineWithGraphs() {
        GraknTestSetup.startCassandraIfNeeded();
        engine();
    }

    /**
     * Shutdown the engine server.
     */
    public static synchronized void noEngine() {
        if (server == null) {
            return;
        }
        try {
            server.close();
        }
        finally {
            server = null;
        }
    }
}
