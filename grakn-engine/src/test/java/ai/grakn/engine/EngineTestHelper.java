package ai.grakn.engine;

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
    
    /**
     * Create (once only, statically) the GraknEngineConfig as per configuration file and return it.
     */
    public static synchronized GraknEngineConfig config() {
        if (config != null) {
            return config;
        }
        config = GraknEngineConfig.create();    
        try (ServerSocket socket = new ServerSocket(0)) {
            config.setConfigProperty(GraknEngineConfig.SERVER_PORT_NUMBER, String.valueOf(socket.getLocalPort()));            
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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
        server = GraknEngineServer.start(config());        
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
