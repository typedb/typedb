package ai.grakn.engine;

import ai.grakn.engine.util.SimpleURI;
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
        config.setConfigProperty(GraknEngineConfig.SERVER_PORT_NUMBER,
                String.valueOf(findAvailablePort()));
        config.setConfigProperty(GraknEngineConfig.REDIS_HOST,
                new SimpleURI("localhost", findAvailablePort()).toString());
        return config;
    }
}