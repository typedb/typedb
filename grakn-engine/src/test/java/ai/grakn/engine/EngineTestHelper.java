package ai.grakn.engine;

import ai.grakn.GraknConfigKey;
import ai.grakn.engine.data.RedisWrapper;
import ai.grakn.engine.factory.EngineGraknTxFactory;
import ai.grakn.engine.lock.LockProvider;
import ai.grakn.engine.tasks.manager.TaskManager;
import ai.grakn.engine.util.EngineID;
import ai.grakn.test.GraknTestSetup;
import com.codahale.metrics.MetricRegistry;
import redis.clients.jedis.Jedis;
import redis.clients.util.Pool;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.ExecutorService;

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
public class EngineTestHelper extends GraknCreator {
    
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
        config.setConfigProperty(GraknConfigKey.SERVER_PORT, findAvailablePort());
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
        server = EngineTestHelper.cleanGraknEngineServer(config());
        server.start();
    }

    /**
     * Similarly to {@link EngineTestHelper#engine()} it creates a test engine with the ability to write {@link ai.grakn.GraknTx} to a
     * persistent backend if needed
     */
    public static synchronized void engineWithKBs() {
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

    public static synchronized GraknEngineServer cleanGraknEngineServer(GraknEngineConfig config) {
        return EngineTestHelper.cleanGraknEngineServer(config,redisWrapper(config));
    }

    public static synchronized GraknEngineServer cleanGraknEngineServer(GraknEngineConfig config, RedisWrapper redisWrapper) {
        EngineGraknTxFactory factory = engineGraknTxFactory(config);
        Pool<Jedis> jedisPool = redisWrapper.getJedisPool();
        LockProvider lockProvider = lockProvider(jedisPool);
        MetricRegistry metricRegistry = metricRegistry();
        EngineID engineID = engineId();
        TaskManager taskManager = taskManager(config, factory, jedisPool, lockProvider, engineID, metricRegistry);
        GraknEngineStatus graknEngineStatus = graknEngineStatus();
        ExecutorService executorService = executorService();
        HttpHandler httpHandler = new HttpHandler(config, sparkService(), factory, metricRegistry, graknEngineStatus, taskManager, executorService);
        return new GraknEngineServer(config, taskManager, factory, lockProvider, graknEngineStatus, redisWrapper, executorService, httpHandler, engineID);
    }

}
