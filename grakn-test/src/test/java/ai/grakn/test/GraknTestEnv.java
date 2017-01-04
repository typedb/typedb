package ai.grakn.test;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import ai.grakn.GraknGraph;
import ai.grakn.engine.util.ConfigProperties;
import ai.grakn.factory.GraphFactory;
import ai.grakn.factory.SystemKeyspace;
import org.slf4j.LoggerFactory;

import ai.grakn.Grakn;
import ai.grakn.GraknGraphFactory;
import ai.grakn.engine.GraknEngineServer;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import jline.internal.Log;

import static ai.grakn.graql.Graql.var;

/**
 * <p>
 * Contains utility methods and statically initialized environment variables to control
 * Grakn unit tests. 
 * </p>
 * 
 * @author borislav
 *
 */
public interface GraknTestEnv {
    static final String CONFIG = System.getProperty("grakn.test-profile");
    static AtomicBoolean CASSANDRA_RUNNING = new AtomicBoolean(false);
    static AtomicBoolean HTTP_RUNNING = new AtomicBoolean(false);
    
    public static void hideLogs() {
        Logger logger = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        logger.setLevel(Level.OFF);
    }

    public static void ensureEngineRunning() throws Exception {
    	// To ensure consistency b/w test profiles and configuration files, when not using Titan
    	// for a unit tests in an IDE, add the following option:
    	// -Dgrakn.conf=../conf/test/tinker/grakn-engine.properties
    	//
    	// When using titan, add -Dgrakn.test-profile=titan
    	//
    	// The reason is that the default configuration of Grakn uses the Titan factory while the default
    	// test profile is tinker: so when running a unit test within an IDE without any extra parameters,
    	// we end up wanting to use the TitanFactory but without starting Cassandra first.
    	
//        System.setProperty(ConfigProperties.CONFIG_FILE_SYSTEM_PROPERTY, ConfigProperties.TEST_CONFIG_FILE);
    	
        if (CASSANDRA_RUNNING.compareAndSet(false, true) && usingTitan()) {
            startEmbeddedCassandra();
            System.out.println("CASSANDRA RUNNING.");
        }
        
        if(HTTP_RUNNING.compareAndSet(false, true)) {
            GraknEngineServer.startHTTP();
            Properties properties = GraphFactory.getInstance().configurationProperties();           
            new SystemKeyspace(null, properties).loadSystemOntology();
        }
    }

    public static void clearGraphs() {
        // Drop all keyspaces
        GraknGraph systemGraph = GraphFactory.getInstance().getGraph(ConfigProperties.SYSTEM_GRAPH_NAME);
        systemGraph.graql().match(var("x").isa("keyspace-name"))
                .execute()
                .forEach(x -> x.values().forEach(y -> {
                    String name = y.asResource().getValue().toString();
                    GraknGraph graph = GraphFactory.getInstance().getGraph(name);
                    graph.clear();
                }));

        // Drop the system keyspaces too
        systemGraph.clear();  	
    }
    
    public static void shutdownEngine()  {
        if(HTTP_RUNNING.compareAndSet(true, false)) {
            GraknEngineServer.stopHTTP();
            // The Spark framework we are using kicks off a shutdown process in a separate
            // thread and there is not way to detect when it is finished. The only option
            // we have is to "wait a while" (Boris).
            try {Thread.sleep(5000);} catch(InterruptedException ex) { Log.info("Thread sleep interrupted."); }
        }
        // There is no way to stop the embedded Casssandra, no such API offered.
    }

    public static GraknGraphFactory factoryWithNewKeyspace() {
        String keyspace;
        if (usingOrientDB()) {
            keyspace = "memory";
        } else {
            // Embedded Casandra has problems dropping keyspaces that start with a number
            keyspace = "a"+UUID.randomUUID().toString().replaceAll("-", "");
        }
        return Grakn.factory(Grakn.DEFAULT_URI, keyspace);
    }

    public static void startEmbeddedCassandra() {
        try {
            // We have to use reflection here because the cassandra dependency is only included when testing the titan profile.
            Class cl = Class.forName("org.cassandraunit.utils.EmbeddedCassandraServerHelper");
            hideLogs();

            //noinspection unchecked
            cl.getMethod("startEmbeddedCassandra", String.class).invoke(null, "cassandra-embedded.yaml");
            hideLogs();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static boolean usingTinker() {
        return "tinker".equals(CONFIG);
    }

    public static boolean usingTitan() {
        return "titan".equals(CONFIG);
    }

    public static boolean usingOrientDB() {
        return "orientdb".equals(CONFIG);
    }
}
