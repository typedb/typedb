package ai.grakn.test;

import ai.grakn.GraknGraph;
import ai.grakn.engine.GraknEngineServer;
import ai.grakn.engine.util.ConfigProperties;
import ai.grakn.factory.GraphFactory;
import ai.grakn.factory.SystemKeyspace;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.auth0.jwt.internal.org.apache.commons.io.FileUtils;
import com.jayway.restassured.RestAssured;
import info.batey.kafka.unit.KafkaUnit;
import jline.internal.Log;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static ai.grakn.engine.util.ExceptionWrapper.noThrow;
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
public abstract class GraknTestEnv {

    private static String CONFIG = System.getProperty("grakn.test-profile");
    private static AtomicBoolean CASSANDRA_RUNNING = new AtomicBoolean(false);
    private static AtomicBoolean ENGINE_RUNNING = new AtomicBoolean(false);
    private static AtomicBoolean HTTP_RUNNING = new AtomicBoolean(false);

    private static final ConfigProperties properties = ConfigProperties.getInstance();

    private static KafkaUnit kafkaUnit = new KafkaUnit(2181, 9092);
    private static Path tempDirectory;

    public static void ensureCassandraRunning() throws Exception {
        if (CASSANDRA_RUNNING.compareAndSet(false, true) && usingTitan()) {
            startEmbeddedCassandra();
            System.out.println("CASSANDRA RUNNING.");
        }
    }

    //TODO :: This will be removed when we fix BUG #12029. We will be able to run AbstractGraphTest classes
    //TODO :: without touching any engine component. Starting the HTTP server will move into startEngine()
    public static void ensureHTTPRunning(){
        if(HTTP_RUNNING.compareAndSet(false, true)) {
            RestAssured.baseURI = "http://" + properties.getProperty("server.host") + ":" + properties.getProperty("server.port");
            GraknEngineServer.startHTTP();
        }
    }

    /**
     * To run engine we must ensure Cassandra, the Grakn HTTP endpoint, Kafka & Zookeeper are running
     */
    static void startEngine() throws Exception {
    	// To ensure consistency b/w test profiles and configuration files, when not using Titan
    	// for a unit tests in an IDE, add the following option:
    	// -Dgrakn.conf=../conf/test/tinker/grakn-engine.properties
    	//
    	// When using titan, add -Dgrakn.test-profile=titan
    	//
    	// The reason is that the default configuration of Grakn uses the Titan factory while the default
    	// test profile is tinker: so when running a unit test within an IDE without any extra parameters,
    	// we end up wanting to use the TitanFactory but without starting Cassandra first.

        if(ENGINE_RUNNING.compareAndSet(false, true)) {
            System.out.println("STARTING ENGINE...");

            ensureCassandraRunning();

            tempDirectory = Files.createTempDirectory("graknKafkaUnit " + UUID.randomUUID());
            kafkaUnit.setKafkaBrokerConfig("log.dirs", tempDirectory.toString());
            kafkaUnit.startup();

            // start engine
            ensureHTTPRunning();
            GraknEngineServer.startCluster();

            try {Thread.sleep(5000);} catch(InterruptedException ex) { Log.info("Thread sleep interrupted."); }
            
            System.out.println("ENGINE STARTED.");
        }
    }

    static void stopEngine() throws IOException {
        if(ENGINE_RUNNING.compareAndSet(true, false)) {
            System.out.println("STOPPING ENGINE...");

            noThrow(GraknEngineServer::stopCluster, "Problem while shutting down Zookeeper cluster.");
            noThrow(kafkaUnit::shutdown, "Problem while shutting down Kafka Unit.");
            noThrow(GraknTestEnv::clearGraphs, "Problem while clearing graphs.");
            noThrow(GraknTestEnv::stopHTTP, "Problem while shutting down Engine");

            FileUtils.deleteDirectory(tempDirectory.toFile());

            System.out.println("ENGINE STOPPED.");
        }

        // There is no way to stop the embedded Casssandra, no such API offered.
    }

    //TODO :: This will be removed when we fix BUG #12029. We will be able to run AbstractGraphTest classes
    //TODO :: without touching any engine component. Stopping the HTTP server will move into stopEngine()
    static void stopHTTP(){
        if(HTTP_RUNNING.compareAndSet(true, false)) {
            GraknEngineServer.stopHTTP();
        }
    }

    static void clearGraphs() {
        // Drop all keyspaces
        GraphFactory graphFactory = GraphFactory.getInstance();

        GraknGraph systemGraph = graphFactory.getGraph(SystemKeyspace.SYSTEM_GRAPH_NAME);
        systemGraph.graql().match(var("x").isa("keyspace-name"))
                .execute()
                .forEach(x -> x.values().forEach(y -> {
                    String name = y.asResource().getValue().toString();
                    GraknGraph graph = graphFactory.getGraph(name);
                    graph.clear();
                }));

        // Drop the system keyspaces too
        systemGraph.clear();

        graphFactory.refershConnections();
    }

    static void startEmbeddedCassandra() {
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

    static void hideLogs() {
        Logger logger = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        logger.setLevel(Level.OFF);
        org.apache.log4j.Logger.getRootLogger().setLevel(org.apache.log4j.Level.ERROR);
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
