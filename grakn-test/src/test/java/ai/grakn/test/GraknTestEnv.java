package ai.grakn.test;

import ai.grakn.GraknGraph;
import ai.grakn.engine.GraknEngineServer;
import ai.grakn.engine.postprocessing.EngineCache;
import ai.grakn.engine.util.ConfigProperties;
import ai.grakn.factory.EngineGraknGraphFactory;
import ai.grakn.factory.SystemKeyspace;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.auth0.jwt.internal.org.apache.commons.io.FileUtils;
import com.jayway.restassured.RestAssured;
import info.batey.kafka.unit.KafkaUnit;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

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

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(GraknTestEnv.class);

    private static final ConfigProperties properties = ConfigProperties.getInstance();

    private static String CONFIG = System.getProperty("grakn.test-profile");
    private static AtomicBoolean CASSANDRA_RUNNING = new AtomicBoolean(false);
    private static AtomicBoolean ENGINE_RUNNING = new AtomicBoolean(false);

    private static KafkaUnit kafkaUnit = new KafkaUnit(2181, 9092);
    private static Path tempDirectory;

    public static void ensureCassandraRunning() throws Exception {
        if (CASSANDRA_RUNNING.compareAndSet(false, true) && usingTitan()) {
            startEmbeddedCassandra();
            LOG.info("CASSANDRA RUNNING.");
        }
    }

    /**
     * To run engine we must ensure Cassandra, the Grakn HTTP endpoint, Kafka & Zookeeper are running
     */
    static void startEngine(String taskManagerClass) throws Exception {
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
            LOG.info("STARTING ENGINE...");

            ensureCassandraRunning();

            // start engine
            RestAssured.baseURI = "http://" + properties.getProperty("server.host") + ":" + properties.getProperty("server.port");
            GraknEngineServer.start(taskManagerClass);

            LOG.info("ENGINE STARTED.");
        }
    }

    static void startKafka() throws Exception {
        tempDirectory = Files.createTempDirectory("graknKafkaUnit " + UUID.randomUUID());
        kafkaUnit.setKafkaBrokerConfig("log.dirs", tempDirectory.toString());
        kafkaUnit.startup();
    }

    static void stopKafka() throws Exception {
        kafkaUnit.shutdown();
        FileUtils.deleteDirectory(tempDirectory.toFile());
    }

    static void stopEngine() throws Exception {
        if(ENGINE_RUNNING.compareAndSet(true, false)) {
            LOG.info("STOPPING ENGINE...");

            GraknEngineServer.stop();
            clearGraphs();

            LOG.info("ENGINE STOPPED.");
        }

        // There is no way to stop the embedded Casssandra, no such API offered.
    }

    static void clearGraphs() {
        // Drop all keyspaces
        EngineGraknGraphFactory engineGraknGraphFactory = EngineGraknGraphFactory.getInstance();

        GraknGraph systemGraph = engineGraknGraphFactory.getGraph(SystemKeyspace.SYSTEM_GRAPH_NAME);
        systemGraph.graql().match(var("x").isa("keyspace-name"))
                .execute()
                .forEach(x -> x.values().forEach(y -> {
                    String name = y.asResource().getValue().toString();
                    GraknGraph graph = engineGraknGraphFactory.getGraph(name);
                    graph.admin().clear(EngineCache.getInstance());
                }));

        engineGraknGraphFactory.refreshConnections();
    }

    static void startEmbeddedCassandra() {
        try {
            // We have to use reflection here because the cassandra dependency is only included when testing the titan profile.
            Class cl = Class.forName("org.cassandraunit.utils.EmbeddedCassandraServerHelper");
            hideLogs();

            //noinspection unchecked
            cl.getMethod("startEmbeddedCassandra", String.class).invoke(null, "cassandra-embedded.yaml");
            hideLogs();

            try {Thread.sleep(5000);} catch(InterruptedException ex) { LOG.info("Thread sleep interrupted."); }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    static String randomKeyspace(){
        // Embedded Casandra has problems dropping keyspaces that start with a number
        return "a"+ UUID.randomUUID().toString().replaceAll("-", "");
    }

    public static void hideLogs() {
        ((Logger) org.slf4j.LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME)).setLevel(Level.OFF);
        ((Logger) org.slf4j.LoggerFactory.getLogger(GraknTestEnv.class)).setLevel(Level.DEBUG);
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
