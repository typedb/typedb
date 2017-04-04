package ai.grakn.test;

import ai.grakn.GraknGraph;
import ai.grakn.GraknTxType;
import ai.grakn.engine.GraknEngineConfig;
import ai.grakn.engine.GraknEngineServer;
import ai.grakn.engine.cache.EngineCacheProvider;
import ai.grakn.factory.EngineGraknGraphFactory;
import ai.grakn.factory.SystemKeyspace;
import com.jayway.restassured.RestAssured;
import info.batey.kafka.unit.KafkaUnit;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static ai.grakn.engine.tasks.config.KafkaTerms.NEW_TASKS_TOPIC;
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

    private static final GraknEngineConfig properties = GraknEngineConfig.getInstance();

    private static String CONFIG = System.getProperty("grakn.test-profile");
    private static AtomicBoolean CASSANDRA_RUNNING = new AtomicBoolean(false);
    private static AtomicInteger KAFKA_COUNTER = new AtomicInteger(0);

    private static KafkaUnit kafkaUnit = new KafkaUnit(2181, 9092);

    public static void ensureCassandraRunning() throws Exception {
        if (CASSANDRA_RUNNING.compareAndSet(false, true) && usingTitan()) {
            LOG.info("starting cassandra...");
            startEmbeddedCassandra();
            LOG.info("cassandra started.");
        }
    }

    /**
     * To run engine we must ensure Cassandra, the Grakn HTTP endpoint, Kafka & Zookeeper are running
     */
    static GraknEngineServer startEngine(String taskManagerClass, int port) throws Exception {
    	// To ensure consistency b/w test profiles and configuration files, when not using Titan
    	// for a unit tests in an IDE, add the following option:
    	// -Dgrakn.conf=../conf/test/tinker/grakn.properties
    	//
    	// When using titan, add -Dgrakn.test-profile=titan
    	//
    	// The reason is that the default configuration of Grakn uses the Titan factory while the default
    	// test profile is tinker: so when running a unit test within an IDE without any extra parameters,
    	// we end up wanting to use the TitanFactory but without starting Cassandra first.
        LOG.info("starting engine...");

        ensureCassandraRunning();

        // start engine
        RestAssured.baseURI = "http://" + properties.getProperty("server.host") + ":" + properties.getProperty("server.port");
        GraknEngineServer server = GraknEngineServer.start(taskManagerClass, port);

        LOG.info("engine started.");

        return server;
    }

    static void startKafka() throws Exception {
        // Kafka is using log4j, which is super annoying. We make sure it only logs error here
        org.apache.log4j.Logger.getRootLogger().setLevel(org.apache.log4j.Level.ERROR);

        // Clean-up ironically uses a lot of memory
        if (KAFKA_COUNTER.getAndIncrement() == 0) {
            LOG.info("starting kafka...");

            kafkaUnit.setKafkaBrokerConfig("log.cleaner.enable", "false");
            kafkaUnit.startup();
            kafkaUnit.createTopic(NEW_TASKS_TOPIC, properties.getAvailableThreads() * 4);

            LOG.info("kafka started.");
        }
    }

    static void stopKafka() throws Exception {
        if (KAFKA_COUNTER.decrementAndGet() == 0) {
            LOG.info("stopping kafka...");
            kafkaUnit.shutdown();
            LOG.info("kafka stopped.");
        }
    }

    static void stopEngine(GraknEngineServer server) throws Exception {
        LOG.info("stopping engine...");

        server.close();
        clearGraphs();

        LOG.info("engine stopped.");

        // There is no way to stop the embedded Casssandra, no such API offered.
    }

    static void clearGraphs() {
        // Drop all keyspaces
        EngineGraknGraphFactory engineGraknGraphFactory = EngineGraknGraphFactory.getInstance();

        try(GraknGraph systemGraph = engineGraknGraphFactory.getGraph(SystemKeyspace.SYSTEM_GRAPH_NAME, GraknTxType.WRITE)) {
            systemGraph.graql().match(var("x").isa("keyspace-name"))
                    .execute()
                    .forEach(x -> x.values().forEach(y -> {
                        String name = y.asResource().getValue().toString();
                        GraknGraph graph = engineGraknGraphFactory.getGraph(name, GraknTxType.WRITE);
                        graph.admin().clear(EngineCacheProvider.getCache());
                    }));
        }
        engineGraknGraphFactory.refreshConnections();
    }

    static void startEmbeddedCassandra() {
        try {
            // We have to use reflection here because the cassandra dependency is only included when testing the titan profile.
            Class cl = Class.forName("org.cassandraunit.utils.EmbeddedCassandraServerHelper");

            //noinspection unchecked
            cl.getMethod("startEmbeddedCassandra", String.class).invoke(null, "cassandra-embedded.yaml");

            try {Thread.sleep(5000);} catch(InterruptedException ex) { LOG.info("Thread sleep interrupted."); }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static String randomKeyspace(){
        // Embedded Casandra has problems dropping keyspaces that start with a number
        return "a"+ UUID.randomUUID().toString().replaceAll("-", "");
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
