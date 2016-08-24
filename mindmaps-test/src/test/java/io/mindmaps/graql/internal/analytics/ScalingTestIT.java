package io.mindmaps.graql.internal.analytics;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import io.mindmaps.MindmapsTransaction;
import io.mindmaps.api.CommitLogController;
import io.mindmaps.api.GraphFactoryController;
import io.mindmaps.api.ImportController;
import io.mindmaps.api.TransactionController;
import io.mindmaps.core.MindmapsGraph;
import io.mindmaps.core.implementation.exception.MindmapsValidationException;
import io.mindmaps.core.model.EntityType;
import io.mindmaps.core.model.RelationType;
import io.mindmaps.core.model.RoleType;
import io.mindmaps.factory.MindmapsClient;
import io.mindmaps.loader.DistributedLoader;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.commons.configuration.Configuration;
import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static io.mindmaps.graql.Graql.var;
import static java.lang.Thread.sleep;

public class ScalingTestIT {

    private static final String[] HOST_NAME =
            {"localhost"};

    String TEST_KEYSPACE = Analytics.keySpace;
    MindmapsGraph graph;
    MindmapsTransaction transaction;

    // concepts
    EntityType thing;
    RoleType relation1;
    RoleType relation2;
    RelationType related;


    @BeforeClass
    public static void startController()
            throws InterruptedException, TTransportException, ConfigurationException, IOException {
        // Disable horrid cassandra logs
        Logger logger = (Logger) org.slf4j.LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        logger.setLevel(Level.OFF);

        EmbeddedCassandraServerHelper.startEmbeddedCassandra("cassandra-embedded.yaml");
        new GraphFactoryController();
        new CommitLogController();
        new TransactionController();

        sleep(5000);
    }

    @AfterClass
    public static void stopController() {
        EmbeddedCassandraServerHelper.stopEmbeddedCassandra();
    }

    @Before
    public void setUp() throws InterruptedException {
        System.out.println();
        System.out.println("Clearing the graph");
        graph = MindmapsClient.getGraph(TEST_KEYSPACE);
        graph.clear();
        graph = MindmapsClient.getGraph(TEST_KEYSPACE);
        transaction = graph.getTransaction();
    }

    @Test
    public void testAll() throws InterruptedException, ExecutionException, MindmapsValidationException {
        PrintWriter writer = null;
        try {
            writer = new PrintWriter("scale-times.txt", "UTF-8");
        } catch (FileNotFoundException | UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        long numberOfSuperNodes = 10L;
//        long[] scales = new long[]{10L, 100L, 1000L, 10000L};
        long[] scales = new long[]{10000L};

        for (long scale : scales) {
            writer.println("current scale - super " + numberOfSuperNodes + " - nodes " + scale);


            writer.println("start generate graph " + System.nanoTime()/1000000000L + "s");
            writer.flush();
            generateSimpleGraph(numberOfSuperNodes, scale);
            writer.println("stop generate graph " + System.nanoTime()/1000000000L + "s");

            Analytics computer = new Analytics();
            writer.println("start count " + System.nanoTime()/1000000000L + "s");
            writer.flush();
            writer.println("count: " + computer.count());
            writer.println("stop count " + System.nanoTime()/1000000000L + "s");
            writer.flush();
//            writer.println("start degree " + System.nanoTime()/1000000000L + "s");
//            writer.flush();
//            computer.degrees();
//            writer.println("stop degree " + System.nanoTime()/1000000000L + "s");
//            writer.flush();
//            writer.println("start persist degree " + System.nanoTime()/1000000000L + "s");
//            writer.flush();
//            computer.degreesAndPersist();
//            writer.println("stop persist degree " + System.nanoTime()/1000000000L + "s");
//            writer.flush();
            writer.println("start clean graph " + System.nanoTime()/1000000000L + "s");
            writer.flush();
            graph.clear();
            writer.println("stop clean graph " + System.nanoTime()/1000000000L + "s");
            Thread.sleep(5000);
            graph = MindmapsClient.getGraph(TEST_KEYSPACE);
            transaction = graph.getTransaction();
        }
    }

    private void generateSimpleGraph(long numberOfSupernodes, long numberOfNodes) throws MindmapsValidationException, InterruptedException {
        simpleOntology();
        transaction.commit();

        // make the supernodes
        refreshOntology();
        Set<String> superNodes = new HashSet<>();
        for (long i = 0; i < numberOfSupernodes; i++) {
            superNodes.add(transaction.addEntity(thing).getId());
        }
        transaction.commit();

        // batch in the nodes
        refreshOntology();

        DistributedLoader distributedLoader = new DistributedLoader(TEST_KEYSPACE,
                Arrays.asList(HOST_NAME));
        distributedLoader.setBatchSize(100);

        for (int nodeIndex = 0; nodeIndex < numberOfNodes; nodeIndex++) {
            String nodeId = "node-" + nodeIndex;
            distributedLoader.addToQueue(var().isa("thing").id(nodeId));
        }

        distributedLoader.waitToFinish();

        for (String supernodeId : superNodes) {
            for (int nodeIndex = 0; nodeIndex < numberOfNodes; nodeIndex++) {
                String nodeId = "node-" + nodeIndex;
                distributedLoader.addToQueue(var().isa("related")
                        .rel("relation1", var().id(nodeId))
                        .rel("relation2", var().id(supernodeId)));
            }
        }

        distributedLoader.waitToFinish();

    }

    private void simpleOntology() {
        thing = transaction.putEntityType("thing");
        relation1 = transaction.putRoleType("relation1");
        relation2 = transaction.putRoleType("relation2");
        thing.playsRole(relation1).playsRole(relation2);
        related = transaction.putRelationType("related").hasRole(relation1).hasRole(relation2);
    }

    private void refreshOntology() {
        thing = transaction.getEntityType("thing");
        relation1 = transaction.getRoleType("relation1");
        relation2 = transaction.getRoleType("relation2");
        related = transaction.getRelationType("related");
    }

}
