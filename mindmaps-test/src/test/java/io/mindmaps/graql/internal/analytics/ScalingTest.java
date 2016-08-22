package io.mindmaps.graql.internal.analytics;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import io.mindmaps.MindmapsTransaction;
import io.mindmaps.core.Data;
import io.mindmaps.core.MindmapsGraph;
import io.mindmaps.core.implementation.exception.MindmapsValidationException;
import io.mindmaps.core.model.EntityType;
import io.mindmaps.core.model.RelationType;
import io.mindmaps.core.model.ResourceType;
import io.mindmaps.core.model.RoleType;
import io.mindmaps.factory.MindmapsClient;
import io.mindmaps.loader.DistributedLoader;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static io.mindmaps.graql.Graql.var;

public class ScalingTest {

    private static final String[] HOST_NAME =
            {"localhost"};

    String TEST_KEYSPACE = "mindmapsanalyticstest";
    MindmapsGraph graph;
    MindmapsTransaction transaction;

    // concepts
    EntityType thing;
    RoleType relation1;
    RoleType relation2;
    RelationType related;
    RoleType degreeTarget;
    RoleType degreeValue;
    RelationType hasDegree;
    ResourceType degreeResource;

    @BeforeClass
    public static void killLogs() {
        Logger logger = (Logger) org.slf4j.LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        logger.setLevel(Level.OFF);
    }

    @Before
    public void setUp() throws Exception {
//        new GraphFactoryController();
//        new CommitLogController();
        Thread.sleep(5000);
        graph = MindmapsClient.getGraph(TEST_KEYSPACE);
        graph.clear();
        Thread.sleep(5000);
        graph = MindmapsClient.getGraph(TEST_KEYSPACE);
        transaction = graph.getTransaction();
    }

    @Test
    public void testAll() throws InterruptedException, ExecutionException {
        PrintWriter writer = null;
        try {
            writer = new PrintWriter("scale-times.txt", "UTF-8");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        long numberOfSuperNodes = 10L;
        long[] scales = new long[]{10L,100L,1000L};
//        long[] scales = new long[]{1000L};

        for (long scale : scales) {
            writer.println("current scale - super " + numberOfSuperNodes + " - nodes " + scale);


            writer.println("start generate graph " + System.nanoTime());
            writer.flush();
            generateSimpleGraph(numberOfSuperNodes, scale);
            writer.println("stop generate graph " + System.nanoTime());
            Thread.sleep(5000);

            Analytics computer = new Analytics();
            writer.println("start count " + System.nanoTime());
            writer.flush();
            writer.println("count: " + computer.count());
            writer.println("stop count " + System.nanoTime());
            writer.flush();
            writer.println("start degree " + System.nanoTime());
            writer.flush();
            computer.degrees();
            writer.println("stop degree " + System.nanoTime());
            writer.flush();
            writer.println("start persist degree " + System.nanoTime());
            writer.flush();
            computer.degreesAndPersist();
            writer.println("stop persist degree " + System.nanoTime());
            writer.flush();
//            writer.println("start clean graph " + System.nanoTime());
//            writer.flush();

//            System.out.println("the count is: "+graph.getGraph().traversal().V().count().next());
//            cleanGraph();
//            writer.println("stop clean graph " + System.nanoTime());
//            Thread.sleep(10000);
//            graph = MindmapsClient.getGraph(TEST_KEYSPACE);
//            transaction = graph.newTransaction();
        }
    }

    private void generateSimpleGraph(long numberOfSupernodes, long numberOfNodes) {
        simpleOntology();
        try {
            transaction.commit();
        } catch (MindmapsValidationException e) {
            e.printStackTrace();
        }

        // make the supernodes
        refreshOntology();
        Set<String> superNodes = new HashSet<>();
        for (long i = 0; i < numberOfSupernodes; i++) {
            superNodes.add(transaction.addEntity(thing).getId());
        }
        try {
            transaction.commit();
        } catch (MindmapsValidationException e) {
            e.printStackTrace();
        }

        // batch in the nodes
        int batchSize = 100;
        int currentBatch = 0;
        int m = 0;
        refreshOntology();

        DistributedLoader distributedLoader = new DistributedLoader(TEST_KEYSPACE,
                Arrays.asList(HOST_NAME));
        distributedLoader.setBatchSize(40);

        for (int nodeIndex = 0; nodeIndex < numberOfNodes; nodeIndex++) {
            String nodeId = "node-" + nodeIndex;
            distributedLoader.addToQueue(var().isa("thing").id(nodeId));
        }

        for (String supernodeId : superNodes) {
            for (int nodeIndex = 0; nodeIndex < numberOfNodes; nodeIndex++) {
                String nodeId = "node-" + nodeIndex;
                distributedLoader.addToQueue(var().isa("related")
                        .rel("relation1", var().id(nodeId))
                        .rel("relation2", var().id(supernodeId)));
            }
        }

    }

    private void simpleOntology() {
        thing = transaction.putEntityType("thing");
        relation1 = transaction.putRoleType("relation1");
        relation2 = transaction.putRoleType("relation2");
        thing.playsRole(relation1).playsRole(relation2);
        related = transaction.putRelationType("related").hasRole(relation1).hasRole(relation2);

        degreeResource = transaction.putResourceType("degree-resource", Data.LONG);
        degreeTarget = transaction.putRoleType("degree-target");
        degreeValue = transaction.putRoleType("degree-value");
        hasDegree = transaction.putRelationType("has-degree")
                .hasRole(degreeTarget).hasRole(degreeValue);
        thing.playsRole(degreeTarget);
        related.playsRole(degreeTarget);
        degreeResource.playsRole(degreeValue);
    }

    private void refreshOntology() {
        thing = transaction.getEntityType("thing");
        relation1 = transaction.getRoleType("relation1");
        relation2 = transaction.getRoleType("relation2");
        related = transaction.getRelationType("related");

        degreeResource = transaction.getResourceType("degree-resource");
        degreeTarget = transaction.getRoleType("degree-target");
        degreeValue = transaction.getRoleType("degree-value");
        hasDegree = transaction.getRelationType("has-degree");
    }

}
