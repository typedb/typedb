package io.mindmaps.graql.internal.analytics;

import io.mindmaps.MindmapsTransaction;
import io.mindmaps.core.Data;
import io.mindmaps.api.CommitLogController;
import io.mindmaps.api.GraphFactoryController;
import io.mindmaps.api.ImportController;
import io.mindmaps.api.TransactionController;
import io.mindmaps.core.Data;
import io.mindmaps.core.MindmapsGraph;
import io.mindmaps.core.implementation.exception.MindmapsValidationException;
import io.mindmaps.core.model.EntityType;
import io.mindmaps.core.model.RelationType;
import io.mindmaps.core.model.ResourceType;
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
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static io.mindmaps.IntegrationUtils.startTestEngine;
import static io.mindmaps.graql.Graql.all;
import static io.mindmaps.graql.Graql.var;
import static java.lang.Thread.sleep;

public class ScalingTestIT {

    private static final String[] HOST_NAME =
            {"localhost"};

    String TEST_KEYSPACE = "mindmapstest";
    MindmapsGraph graph;
    MindmapsTransaction transaction;

    // concepts
    EntityType thing;
    RoleType relation1;
    RoleType relation2;
    RelationType related;


    @BeforeClass
    public static void startController() throws Exception {
        startTestEngine();
    }

    @Before
    public void setUp() throws InterruptedException {
        graph = MindmapsClient.getGraph(TEST_KEYSPACE);
        graph.clear();
        graph = MindmapsClient.getGraph(TEST_KEYSPACE);
        transaction = graph.getTransaction();
    }

    @Test
    public void countAndDegreeIT() throws InterruptedException, ExecutionException, MindmapsValidationException {
        PrintWriter writer = null;
        try {
            writer = new PrintWriter("scale-times.txt", "UTF-8");
        } catch (FileNotFoundException | UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        // compute the sample of graph sizes
        int MAX_SIZE = 2;
        int NUM_DIVS = 2;
        int REPEAT = 2;

        int STEP_SIZE = MAX_SIZE/NUM_DIVS;
        List<Integer> graphSizes = new ArrayList<>();
        for (int i = 1;i < NUM_DIVS;i++) graphSizes.add(i*STEP_SIZE);
        graphSizes.add(MAX_SIZE);

        Map<Integer, Long> scaleToAverageTimeCount = new HashMap<>();
        Map<Integer, Long> scaleToAverageTimeDegree = new HashMap<>();
        Map<Integer, Long> scaleToAverageTimeDegreeAndPersist = new HashMap<>();

        int NUM_SUPER_NODES = 10;

        // Insert super nodes into graph
        simpleOntology();
        transaction.commit();

        // make the supernodes
        refreshOntology();
        Set<String> superNodes = new HashSet<>();
        for (int i = 0; i < NUM_SUPER_NODES; i++) {
            superNodes.add(transaction.addEntity(thing).getId());
        }
        transaction.commit();

        // add resources in advance
        ResourceType<Long> resourceType = transaction.putResourceType("degree", Data.LONG);
        for (long i = 0;i<MAX_SIZE;i++) {
            transaction.putResource(i,resourceType);
        }
        transaction.commit();

        int previousGraphSize = 0;
        for (int graphSize : graphSizes) {
            writer.println("current scale - super " + NUM_SUPER_NODES + " - nodes " + graphSize);


            writer.println("start generate graph " + System.currentTimeMillis()/1000L + "s");
            writer.flush();
            addNodes(superNodes, previousGraphSize, graphSize);
            previousGraphSize = graphSize;
            writer.println("stop generate graph " + System.currentTimeMillis()/1000L + "s");

            Analytics computer = new Analytics(TEST_KEYSPACE);

            Long countTime = 0L;
            Long degreeTime = 0L;
            Long degreeAndPersistTime = 0L;
            Long startTime = 0L;
            Long stopTime = 0L;

            for (int i=0;i<REPEAT;i++) {
                writer.println("gremlin count is: " + transaction.getTinkerTraversal().V().count().next());
                writer.println("repeat number: "+i);
                writer.flush();
                startTime = System.currentTimeMillis();
                writer.println("count: " + computer.count());
                writer.flush();
                stopTime = System.currentTimeMillis();
                countTime+=stopTime-startTime;
                writer.println("count time: " + countTime / ((i + 1) * 1000));

//                writer.println("degree");
//                writer.flush();
//                startTime = System.currentTimeMillis();
//                computer.degrees();
//                stopTime = System.currentTimeMillis();
//                degreeTime+=stopTime-startTime;
//                writer.println("degree time: " + degreeTime / ((i + 1) * 1000));

                writer.println("persist degree");
                writer.flush();
                startTime = System.currentTimeMillis();
                computer.degreesAndPersist();
                stopTime = System.currentTimeMillis();
                degreeAndPersistTime+=stopTime-startTime;
                System.out.println("persist time: "+degreeAndPersistTime/((i+1)*1000));
            }

            countTime /= REPEAT*1000;
            degreeTime /= REPEAT*1000;
            degreeAndPersistTime /= REPEAT*1000;
            writer.println("time to count: " + countTime);
            scaleToAverageTimeCount.put(graphSize,countTime);
            writer.println("time to degrees: " + degreeTime);
            scaleToAverageTimeDegree.put(graphSize,degreeTime);
            System.out.println("time to degreesAndPersist: "+degreeAndPersistTime);
            scaleToAverageTimeDegreeAndPersist.put(graphSize,degreeAndPersistTime);
        }

        writer.println("start clean graph " + System.currentTimeMillis()/1000L + "s");
        writer.flush();
        graph.clear();
        writer.println("stop clean graph " + System.currentTimeMillis()/1000L + "s");

        writer.println("counts: " + scaleToAverageTimeCount);
        writer.println("degrees: " + scaleToAverageTimeDegree);
        System.out.println("degreesAndPersist: " + scaleToAverageTimeDegreeAndPersist);
    }

    private void addNodes(Set<String> superNodes, int startRange, int endRange) throws MindmapsValidationException, InterruptedException {
        // batch in the nodes
        DistributedLoader distributedLoader = new DistributedLoader(TEST_KEYSPACE,
                Arrays.asList(HOST_NAME));
        distributedLoader.setThreadsNumber(30);
        distributedLoader.setPollingFrequency(1000);
        distributedLoader.setBatchSize(100);

        for (int nodeIndex = startRange; nodeIndex < endRange; nodeIndex++) {
            String nodeId = "node-" + nodeIndex;
            distributedLoader.addToQueue(var().isa("thing").id(nodeId));
        }

        distributedLoader.waitToFinish();

        for (String supernodeId : superNodes) {
            for (int nodeIndex = startRange; nodeIndex < endRange; nodeIndex++) {
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
