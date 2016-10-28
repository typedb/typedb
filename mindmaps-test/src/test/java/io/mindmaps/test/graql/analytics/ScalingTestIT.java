/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.test.graql.analytics;

import io.mindmaps.Mindmaps;
import io.mindmaps.MindmapsGraph;
import io.mindmaps.MindmapsGraphFactory;
import io.mindmaps.concept.Entity;
import io.mindmaps.concept.EntityType;
import io.mindmaps.concept.Relation;
import io.mindmaps.concept.RelationType;
import io.mindmaps.concept.RoleType;
import io.mindmaps.engine.loader.DistributedLoader;
import io.mindmaps.exception.MindmapsValidationException;
import io.mindmaps.graph.internal.AbstractMindmapsGraph;
import io.mindmaps.graql.internal.analytics.Analytics;
import io.mindmaps.test.AbstractScalingTest;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static io.mindmaps.graql.Graql.var;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * These tests are used for generating a report of the performance of analytics. In order to run them on a machine use
 * this maven command: mvn verify -Dtest=ScalingTestIT -DfailIfNoTests=false -Pscaling
 *
 * NB: Mindmaps must be running on a machine already and you may need to significantly increase the size of the java
 * heap to stop failures.
 */
public class ScalingTestIT extends AbstractScalingTest {

    private static final String[] HOST_NAME =
            {"localhost"};

    String keyspace;
    private MindmapsGraphFactory factory;

    // test parameters
    int NUM_SUPER_NODES = 10; // the number of supernodes to generate in the test graph
    int MAX_SIZE = 10000; // the maximum number of non super nodes to add to the test graph
    int NUM_DIVS = 4; // the number of divisions of the MAX_SIZE to use in the scaling test
    int REPEAT = 3; // the number of times to repeat at each size for average runtimes
    int MAX_WORKERS = Runtime.getRuntime().availableProcessors(); // the maximum number of workers that spark should use
    int WORKER_DIVS = 4; // the number of divisions of MAX_WORKERS to use for testing

    // test variables
    int STEP_SIZE;
    List<Integer> graphSizes;
    List<Integer> workerNumbers;

    @Before
    public void setUp() {
        // compute the sample of graph sizes
        STEP_SIZE = MAX_SIZE/NUM_DIVS;
        graphSizes = new ArrayList<>();
        for (int i = 1;i < NUM_DIVS;i++) graphSizes.add(i*STEP_SIZE);
        graphSizes.add(MAX_SIZE);
        STEP_SIZE = MAX_WORKERS/WORKER_DIVS;
        workerNumbers = new ArrayList<>();
        for (int i = 1;i <= WORKER_DIVS;i++) workerNumbers.add(i*STEP_SIZE);

        // get a random keyspace
        factory = factoryWithNewKeyspace();
        MindmapsGraph graph = factory.getGraph();
        keyspace = graph.getKeyspace();
    }

    @After
    public void cleanGraph() {
        MindmapsGraph graph = Mindmaps.factory(Mindmaps.DEFAULT_URI, keyspace).getGraph();
        graph.clear();
    }

    @Test
    public void countIT() throws InterruptedException, ExecutionException, MindmapsValidationException, IOException {
        Appendable out = new PrintWriter("countIT.txt","UTF-8");
        List<String> headers = new ArrayList<>();
        headers.add("Size");
        headers.addAll(workerNumbers.stream().map(String::valueOf).collect(Collectors.toList()));
        CSVPrinter printer = CSVFormat.DEFAULT.withHeader(headers.toArray(new String[0])).print(out);

        PrintWriter writer = null;
        try {
            writer = new PrintWriter("countITProgress.txt", "UTF-8");
        } catch (FileNotFoundException | UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        // Insert super nodes into graph
        simpleOntology(keyspace);

        Set<String> superNodes = makeSuperNodes(keyspace);

        int previousGraphSize = 0;
        for (int graphSize : graphSizes) {
            writer.println("current scale - super " + NUM_SUPER_NODES + " - nodes " + graphSize);
            Long conceptCount = Long.valueOf(NUM_SUPER_NODES * (graphSize + 1) + graphSize);
            printer.print(String.valueOf(graphSize));

            writer.println("start generate graph " + System.currentTimeMillis() / 1000L + "s");
            writer.flush();
            addNodes(keyspace, previousGraphSize, graphSize);
            addEdgesToSuperNodes(keyspace, superNodes, previousGraphSize, graphSize);
            previousGraphSize = graphSize;
            writer.println("stop generate graph " + System.currentTimeMillis() / 1000L + "s");
            writer.flush();

            for (int workerNumber : workerNumbers) {
                Analytics computer = new AnalyticsMock(keyspace, new HashSet<>(), new HashSet<>(), workerNumber);

                Long countTime = 0L;

                MindmapsGraph graph = Mindmaps.factory(Mindmaps.DEFAULT_URI, keyspace).getGraph();

                for (int i = 0; i < REPEAT; i++) {
                    writer.println("gremlin count is: " + graph.getTinkerTraversal().count().next());
                    writer.println("repeat number: " + i);
                    writer.flush();
                    Long startTime = System.currentTimeMillis();
                    Long count = computer.count();
                    assertEquals(conceptCount, count);
                    writer.println("count: " + count);
                    writer.flush();
                    Long stopTime = System.currentTimeMillis();
                    countTime += stopTime - startTime;
                    writer.println("count time: " + countTime / ((i + 1) * 1000));
                }

                countTime /= REPEAT * 1000;
                writer.println("time to count: " + countTime);
                printer.print(String.valueOf(countTime));
            }
            printer.println();
            printer.flush();
        }

        writer.flush();
        writer.close();

        printer.flush();
        printer.close();
    }

    @Test
    public void persistConstantIncreasingLoadIT() throws InterruptedException, MindmapsValidationException, ExecutionException, IOException {
        Appendable outWrite = new PrintWriter("persistConstantIncreasingLoadITWrite.txt","UTF-8");
        Appendable outMutate = new PrintWriter("persistConstantIncreasingLoadITMutate.txt","UTF-8");
        List<String> headers = new ArrayList<>();
        headers.add("Size");
        headers.addAll(workerNumbers.stream().map(String::valueOf).collect(Collectors.toList()));
        CSVPrinter printerWrite = CSVFormat.DEFAULT.withHeader(headers.toArray(new String[0])).print(outWrite);
        CSVPrinter printerMutate = CSVFormat.DEFAULT.withHeader(headers.toArray(new String[0])).print(outMutate);

        PrintWriter writer = null;
        try {
            writer = new PrintWriter("persistConstantIncreasingLoadITProgress.txt", "UTF-8");
        } catch (FileNotFoundException | UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        for (int graphSize : graphSizes) {
            Long conceptCount = Long.valueOf(graphSize) * 3 / 2;
            printerWrite.print(String.valueOf(conceptCount));
            printerMutate.print(String.valueOf(conceptCount));
            for (int workerNumber : workerNumbers) {
                Long degreeAndPersistTimeWrite = 0L;
                Long degreeAndPersistTimeMutate = 0L;

                // repeat persist
                for (int i = 0; i < REPEAT; i++) {
                    writer.println("repeat number: " + i);
                    String CURRENT_KEYSPACE = keyspace
                            + "g" + String.valueOf(graphSize)
                            + "w" + String.valueOf(workerNumber)
                            + "r" + String.valueOf(i);
                    simpleOntology(CURRENT_KEYSPACE);

                    // construct graph
                    writer.println("start generate graph " + System.currentTimeMillis() / 1000L + "s");
                    writer.flush();
                    addNodes(CURRENT_KEYSPACE, 0, graphSize);
                    writer.println("stop generate graph " + System.currentTimeMillis() / 1000L + "s");

                    MindmapsGraph graph = Mindmaps.factory(Mindmaps.DEFAULT_URI, CURRENT_KEYSPACE).getGraph();
                    writer.println("gremlin count is: " + graph.getTinkerTraversal().count().next());

                    Analytics computer = new AnalyticsMock(CURRENT_KEYSPACE, new HashSet<>(), new HashSet<>(), workerNumber);

                    writer.println("persist degree");
                    writer.flush();
                    Long startTime = System.currentTimeMillis();
                    computer.degreesAndPersist();
                    Long stopTime = System.currentTimeMillis();
                    degreeAndPersistTimeWrite += stopTime - startTime;
                    writer.println("persist time: " + degreeAndPersistTimeWrite / ((i + 1) * 1000));

                    // mutate graph
                    writer.println("start mutate graph " + System.currentTimeMillis() / 1000L + "s");
                    writer.flush();

                    // add edges to force mutation
                    addEdges(CURRENT_KEYSPACE, graphSize);

                    writer.println("stop mutate graph " + System.currentTimeMillis() / 1000L + "s");

                    graph = Mindmaps.factory(Mindmaps.DEFAULT_URI, CURRENT_KEYSPACE).getGraph();
                    writer.println("gremlin count is: " + graph.getTinkerTraversal().count().next());

                    writer.println("mutate degree");
                    computer = new AnalyticsMock(CURRENT_KEYSPACE, new HashSet<>(), new HashSet<>(), workerNumber);
                    writer.flush();
                    startTime = System.currentTimeMillis();
                    computer.degreesAndPersist();
                    stopTime = System.currentTimeMillis();
                    degreeAndPersistTimeMutate += stopTime - startTime;
                    writer.println("mutate time: " + degreeAndPersistTimeMutate / ((i + 1) * 1000));

                    writer.println("start clean graph" + System.currentTimeMillis() / 1000L + "s");
                    graph.clear();
                    writer.println("stop clean graph" + System.currentTimeMillis() / 1000L + "s");
                }

                degreeAndPersistTimeWrite /= REPEAT * 1000;
                degreeAndPersistTimeMutate /= REPEAT * 1000;
                writer.println("time to degreesAndPersistWrite: " + degreeAndPersistTimeWrite);
                writer.println("time to degreesAndPersistMutate: " + degreeAndPersistTimeMutate);

                printerWrite.print(String.valueOf(degreeAndPersistTimeWrite));
                printerMutate.print(String.valueOf(degreeAndPersistTimeMutate));
            }
            printerWrite.println();
            printerWrite.flush();
            printerMutate.println();
            printerMutate.flush();
        }

        printerWrite.close();
        printerMutate.close();

        writer.flush();
        writer.close();
    }

    @Test
    public void testLargeDegreeMutationResultsInReadableGraphIT() throws Exception {

        simpleOntology(keyspace);

        // construct graph
        addNodes(keyspace, 0, MAX_SIZE);

        //TODO: Get rid of this close. We should be refreshing the graph in the factory when switching between normal and batch
        ((AbstractMindmapsGraph) factory.getGraph()).getTinkerPopGraph().close();

        Analytics computer = new Analytics(keyspace, new HashSet<>(), new HashSet<>());

        computer.degreesAndPersist();

        // assert mutated degrees are as expected
        MindmapsGraph graph = factory.getGraph();
        EntityType thing = graph.getEntityType("thing");
        Collection<Entity> things = thing.instances();

        assertFalse(things.isEmpty());
        things.forEach(thisThing -> {
            assertEquals(1L, thisThing.resources().size());
            assertEquals(0L, thisThing.resources().iterator().next().getValue());
        });


        // add edges to force mutation
        addEdges(keyspace, MAX_SIZE);

        //TODO: Get rid of this close. We should be refreshing the graph in the factory when switching between normal and batch
        ((AbstractMindmapsGraph) factory.getGraph()).getTinkerPopGraph().close();

        computer = new Analytics(keyspace, new HashSet<>(), new HashSet<>());

        computer.degreesAndPersist();

        // assert mutated degrees are as expected
        graph = factory.getGraph();
        thing = graph.getEntityType("thing");
        things = thing.instances();

        assertFalse(things.isEmpty());
        things.forEach(thisThing -> {
            assertEquals(1L, thisThing.resources().size());
            assertEquals(1L, thisThing.resources().iterator().next().getValue());
        });

        Collection<Relation> relations = graph.getRelationType("related").instances();

        assertFalse(relations.isEmpty());
        relations.forEach(thisRelation -> {
            assertEquals(1L, thisRelation.resources().size());
            assertEquals(2L, thisRelation.resources().iterator().next().getValue());
        });

    }

    /**
     * This test creates a graph of S*N*2 entities in total where: S is the total number of steps and N is the number of
     * entities per step. Each entity has a single numeric resource attached with the values chosen so that each size
     * of graph results in different values for the min, max, mean, sum, std. The median value always remains the same
     * but because the graph is growing, this in itself is a good test of median. The values follow the pattern:
     *
     * STEP g=1:         5 6 14 16
     * STEP g=2:     3 4 5 6 14 16 18 20
     * STEP g=3: 1 2 3 4 5 6 14 16 18 20 22 24
     *
     * for S = 3, N = 2.
     *
     * To generate the sequence there are two recursive formulae, one for the lower half of values v_m, another for the
     * upper half V_m:
     *
     * v_m+1 = v_m - 1, v_1 = S*N
     * V_m+1 = V_m + 2, V_1 = (S*N + 1)*2
     *
     * The sum of these values at STEP g is given by the formula:
     *
     * sum(g) = g*N(1/2 + g*N/2 + 3*S*N + 1)
     */
    @Test
    public void testStatisticsWithConstantDegree() throws IOException, MindmapsValidationException {
        Appendable outSum = new PrintWriter("testStatisticsWithConstantDegreeSum.txt","UTF-8");
        List<String> headers = new ArrayList<>();
        headers.add("Size");
        headers.addAll(workerNumbers.stream().map(String::valueOf).collect(Collectors.toList()));
        CSVPrinter printerSum = CSVFormat.DEFAULT.withHeader(headers.toArray(new String[0])).print(outSum);

        // create the ontology
        simpleOntology(keyspace);
        new Analytics(keyspace, new HashSet<>(), new HashSet<>());

        DistributedLoader distributedLoader = new DistributedLoader(keyspace,
                Arrays.asList(HOST_NAME));
        distributedLoader.setThreadsNumber(30);
        distributedLoader.setPollingFrequency(1000);
        distributedLoader.setBatchSize(100);

        int totalSteps = 3;
        int nodesPerStep = 2;
        int v_m = totalSteps*nodesPerStep;
        int V_m = 2*(totalSteps*nodesPerStep+1);
        for (int g=1; g<totalSteps+1; g++) {
            printerSum.print(2*g*nodesPerStep);

            // load data
            for (int m=1; m<nodesPerStep+1; m++) {
                distributedLoader.addToQueue(var().isa("thing").has("degree",v_m));
                distributedLoader.addToQueue(var().isa("thing").has("degree",V_m));
                v_m--;
                V_m+=2;
            }
            distributedLoader.waitToFinish();

            // check the sum is correct
            assertEquals((long) g*nodesPerStep*(1+g*nodesPerStep+6*totalSteps*nodesPerStep+2)/2
                    ,new Analytics(keyspace, new HashSet<String>(), Collections.singleton("degree")).sum().get());

            printerSum.println();
            printerSum.flush();
        }

        printerSum.flush();
        printerSum.close();
        factory.getGraph().clear();
    }

    private void addNodes(String keyspace, int startRange, int endRange) throws MindmapsValidationException, InterruptedException {
        // batch in the nodes
        DistributedLoader distributedLoader = new DistributedLoader(keyspace,
                Arrays.asList(HOST_NAME));
        distributedLoader.setThreadsNumber(30);
        distributedLoader.setPollingFrequency(1000);
        distributedLoader.setBatchSize(100);

        for (int nodeIndex = startRange; nodeIndex < endRange; nodeIndex++) {
            String nodeId = "node-" + nodeIndex;
            distributedLoader.addToQueue(var().isa("thing").id(nodeId));
        }

        distributedLoader.waitToFinish();

    }

    private void addEdgesToSuperNodes(String keyspace, Set<String> superNodes, int startRange, int endRange) {
        // batch in the nodes
        DistributedLoader distributedLoader = new DistributedLoader(keyspace,
                Arrays.asList(HOST_NAME));
        distributedLoader.setThreadsNumber(30);
        distributedLoader.setPollingFrequency(1000);
        distributedLoader.setBatchSize(100);

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

    private void simpleOntology(String keyspace) throws MindmapsValidationException {
        MindmapsGraph graph = Mindmaps.factory(Mindmaps.DEFAULT_URI, keyspace).getGraph();
        EntityType thing = graph.putEntityType("thing");
        RoleType relation1 = graph.putRoleType("relation1");
        RoleType relation2 = graph.putRoleType("relation2");
        thing.playsRole(relation1).playsRole(relation2);
        RelationType related = graph.putRelationType("related").hasRole(relation1).hasRole(relation2);
        graph.commit();
    }

    private Set<String> makeSuperNodes(String keyspace) throws MindmapsValidationException {
        // make the supernodes
        MindmapsGraph graph = Mindmaps.factory(Mindmaps.DEFAULT_URI, keyspace).getGraph();
        EntityType thing = graph.getEntityType("thing");
        RoleType relation1 = graph.getRoleType("relation1");
        RoleType relation2 = graph.getRoleType("relation2");
        RelationType related = graph.getRelationType("related");
        Set<String> superNodes = new HashSet<>();
        for (int i = 0; i < NUM_SUPER_NODES; i++) {
            superNodes.add(graph.addEntity(thing).getId());
        }
        graph.commit();
        return superNodes;
    }

    private void addEdges(String keyspace, int graphSize) {
        // if graph size not even throw error for now
        if (graphSize%2!=0) {
            throw new RuntimeException("sorry graphsize has to be even");
        }

        // batch in the nodes
        DistributedLoader distributedLoader = new DistributedLoader(keyspace,
                Arrays.asList(HOST_NAME));
        distributedLoader.setThreadsNumber(30);
        distributedLoader.setPollingFrequency(1000);
        distributedLoader.setBatchSize(100);

        int startNode = 0;
        while (startNode<graphSize) {

            String nodeId1 = "node-" + startNode;
            String nodeId2 = "node-" + ++startNode;
            distributedLoader.addToQueue(var().isa("related")
                    .rel("relation1", var().id(nodeId1))
                    .rel("relation2", var().id(nodeId2)));

            startNode++;
        }
        distributedLoader.waitToFinish();
    }
}
