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

package io.mindmaps.graql.internal.analytics;

import io.mindmaps.MindmapsGraph;
import io.mindmaps.concept.*;
import io.mindmaps.exception.MindmapsValidationException;
import io.mindmaps.engine.loader.DistributedLoader;
import io.mindmaps.factory.MindmapsClient;
import org.javatuples.Pair;
import org.junit.*;

import static io.mindmaps.IntegrationUtils.graphWithNewKeyspace;
import static org.junit.Assert.*;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.concurrent.*;

import static io.mindmaps.IntegrationUtils.startTestEngine;
import static io.mindmaps.graql.Graql.var;

public class ScalingTestIT {

    private static final String[] HOST_NAME =
            {"localhost"};

    String keyspace;
    MindmapsGraph graph;

    // test parameters
    int NUM_SUPER_NODES = 1; // the number of supernodes to generate in the test graph
    int MAX_SIZE = 150; // the maximum number of non super nodes to add to the test graph
    int NUM_DIVS = 3; // the number of divisions of the MAX_SIZE to use in the scaling test
    int REPEAT = 3; // the number of times to repeat at each size for average runtimes

    // test variables
    int STEP_SIZE;
    List<Integer> graphSizes;

    @BeforeClass
    public static void startController() throws Exception {
        startTestEngine();
    }

    @Before
    public void setUp() throws InterruptedException {
        Pair<MindmapsGraph, String> result = graphWithNewKeyspace();
        graph = result.getValue0();
        keyspace = result.getValue1();

        // compute the sample of graph sizes
        STEP_SIZE = MAX_SIZE/NUM_DIVS;
        graphSizes = new ArrayList<>();
        for (int i = 1;i < NUM_DIVS;i++) graphSizes.add(i*STEP_SIZE);
        graphSizes.add(MAX_SIZE);
    }

    @After
    public void cleanGraph() {
        graph.clear();
    }

    @Test
    public void countAndDegreeIT() throws InterruptedException, ExecutionException, MindmapsValidationException {
        PrintWriter writer = null;

        try {
            writer = new PrintWriter("timingsCountAndDegree.txt", "UTF-8");
        } catch (FileNotFoundException | UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        Map<Integer, Long> scaleToAverageTimeCount = new HashMap<>();
        Map<Integer, Long> scaleToAverageTimeDegree = new HashMap<>();

        // Insert super nodes into graph
        simpleOntology(keyspace);

        Set<String> superNodes = makeSuperNodes(keyspace);

        int previousGraphSize = 0;
        for (int graphSize : graphSizes) {
            writer.println("current scale - super " + NUM_SUPER_NODES + " - nodes " + graphSize);


            writer.println("start generate graph " + System.currentTimeMillis()/1000L + "s");
            writer.flush();
            addNodes(keyspace, previousGraphSize, graphSize);
            addEdgesToSuperNodes(keyspace, superNodes, previousGraphSize, graphSize);
            previousGraphSize = graphSize;
            writer.println("stop generate graph " + System.currentTimeMillis()/1000L + "s");

            Analytics computer = new Analytics(keyspace);

            Long countTime = 0L;
            Long degreeTime = 0L;
            Long degreeAndPersistTime = 0L;
            Long startTime = 0L;
            Long stopTime = 0L;

            for (int i=0;i<REPEAT;i++) {
                writer.println("gremlin count is: " + graph.getTinkerTraversal().V().count().next());
                writer.println("repeat number: "+i);
                writer.flush();
                startTime = System.currentTimeMillis();
                writer.println("count: " + computer.count());
                writer.flush();
                stopTime = System.currentTimeMillis();
                countTime+=stopTime-startTime;
                writer.println("count time: " + countTime / ((i + 1) * 1000));

                writer.println("degree");
                writer.flush();
                startTime = System.currentTimeMillis();
                computer.degrees();
                stopTime = System.currentTimeMillis();
                degreeTime+=stopTime-startTime;
                writer.println("degree time: " + degreeTime / ((i + 1) * 1000));

            }

            countTime /= REPEAT*1000;
            degreeTime /= REPEAT*1000;
            writer.println("time to count: " + countTime);
            scaleToAverageTimeCount.put(graphSize,countTime);
            writer.println("time to degrees: " + degreeTime);
            scaleToAverageTimeDegree.put(graphSize,degreeTime);
        }

        writer.println("counts: " + scaleToAverageTimeCount);
        writer.println("degrees: " + scaleToAverageTimeDegree);

        writer.flush();
        writer.close();
    }

    @Test
    public void persistConstantIncreasingLoadIT() throws InterruptedException, MindmapsValidationException, ExecutionException {

        PrintWriter writer = null;

        try {
            writer = new PrintWriter("timingsPersist.txt", "UTF-8");
        } catch (FileNotFoundException | UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        Long startTime = 0L;
        Long stopTime = 0L;
        Map<Integer, Long> scaleToAverageTimeDegreeAndPersistWrite = new HashMap<>();
        Map<Integer, Long> scaleToAverageTimeDegreeAndPersistMutate = new HashMap<>();

        for (int graphSize : graphSizes) {
            Long degreeAndPersistTimeWrite = 0L;
            Long degreeAndPersistTimeMutate = 0L;

            // repeat persist
            for (int i=0;i<REPEAT;i++) {
                writer.println("repeat number: "+i);
                String CURRENT_KEYSPACE = keyspace + String.valueOf(graphSize) + String.valueOf(i);
                simpleOntology(CURRENT_KEYSPACE);

                // construct graph
                writer.println("start generate graph " + System.currentTimeMillis()/1000L + "s");
                writer.flush();
                addNodes(CURRENT_KEYSPACE, 0, graphSize);
                writer.println("stop generate graph " + System.currentTimeMillis()/1000L + "s");

                graph = MindmapsClient.getGraph(keyspace);
                writer.println("gremlin count is: " + graph.getTinkerTraversal().V().count().next());

                Analytics computer = new Analytics(CURRENT_KEYSPACE);

                writer.println("persist degree");
                writer.flush();
                startTime = System.currentTimeMillis();
                computer.degreesAndPersist();
                stopTime = System.currentTimeMillis();
                degreeAndPersistTimeWrite+=stopTime-startTime;
                writer.println("persist time: " + degreeAndPersistTimeWrite / ((i + 1) * 1000));

                // mutate graph
                writer.println("start mutate graph " + System.currentTimeMillis()/1000L + "s");
                writer.flush();

                // add edges to force mutation
                addEdges(CURRENT_KEYSPACE, graphSize);

                writer.println("stop mutate graph " + System.currentTimeMillis() / 1000L + "s");

                graph = MindmapsClient.getGraph(CURRENT_KEYSPACE);
                writer.println("gremlin count is: " + graph.getTinkerTraversal().V().count().next());

                writer.println("mutate degree");
                computer = new Analytics(CURRENT_KEYSPACE);
                writer.flush();
                startTime = System.currentTimeMillis();
                computer.degreesAndPersist();
                stopTime = System.currentTimeMillis();
                degreeAndPersistTimeMutate+=stopTime-startTime;
                writer.println("mutate time: " + degreeAndPersistTimeMutate / ((i + 1) * 1000));

                writer.println("start clean graph" + System.currentTimeMillis()/1000L + "s");
                graph.clear();
                writer.println("stop clean graph" + System.currentTimeMillis()/1000L + "s");
            }

            degreeAndPersistTimeWrite /= REPEAT*1000;
            degreeAndPersistTimeMutate /= REPEAT*1000;
            writer.println("time to degreesAndPersistWrite: " + degreeAndPersistTimeWrite);
            writer.println("time to degreesAndPersistMutate: " + degreeAndPersistTimeMutate);
            scaleToAverageTimeDegreeAndPersistWrite.put(graphSize,degreeAndPersistTimeWrite);
            scaleToAverageTimeDegreeAndPersistMutate.put(graphSize,degreeAndPersistTimeMutate);
        }

        writer.println("degreesAndPersistWrite: " + scaleToAverageTimeDegreeAndPersistWrite);
        writer.println("degreesAndPersistMutate: " + scaleToAverageTimeDegreeAndPersistMutate);

        writer.flush();
        writer.close();
    }

    @Test
    public void testLargeDegreeMutationResultsInReadableGraphIT() throws MindmapsValidationException, InterruptedException, ExecutionException {

        simpleOntology(keyspace);

        // construct graph
        addNodes(keyspace, 0, MAX_SIZE);

        Analytics computer = new Analytics(keyspace);

        computer.degreesAndPersist();

        // assert mutated degrees are as expected
        graph = MindmapsClient.getGraph(keyspace);
        EntityType thing = graph.getEntityType("thing");
        Collection<Entity> things = thing.instances();

        assertFalse(things.isEmpty());
        things.forEach(thisThing -> {
            assertEquals(1L, thisThing.resources().size());
            assertEquals(0L, thisThing.resources().iterator().next().getValue());
        });

        // add edges to force mutation
        addEdges(keyspace, MAX_SIZE);

        computer = new Analytics(keyspace);

        computer.degreesAndPersist();

        // assert mutated degrees are as expected
        graph = MindmapsClient.getGraph(keyspace);
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
        MindmapsGraph graph = MindmapsClient.getGraph(keyspace);
        EntityType thing = graph.putEntityType("thing");
        RoleType relation1 = graph.putRoleType("relation1");
        RoleType relation2 = graph.putRoleType("relation2");
        thing.playsRole(relation1).playsRole(relation2);
        RelationType related = graph.putRelationType("related").hasRole(relation1).hasRole(relation2);
        graph.commit();
    }

    private Set<String> makeSuperNodes(String keyspace) throws MindmapsValidationException {
        // make the supernodes
        MindmapsGraph graph = MindmapsClient.getGraph(keyspace);
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
