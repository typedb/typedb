/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.test.graql.analytics;

import ai.grakn.GraknGraph;
import ai.grakn.GraknGraphFactory;
import ai.grakn.Grakn;
import ai.grakn.concept.Entity;
import ai.grakn.concept.Relation;
import ai.grakn.concept.RelationType;
import ai.grakn.engine.loader.client.LoaderClient;
import ai.grakn.graph.internal.AbstractGraknGraph;
import ai.grakn.graql.internal.analytics.Analytics;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.RoleType;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.test.AbstractScalingTest;
import org.apache.commons.csv.CSVFormat;
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
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static ai.grakn.graql.Graql.insert;
import static ai.grakn.graql.Graql.var;
import static java.lang.Math.pow;
import static java.lang.Math.sqrt;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * These tests are used for generating a report of the performance of analytics. In order to run them on a machine use
 * this maven command: mvn test -Dtest=ScalingTestIT -DfailIfNoTests=false -Pscaling
 *
 * NB: Grakn must be running on a machine already and you may need to significantly increase the size of the java
 * heap to stop failures.
 */
public class ScalingTestIT extends AbstractScalingTest {

    private static final String[] HOST_NAME =
            {"localhost"};

    String keyspace;
    private GraknGraphFactory factory;

    // test parameters
    int NUM_SUPER_NODES = 10; // the number of supernodes to generate in the test graph
    int MAX_SIZE = 100000; // the maximum number of non super nodes to add to the test graph
    int NUM_DIVS = 4; // the number of divisions of the MAX_SIZE to use in the scaling test
    int REPEAT = 3; // the number of times to repeat at each size for average runtimes
    int MAX_WORKERS = Runtime.getRuntime().availableProcessors(); // the maximum number of workers that spark should use
    int WORKER_DIVS = 4; // the number of divisions of MAX_WORKERS to use for testing

    // test variables
    int STEP_SIZE;
    List<Integer> graphSizes;
    List<Integer> workerNumbers;
    List<String> headers;

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
        GraknGraph graph = factory.getGraph();
        keyspace = graph.getKeyspace();

        headers = new ArrayList<>();
        headers.add("Size");
        headers.addAll(workerNumbers.stream().map(String::valueOf).collect(Collectors.toList()));
    }

    @After
    public void cleanGraph() {
        GraknGraph graph = Grakn.factory(Grakn.DEFAULT_URI, keyspace).getGraph();
        graph.clear();
    }

    @Test
    public void countIT() throws InterruptedException, ExecutionException, GraknValidationException, IOException {
        CSVPrinter printer = createCSVPrinter("countIT.txt");

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

                GraknGraph graph = Grakn.factory(Grakn.DEFAULT_URI, keyspace).getGraph();

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
    public void persistConstantIncreasingLoadIT() throws InterruptedException, GraknValidationException, ExecutionException, IOException {
        CSVPrinter printerWrite = createCSVPrinter("persistConstantIncreasingLoadITWrite.txt");
        CSVPrinter printerMutate = createCSVPrinter("persistConstantIncreasingLoadITMutate.txt");

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

                    GraknGraph graph = Grakn.factory(Grakn.DEFAULT_URI, CURRENT_KEYSPACE).getGraph();
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

                    graph = Grakn.factory(Grakn.DEFAULT_URI, CURRENT_KEYSPACE).getGraph();
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
        ((AbstractGraknGraph) factory.getGraph()).getTinkerPopGraph().close();

        Analytics computer = new Analytics(keyspace, new HashSet<>(), new HashSet<>());

        computer.degreesAndPersist();

        // assert mutated degrees are as expected
        GraknGraph graph = factory.getGraph();
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
        ((AbstractGraknGraph) factory.getGraph()).getTinkerPopGraph().close();

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
     *
     * The min:
     *
     * min(g) = (S-g)*N + 1
     *
     * The max:
     *
     * max(g) = 2*N*(g+S)
     *
     * The mean:
     *
     * mean(g) = sum(g)/(2*g*N)
     *
     * The std:
     *
     * ss = 5*(1/6 + SN + S^2*N^2) + 3*g*N(1/2 +S*N) + 5*g^2*N^2/3
     *
     * std(g) = sqrt(ss/2 - mean(g))
     *
     * The median:
     *
     * median(g) = S*N
     */
    @Test
    public void testStatisticsWithConstantDegree() throws IOException, GraknValidationException {
        int totalSteps = NUM_DIVS;
        int nodesPerStep = MAX_SIZE/NUM_DIVS/2;
        int v_m = totalSteps*nodesPerStep;
        int V_m = 2*(totalSteps*nodesPerStep+1);

        // detail methods that must be executed when testing
        List<String> methods = new ArrayList<>();
        Map<String,Function<AnalyticsMock,Optional>> statisticsMethods = new HashMap<>();
        Map<String,Consumer<Number>> statisticsAssertions = new HashMap<>();
        methods.add("testStatisticsWithConstantDegreeSum.txt");
        statisticsMethods.put(methods.get(0),analyticsMock -> analyticsMock.sum());
        methods.add("testStatisticsWithConstantDegreeMin.txt");
        statisticsMethods.put(methods.get(1),analyticsMock -> analyticsMock.min());
        methods.add("testStatisticsWithConstantDegreeMax.txt");
        statisticsMethods.put(methods.get(2),analyticsMock -> analyticsMock.max());
        methods.add("testStatisticsWithConstantDegreeMean.txt");
        statisticsMethods.put(methods.get(3),analyticsMock -> analyticsMock.mean());
        methods.add("testStatisticsWithConstantDegreeStd.txt");
        statisticsMethods.put(methods.get(4),analyticsMock -> analyticsMock.std());
        methods.add("testStatisticsWithConstantDegreeMedian.txt");
        statisticsMethods.put(methods.get(5),analyticsMock -> analyticsMock.median());

        // load up the result files
        Map<String,CSVPrinter> printers = new HashMap<>();
        for (String method: methods) {
            printers.put(method,createCSVPrinter(method));
        }

        // generic output
        PrintWriter writer = null;
        try {
            writer = new PrintWriter("testStatisticsWithConstantDegreeProgress.txt", "UTF-8");
        } catch (FileNotFoundException | UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        // create the ontology
        simpleOntology(keyspace);
        new Analytics(keyspace, new HashSet<>(), new HashSet<>());

        LoaderClient loaderClient = new LoaderClient(keyspace, Arrays.asList(HOST_NAME));
//        loaderClient.setThreadsNumber(30);
        loaderClient.setPollingFrequency(1000);
        loaderClient.setBatchSize(100);

        for (int g=1; g<totalSteps+1; g++) {
            writer.println("starting step: "+g);

            // load data
            writer.println("start loading data");
            writer.flush();
            for (int m=1; m<nodesPerStep+1; m++) {
                loaderClient.add(insert(var().isa("thing").has("degree", v_m)));
                loaderClient.add(insert(var().isa("thing").has("degree", V_m)));
                v_m--;
                V_m+=2;
            }
            loaderClient.waitToFinish();
            writer.println("stop loading data");
            writer.println("gremlin count is: " + factory.getGraph().getTinkerTraversal().count().next());
            writer.flush();

            for (String method : methods) {
                printers.get(method).print(2 * g * nodesPerStep);
                writer.println("starting to execute: "+method);
                for (int workerNumber : workerNumbers) {
                    writer.println("starting with: " + workerNumber + " threads");

                    // configure assertions
                    final long currentG = Long.valueOf(g);
                    final long N = Long.valueOf(nodesPerStep);
                    final long S = Long.valueOf(totalSteps);
                    statisticsAssertions.put(methods.get(0), number -> {
                        Number sum = currentG * N * (1L + currentG * N + 6L * S * N + 2L) / 2L;
                        assertEquals(sum.doubleValue(),
                                number.doubleValue(), 1E-9);
                    });
                    statisticsAssertions.put(methods.get(1), number -> {
                        Number min = (S-currentG)*N+1L;
                        assertEquals(min.doubleValue(),
                                number.doubleValue(), 1E-9);
                    });
                    statisticsAssertions.put(methods.get(2), number -> {
                        Number max = (S+currentG)*N*2D;
                        assertEquals(max.doubleValue(),
                                number.doubleValue(), 1E-9);
                    });
                    statisticsAssertions.put(methods.get(3), number -> {
                        double mean = meanOfSequence(currentG, N, S);
                        assertEquals(mean,
                                number.doubleValue(), 1E-9);
                    });
                    statisticsAssertions.put(methods.get(4), number -> {
                        double std = stdOfSequence(currentG, N, S);
                        assertEquals(std,
                                number.doubleValue(), 1E-9);
                    });
                    statisticsAssertions.put(methods.get(5), number -> {
                        Number median = S*N;
                        assertEquals(median.doubleValue(),
                                number.doubleValue(), 1E-9);
                    });

                    long averageTime = 0;
                    for (int i = 0; i < REPEAT; i++) {
                        writer.println("starting repeat: " + i);
                        writer.flush();
                        // check stats are correct
                        Long startTime = System.currentTimeMillis();
                        Number currentResult = (Number) statisticsMethods.get(method).apply(new AnalyticsMock(keyspace, new HashSet<String>(), Collections.singleton("degree"), workerNumber)).get();
                        Long stopTime = System.currentTimeMillis();
                        averageTime += stopTime - startTime;
                        statisticsAssertions.get(method).accept(currentResult);
                    }
                    averageTime /= REPEAT * 1000;
                    printers.get(method).print(averageTime);
                }
                printers.get(method).println();
                printers.get(method).flush();
            }
        }

        for (String method : methods) {
            printers.get(method).flush();
            printers.get(method).close();
        }
        factory.getGraph().clear();
    }

    private double meanOfSequence(long currentG, long nodesPerStep, long totalSteps) {
        return ((double) (1 + currentG * nodesPerStep + 6 * totalSteps * nodesPerStep + 2) / 4.0);
    }

    private double stdOfSequence(long currentG, long nodesPerStep, long totalSteps) {
        double mean = meanOfSequence(currentG, nodesPerStep, totalSteps);
        double S = (double) totalSteps;
        double N = (double) nodesPerStep;
        double g = (double) currentG;
        double t = 5.0*(1.0/6.0 + S*N + pow(S*N,2.0));
        t += 3.0*g*N*(1.0/2.0 + S*N);
        t += 5.0*pow(g*N,2.0)/3.0;
        return sqrt(t/2.0 - pow(mean,2.0));
    }

    private CSVPrinter createCSVPrinter(String fileName) throws IOException {
        Appendable out = new PrintWriter(fileName,"UTF-8");
        return CSVFormat.DEFAULT.withHeader(headers.toArray(new String[0])).print(out);

    }

    private void addNodes(String keyspace, int startRange, int endRange) throws GraknValidationException, InterruptedException {
        // batch in the nodes
        LoaderClient loaderClient = new LoaderClient(keyspace,
                Arrays.asList(HOST_NAME));
//        loaderClient.setThreadsNumber(30);
        loaderClient.setPollingFrequency(1000);
        loaderClient.setBatchSize(100);

        for (int nodeIndex = startRange; nodeIndex < endRange; nodeIndex++) {
            String nodeId = "node-" + nodeIndex;
            loaderClient.add(insert(var().isa("thing").id(nodeId)));
        }

        loaderClient.waitToFinish();

    }

    private void addEdgesToSuperNodes(String keyspace, Set<String> superNodes, int startRange, int endRange) {
        // batch in the nodes
        LoaderClient loaderClient = new LoaderClient(keyspace,
                Arrays.asList(HOST_NAME));
//        loaderClient.setThreadsNumber(30);
        loaderClient.setPollingFrequency(1000);
        loaderClient.setBatchSize(100);

        for (String supernodeId : superNodes) {
            for (int nodeIndex = startRange; nodeIndex < endRange; nodeIndex++) {
                String nodeId = "node-" + nodeIndex;
                loaderClient.add(insert(var().isa("related")
                        .rel("relation1", var().id(nodeId))
                        .rel("relation2", var().id(supernodeId))));
            }
        }

        loaderClient.waitToFinish();
    }

    private void simpleOntology(String keyspace) throws GraknValidationException {
        GraknGraph graph = Grakn.factory(Grakn.DEFAULT_URI, keyspace).getGraph();
        EntityType thing = graph.putEntityType("thing");
        RoleType relation1 = graph.putRoleType("relation1");
        RoleType relation2 = graph.putRoleType("relation2");
        thing.playsRole(relation1).playsRole(relation2);
        RelationType related = graph.putRelationType("related").hasRole(relation1).hasRole(relation2);
        graph.commit();
    }

    private Set<String> makeSuperNodes(String keyspace) throws GraknValidationException {
        // make the supernodes
        GraknGraph graph = Grakn.factory(Grakn.DEFAULT_URI, keyspace).getGraph();
        EntityType thing = graph.getEntityType("thing");
        RoleType relation1 = graph.getRoleType("relation1");
        RoleType relation2 = graph.getRoleType("relation2");
        RelationType related = graph.getRelationType("related");
        Set<String> superNodes = new HashSet<>();
        for (int i = 0; i < NUM_SUPER_NODES; i++) {
            superNodes.add(thing.addEntity().getId());
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
        LoaderClient loaderClient = new LoaderClient(keyspace,
                Arrays.asList(HOST_NAME));
//        loaderClient.setThreadsNumber(30);
        loaderClient.setPollingFrequency(1000);
        loaderClient.setBatchSize(100);

        int startNode = 0;
        while (startNode<graphSize) {

            String nodeId1 = "node-" + startNode;
            String nodeId2 = "node-" + ++startNode;
            loaderClient.add(insert(var().isa("related")
                    .rel("relation1", var().id(nodeId1))
                    .rel("relation2", var().id(nodeId2))));

            startNode++;
        }
        loaderClient.waitToFinish();
    }
}
