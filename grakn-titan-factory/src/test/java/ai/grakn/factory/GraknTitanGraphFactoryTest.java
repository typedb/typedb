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

package ai.grakn.factory;

import ai.grakn.GraknGraph;
import ai.grakn.graph.internal.GraknTitanGraph;
import ai.grakn.util.Schema;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.ResourceBundle;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class GraknTitanGraphFactoryTest extends TitanTestBase{
    private final static String TEST_SHARED = "shared";

    private static TitanGraph sharedGraph;
    private static TitanGraph noIndexGraph;
    private static TitanGraph indexGraph;

    private static InternalFactory titanGraphFactory ;


    @BeforeClass
    public static void setupClass() throws InterruptedException {
        Logger logger = (Logger) org.slf4j.LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        logger.setLevel(Level.OFF);

        titanGraphFactory = new TitanInternalFactory(TEST_SHARED, TEST_URI, TEST_CONFIG);

        sharedGraph = ((GraknTitanGraph) titanGraphFactory.getGraph(TEST_BATCH_LOADING)).getTinkerPopGraph();

        int max = 1000;
        noIndexGraph = getGraph();
        createGraphTestNoIndex("", noIndexGraph, max);

        indexGraph = getGraph();
        createGraphTestVertexCentricIndex("", indexGraph, max);
    }

    @Test
    public void productionIndexConstructionTest() throws InterruptedException {
        TitanManagement management = sharedGraph.openManagement();

        assertEquals("byIndex", management.getGraphIndex("byIndex").toString());
        assertEquals("byValueString", management.getGraphIndex("byValueString").toString());
        assertEquals("byValueLong", management.getGraphIndex("byValueLong").toString());
        assertEquals("byValueDouble", management.getGraphIndex("byValueDouble").toString());
        assertEquals("byValueBoolean", management.getGraphIndex("byValueBoolean").toString());
        assertEquals("NAME", management.getPropertyKey("NAME").toString());
        assertEquals("VALUE_STRING", management.getPropertyKey("VALUE_STRING").toString());
        assertEquals("VALUE_LONG", management.getPropertyKey("VALUE_LONG").toString());
        assertEquals("VALUE_BOOLEAN", management.getPropertyKey("VALUE_BOOLEAN").toString());
        assertEquals("VALUE_DOUBLE", management.getPropertyKey("VALUE_DOUBLE").toString());
    }

    @Test
    public void testSimpleBuild(){
        GraknTitanGraph mg1 = (GraknTitanGraph) titanGraphFactory.getGraph(true);
        GraknTitanGraph mg2 = (GraknTitanGraph) titanGraphFactory.getGraph(false);

        assertTrue(mg1.isBatchLoadingEnabled());
        assertFalse(mg2.isBatchLoadingEnabled());
        assertNotEquals(mg1, mg2);
        assertNotEquals(mg1.getTinkerPopGraph(), mg2.getTinkerPopGraph());
    }

    @Test
    public void testBuildIndexedGraphWithCommit() throws Exception {
        Graph graph = getGraph();
        addConcepts(graph);
        graph.tx().commit();
        assertIndexCorrect(graph);
    }

    @Test
    public void testBuildIndexedGraphWithoutCommit() throws Exception {
        Graph graph = getGraph();
        addConcepts(graph);
        assertIndexCorrect(graph);
    }

    @Test
    public void testVertexLabels(){
        TitanManagement management = sharedGraph.openManagement();
        for (Schema.BaseType baseType : Schema.BaseType.values()) {
            assertNotNull(management.getVertexLabel(baseType.name()));
        }
    }

    @Test
    public void testBatchLoading(){
        TitanManagement management = sharedGraph.openManagement();

        Arrays.stream(Schema.ConceptProperty.values()).forEach(property ->
                assertNotNull(management.getPropertyKey(property.name())));
        Arrays.stream(Schema.EdgeProperty.values()).forEach(property ->
                assertNotNull(management.getPropertyKey(property.name())));

        ResourceBundle keys = ResourceBundle.getBundle("indices-edges");
        Set<String> keyString = keys.keySet();
        for(String label : keyString){
            assertNotNull(management.getEdgeLabel(label));
        }
    }

    @Test
    public void testSingleton(){
        GraknGraph mg1 = titanGraphFactory.getGraph(TEST_BATCH_LOADING);
        GraknGraph mg2 = titanGraphFactory.getGraph(TEST_BATCH_LOADING);

        Graph graph1 = ((GraknTitanGraph) titanGraphFactory.getGraph(TEST_BATCH_LOADING)).getTinkerPopGraph();
        Graph graph2 = ((GraknTitanGraph) titanGraphFactory.getGraph(TEST_BATCH_LOADING)).getTinkerPopGraph();

        assertEquals(mg1, mg2);
        assertEquals(graph1, graph2);
    }

    @Test
    public void confirmPagingOfResultsHasCorrectBehaviour() throws InterruptedException {
        Integer max = 100; // set size of test graph
        int nTimes = 10; // number of times to run specific traversal

        // Gremlin Indexed Lookup ////////////////////////////////////////////////////
        Graph graph = getGraph();
        createGraphTestVertexCentricIndex("rand",graph, max);

        Vertex first = graph.traversal().V().has(Schema.ConceptProperty.VALUE_STRING.name(),String.valueOf(0)).next();
        List<Object> result, oldResult = new ArrayList<>();
        for (int i=0; i<nTimes; i++) {
            // confirm every iteration fetches exactly the same results
            result = graph.traversal().V(first).
                    local(__.outE(Schema.EdgeLabel.SHORTCUT.getLabel()).order().by(Schema.EdgeProperty.TO_ROLE_NAME.name(), Order.decr).range(0, 10)).
                    inV().values(Schema.ConceptProperty.VALUE_STRING.name()).toList();
            if (i>0) assertEquals(result,oldResult);
            oldResult = result;

            // confirm paging works
            List allNodes = graph.traversal().V(first).
                    local(__.outE(Schema.EdgeLabel.SHORTCUT.getLabel()).order().by(Schema.EdgeProperty.TO_ROLE_NAME.name(), Order.decr)).
                    inV().values(Schema.ConceptProperty.VALUE_STRING.name()).toList();

            for (int j=0;j<max-1;j++) {
                List currentNode = graph.traversal().V(first).
                        local(__.outE(Schema.EdgeLabel.SHORTCUT.getLabel()).order().by(Schema.EdgeProperty.TO_ROLE_NAME.name(), Order.decr).range(j, j + 1)).
                        inV().values(Schema.ConceptProperty.VALUE_STRING.name()).toList();
                assertEquals(currentNode.get(0),allNodes.get(j));
            }
        }

    }

    @Ignore //TODO: Race condition
    @Test
    public void testMultithreadedRetrievalOfGraphs(){
        Set<Future> futures = new HashSet<>();
        ExecutorService pool = Executors.newFixedThreadPool(10);
        TitanInternalFactory factory = new TitanInternalFactory("simplekeyspace", TEST_URI, TEST_CONFIG);

        for(int i = 0; i < 200; i ++) {
            futures.add(pool.submit(() -> {
                GraknTitanGraph graph = factory.getGraph(false);
                assertFalse("Grakn graph is closed", graph.isClosed());
                assertFalse("Internal tinkerpop graph is closed", graph.getTinkerPopGraph().isClosed());
                graph.putEntityType("A Thing");
                graph.close();
            }));
        }

        boolean exceptionThrown = false;
        for (Future future: futures){
            try {
                future.get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e){
                e.printStackTrace();
                exceptionThrown = true;
            }

            assertFalse(exceptionThrown);
        }
    }

    @Test
    public void testGraphNotClosed(){
        GraknTitanGraph graph = (GraknTitanGraph) titanGraphFactory.getGraph(false);
        assertFalse(graph.getTinkerPopGraph().isClosed());
        graph.putEntityType("A Thing");
        graph.close();

        graph = (GraknTitanGraph) titanGraphFactory.getGraph(false);
        assertFalse(graph.getTinkerPopGraph().isClosed());
        graph.putEntityType("A Thing");
    }


    private static TitanGraph getGraph() {
        String name = UUID.randomUUID().toString().replaceAll("-", "");
        titanGraphFactory = new TitanInternalFactory(name, TEST_URI, TEST_CONFIG);
        Graph graph = ((GraknTitanGraph) titanGraphFactory.getGraph(TEST_BATCH_LOADING)).getTinkerPopGraph();
        assertThat(graph, instanceOf(TitanGraph.class));
        return (TitanGraph) graph;
    }

    private void addConcepts(Graph graph) {
        Vertex vertex1 = graph.addVertex();
        vertex1.property("ITEM_IDENTIFIER", "www.grakn.com/action-movie/");
        vertex1.property(Schema.ConceptProperty.VALUE_STRING.name(), "hi there");

        Vertex vertex2 = graph.addVertex();
        vertex2.property(Schema.ConceptProperty.VALUE_STRING.name(), "hi there");
    }

    private void assertIndexCorrect(Graph graph) {
        assertEquals(2, graph.traversal().V().has(Schema.ConceptProperty.VALUE_STRING.name(), "hi there").count().next().longValue());
        assertFalse(graph.traversal().V().has(Schema.ConceptProperty.VALUE_STRING.name(), "hi").hasNext());
    }

    private static void createGraphTestNoIndex(String indexProp,Graph graph, int max) throws InterruptedException {
        createGraphGeneric(indexProp, graph, max, "ITEM_IDENTIFIER", Schema.EdgeLabel.ISA.getLabel(), "TYPE");
    }

    private static void createGraphTestVertexCentricIndex(String indexProp,Graph graph, int max) throws InterruptedException {
        createGraphGeneric(indexProp,graph,max, Schema.ConceptProperty.VALUE_STRING.name(), Schema.EdgeLabel.SHORTCUT.getLabel(), Schema.EdgeProperty.TO_ROLE_NAME.name());
    }

    private static void createGraphGeneric(String indexProp,Graph graph,int max,String nodeProp,String edgeLabel,String edgeProp) throws InterruptedException {
        ExecutorService pLoad = Executors.newFixedThreadPool(1000);
        int commitSize = 10;

        graph.addVertex(nodeProp, String.valueOf(0));
        graph.tx().commit();

        // get the list of start and end points
        int x=1;
        List<Integer> start = new ArrayList<>();
        List<Integer> end = new ArrayList<>();
        while (x<max) {
            start.add(x);
            if (x+commitSize<max) {
                end.add(x+commitSize);
            } else {
                end.add(max);
            }
            x += commitSize;
        }

        for (int i=0;i < start.size();i++) {
            final int j = i;
            pLoad.submit(() -> addSpecificNodes(indexProp, graph, start.get(j), end.get(j), nodeProp, edgeLabel, edgeProp));
        }
        pLoad.shutdown();
        pLoad.awaitTermination(100, TimeUnit.SECONDS);
    }

    private static void addSpecificNodes(String indexProp, Graph graph, int start, int end,String nodeProp,String edgeLabel,String edgeProp) {
        TitanTransaction transaction = ((TitanGraph) graph).newTransaction();
        Vertex first = transaction.traversal().V().has(nodeProp, String.valueOf("0")).next();
        Integer edgePropValue;
        for (Integer i=start; i<end; i++) {
            Vertex current = transaction.addVertex(nodeProp, i.toString());
            if (indexProp.equals("rand")) {
                edgePropValue = ThreadLocalRandom.current().nextInt(1, 11);
            } else {
                edgePropValue = i;
            }
            first.addEdge(edgeLabel, current, edgeProp, edgePropValue.toString());
        }
        transaction.commit();
    }
}