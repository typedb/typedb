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

package io.mindmaps.factory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import io.mindmaps.util.Schema;
import io.mindmaps.graph.internal.MindmapsTitanGraph;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.List;
import java.util.ResourceBundle;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class MindmapsTitanGraphFactoryTest {
    private final static String TEST_CONFIG = "../conf/test/mindmaps-test.properties";
    private final static String TEST_URI = "localhost";
    private final static String TEST_SHARED = "shared";
    private static final boolean TEST_BATCH_LOADING = false;

    private static TitanGraph sharedGraph;
    private static TitanGraph noIndexGraph;
    private static TitanGraph indexGraph;

    private static MindmapsGraphFactory titanGraphFactory ;
    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @BeforeClass
    public static void setupClass() throws InterruptedException {
        Logger logger = (Logger) org.slf4j.LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        logger.setLevel(Level.OFF);

        titanGraphFactory = new MindmapsTitanGraphFactory();

        sharedGraph = ((MindmapsTitanGraph) titanGraphFactory.getGraph(TEST_SHARED, TEST_URI, TEST_CONFIG, TEST_BATCH_LOADING)).getTinkerPopGraph();

        int max = 1000;
        noIndexGraph = getGraph();
        createGraphTestNoIndex("", noIndexGraph, max);

        indexGraph = getGraph();
        createGraphTestVertexCentricIndex("", indexGraph, max);
    }

    @Test
    public void productionIndexConstructionTest() throws InterruptedException {
        TitanManagement management = sharedGraph.openManagement();

        assertEquals("byItemIdentifier", management.getGraphIndex("byItemIdentifier").toString());
        assertEquals("byValueString", management.getGraphIndex("byValueString").toString());
        assertEquals("byValueLong", management.getGraphIndex("byValueLong").toString());
        assertEquals("byValueDouble", management.getGraphIndex("byValueDouble").toString());
        assertEquals("byValueBoolean", management.getGraphIndex("byValueBoolean").toString());
        assertEquals("ITEM_IDENTIFIER", management.getPropertyKey("ITEM_IDENTIFIER").toString());
        assertEquals("VALUE_STRING", management.getPropertyKey("VALUE_STRING").toString());
        assertEquals("VALUE_LONG", management.getPropertyKey("VALUE_LONG").toString());
        assertEquals("VALUE_BOOLEAN", management.getPropertyKey("VALUE_BOOLEAN").toString());
        assertEquals("VALUE_DOUBLE", management.getPropertyKey("VALUE_DOUBLE").toString());
    }

    @Test
    public void testSimpleBuild(){
        MindmapsTitanGraph mg1 = (MindmapsTitanGraph) titanGraphFactory.getGraph(TEST_SHARED, TEST_URI, TEST_CONFIG, true);
        MindmapsTitanGraph mg2 = (MindmapsTitanGraph) titanGraphFactory.getGraph(TEST_SHARED, TEST_URI, TEST_CONFIG, false);

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

        ResourceBundle keys = ResourceBundle.getBundle("base-types");
        Set<String> keyString = keys.keySet();
        for(String label : keyString){
            assertNotNull(management.getVertexLabel(label));
        }
    }

    @Test
    public void testBatchLoading(){
        TitanManagement management = sharedGraph.openManagement();

        ResourceBundle keys = ResourceBundle.getBundle("property-keys");
        Set<String> keyString = keys.keySet();
        for(String propertyKey : keyString){
            assertNotNull(management.getPropertyKey(propertyKey));
        }

        keys = ResourceBundle.getBundle("indices-edges");
        keyString = keys.keySet();
        for(String label : keyString){
            assertNotNull(management.getEdgeLabel(label));
        }
    }

    @Test
    public void testSingleton(){
        Graph graph1 = sharedGraph;
        Graph graph2 = ((MindmapsTitanGraph) titanGraphFactory.getGraph("b", TEST_URI, TEST_CONFIG, TEST_BATCH_LOADING)).getTinkerPopGraph();
        Graph graph3 = ((MindmapsTitanGraph) titanGraphFactory.getGraph(TEST_SHARED, TEST_URI, TEST_CONFIG, TEST_BATCH_LOADING)).getTinkerPopGraph();

        assertEquals(graph1, graph3);
        assertNotEquals(graph2, graph1);
    }

    @Test
    public void testIndexedEdgesFasterThanStandardReverseOrder() throws InterruptedException {

        int nTimes = 100; // number of times to run specific traversal

        // Indexed Lookup /////////////////////////////////////////////////////
        // time the same query multiple times
        Vertex first = indexGraph.traversal().V().has(Schema.ConceptProperty.VALUE_STRING.name(), String.valueOf(0)).next();
        List<Object> indexResult = new ArrayList<>();
        double startTime = System.nanoTime();
        for (int i=0; i<nTimes; i++) {
            indexResult = indexGraph.traversal().V(first).
                    outE(Schema.EdgeLabel.SHORTCUT.getLabel()).
                    has(Schema.EdgeProperty.TO_ROLE.name(), String.valueOf(1)).inV().
                    values(Schema.ConceptProperty.VALUE_STRING.name()).toList();
        }
        double endTime = System.nanoTime();
        double indexDuration = (endTime - startTime);  // this is the difference (divide by 1000000 to get milliseconds).

        // Non-Indexed Lookup /////////////////////////////////////////////////////
        // time the same query multiple times
        first = noIndexGraph.traversal().V().has(Schema.ConceptPropertyUnique.ITEM_IDENTIFIER.name(),String.valueOf(0)).next();
        List<Object> result = new ArrayList<>();
        startTime = System.nanoTime();
        for (int i=0; i<nTimes; i++) {
            result = noIndexGraph.traversal().V(first).
                    outE(Schema.EdgeLabel.ISA.getLabel()).
                    has(Schema.ConceptProperty.TYPE.name(), String.valueOf(1)).inV().
                    values(Schema.ConceptPropertyUnique.ITEM_IDENTIFIER.name()).toList();
        }
        endTime = System.nanoTime();
        double duration = (endTime - startTime);  //divide by 1000000 to get milliseconds.

        System.out.println("Indexed lookup (ms): " + indexDuration / 1E6);
        System.out.println("Non-Indexed lookup (ms): " + duration / 1E6);

        // check that the indexed version is at least twice as fast
        assertEquals(indexResult, result);
        assertTrue(indexDuration < duration / 2);

    }


    @Test
    public void retrieveOrderedEdgeViaVertexCentricIndexTest() throws InterruptedException {
        // For some reason the first query will take longer by default.
        // Therefore the query that is expected to run fastest is placed first.

        int nTimes = 100; // number of times to run specific traversal

        // Gremlin Indexed Lookup ////////////////////////////////////////////////////

        // time the same query multiple times
        Vertex first = indexGraph.traversal().V().has(Schema.ConceptProperty.VALUE_STRING.name(),String.valueOf(0)).next();
        List<Object> gremlinIndexedTraversalResult = new ArrayList<>();
        double startTime = System.nanoTime();
        for (int i=0; i<nTimes; i++) {
            gremlinIndexedTraversalResult = indexGraph.traversal().V(first).
                    local(__.outE(Schema.EdgeLabel.SHORTCUT.getLabel()).order().by(Schema.EdgeProperty.TO_ROLE.name(), Order.decr).range(0, 10)).
                    inV().values(Schema.ConceptProperty.VALUE_STRING.name()).toList();
        }
        double endTime = System.nanoTime();
        double gremlinIndexedTraversalDuration = (endTime - startTime);  // this is the difference (divide by 1000000 to get milliseconds).

        // Non-Indexed Gremlin Lookup ////////////////////////////////////////////////////

        // time the same query multiple times
        first = noIndexGraph.traversal().V().has(Schema.ConceptPropertyUnique.ITEM_IDENTIFIER.name(), String.valueOf(0)).next();
        List<Object> gremlinTraversalResult = new ArrayList<>();
        startTime = System.nanoTime();
        for (int i=0; i < nTimes; i++) {
            gremlinTraversalResult = noIndexGraph.traversal().V(first).
                    local(__.outE(Schema.EdgeLabel.ISA.getLabel()).order().by(Schema.ConceptProperty.TYPE.name(), Order.decr).range(0, 10)).
                    inV().values(Schema.ConceptPropertyUnique.ITEM_IDENTIFIER.name()).toList();
        }
        endTime = System.nanoTime();
        double gremlinTraversalDuration = (endTime - startTime);  //divide by 1000000 to get milliseconds.

        System.out.println("Indexed lookup (ms): " + gremlinIndexedTraversalDuration/1E6);
        System.out.println("Non-Indexed lookup (ms): " + gremlinTraversalDuration/1E6);

        assertEquals(gremlinIndexedTraversalResult, gremlinTraversalResult);
        assertTrue(gremlinIndexedTraversalDuration < gremlinTraversalDuration/2);
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
                    local(__.outE(Schema.EdgeLabel.SHORTCUT.getLabel()).order().by(Schema.EdgeProperty.TO_ROLE.name(), Order.decr).range(0, 10)).
                    inV().values(Schema.ConceptProperty.VALUE_STRING.name()).toList();
            if (i>0) assertEquals(result,oldResult);
            oldResult = result;

            // confirm paging works
            List allNodes = graph.traversal().V(first).
                    local(__.outE(Schema.EdgeLabel.SHORTCUT.getLabel()).order().by(Schema.EdgeProperty.TO_ROLE.name(), Order.decr)).
                    inV().values(Schema.ConceptProperty.VALUE_STRING.name()).toList();

            for (int j=0;j<max-1;j++) {
                List currentNode = graph.traversal().V(first).
                        local(__.outE(Schema.EdgeLabel.SHORTCUT.getLabel()).order().by(Schema.EdgeProperty.TO_ROLE.name(), Order.decr).range(j, j + 1)).
                        inV().values(Schema.ConceptProperty.VALUE_STRING.name()).toList();
                assertEquals(currentNode.get(0),allNodes.get(j));
            }
        }

    }

    private static TitanGraph getGraph() {
        String name = UUID.randomUUID().toString();
        Graph graph = ((MindmapsTitanGraph) titanGraphFactory.getGraph(name, TEST_URI, TEST_CONFIG, TEST_BATCH_LOADING)).getTinkerPopGraph();
        assertThat(graph, instanceOf(TitanGraph.class));
        return (TitanGraph) graph;
    }

    private void addConcepts(Graph graph) {
        Vertex vertex1 = graph.addVertex();
        vertex1.property("ITEM_IDENTIFIER", "www.mindmaps.com/action-movie/");
        vertex1.property(Schema.ConceptProperty.VALUE_STRING.name(), "hi there");

        Vertex vertex2 = graph.addVertex();
        vertex2.property(Schema.ConceptProperty.VALUE_STRING.name(), "hi there");
    }

    private void assertIndexCorrect(Graph graph) {
        assertTrue(graph.traversal().V().has(Schema.ConceptPropertyUnique.ITEM_IDENTIFIER.name(), "www.mindmaps.com/action-movie/").hasNext());
        assertEquals(2, graph.traversal().V().has(Schema.ConceptProperty.VALUE_STRING.name(), "hi there").count().next().longValue());
        assertFalse(graph.traversal().V().has(Schema.ConceptPropertyUnique.ITEM_IDENTIFIER.name(), "mind").hasNext());
        assertFalse(graph.traversal().V().has(Schema.ConceptPropertyUnique.ITEM_IDENTIFIER.name(), "www").hasNext());
        assertFalse(graph.traversal().V().has(Schema.ConceptProperty.VALUE_STRING.name(), "hi").hasNext());
    }

    private static void createGraphTestNoIndex(String indexProp,Graph graph, int max) throws InterruptedException {
        createGraphGeneric(indexProp, graph, max, "ITEM_IDENTIFIER", Schema.EdgeLabel.ISA.getLabel(), "TYPE");
    }

    private static void createGraphTestVertexCentricIndex(String indexProp,Graph graph, int max) throws InterruptedException {
        createGraphGeneric(indexProp,graph,max, Schema.ConceptProperty.VALUE_STRING.name(), Schema.EdgeLabel.SHORTCUT.getLabel(), Schema.EdgeProperty.TO_ROLE.name());
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