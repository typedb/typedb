package io.mindmaps.factory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.thinkaurelius.titan.core.util.TitanCleanup;
import io.mindmaps.core.dao.MindmapsGraph;
import io.mindmaps.core.implementation.DataType;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.List;
import java.util.ResourceBundle;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.*;

public class MindmapsTitanGraphFactoryTest {
    private final String TEST_CONFIG = "../conf/titan/titan-cassandra-es-test.properties";
    private MindmapsGraphFactory titanGraphFactory ;
    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setup() {
        Logger logger = (Logger) org.slf4j.LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        logger.setLevel(Level.OFF);

        titanGraphFactory = new MindmapsTitanGraphFactory();
    }

    @After
    public void cleanup() throws InterruptedException {
        MindmapsGraph graph = titanGraphFactory.newGraph(TEST_CONFIG);
        graph.clear();
    }

    @Test
    public void testBuildTitanGraph() throws Exception {
        Graph graph = titanGraphFactory.newGraph(TEST_CONFIG).getGraph();
        assertThat(graph, instanceOf(TitanGraph.class));
    }

    @Test
    public void productionIndexConstructionTest() throws InterruptedException {
        TitanGraph graph = (TitanGraph) titanGraphFactory.newGraph(TEST_CONFIG).getGraph();
        TitanManagement management = graph.openManagement();

        assertEquals("byItemIdentifier", management.getGraphIndex("byItemIdentifier").toString());
        assertEquals("bySubjectIdentifier", management.getGraphIndex("bySubjectIdentifier").toString());
        assertEquals("byValueString", management.getGraphIndex("byValueString").toString());
        assertEquals("byValueLong", management.getGraphIndex("byValueLong").toString());
        assertEquals("byValueDouble", management.getGraphIndex("byValueDouble").toString());
        assertEquals("byValueBoolean", management.getGraphIndex("byValueBoolean").toString());
        assertEquals("ITEM_IDENTIFIER", management.getPropertyKey("ITEM_IDENTIFIER").toString());
        assertEquals("SUBJECT_IDENTIFIER", management.getPropertyKey("SUBJECT_IDENTIFIER").toString());
        assertEquals("VALUE_STRING", management.getPropertyKey("VALUE_STRING").toString());
        assertEquals("VALUE_LONG", management.getPropertyKey("VALUE_LONG").toString());
        assertEquals("VALUE_BOOLEAN", management.getPropertyKey("VALUE_BOOLEAN").toString());
        assertEquals("VALUE_DOUBLE", management.getPropertyKey("VALUE_DOUBLE").toString());
    }

    @Test
    public void testBuildIndexedGraphWithCommit() throws Exception {
        clearGraph();
        Graph graph = titanGraphFactory.newGraph(TEST_CONFIG).getGraph();
        addConcepts(graph);
        graph.tx().commit();
        assertIndexCorrect(graph);
    }

    @Test
    public void testBuildIndexedGraphWithoutCommit() throws Exception {
        clearGraph();
        Graph graph = titanGraphFactory.newGraph(TEST_CONFIG).getGraph();
        addConcepts(graph);
        assertIndexCorrect(graph);
    }

    @Test
    public void testVertexLabels(){
        TitanGraph graph = (TitanGraph) titanGraphFactory.newGraph(TEST_CONFIG).getGraph();
        TitanManagement management = graph.openManagement();

        ResourceBundle keys = ResourceBundle.getBundle("base-types");
        Set<String> keyString = keys.keySet();
        for(String label : keyString){
            assertNotNull(management.getVertexLabel(label));
        }
    }

    @Test
    public void testBatchLoading(){
        TitanGraph graph = (TitanGraph) titanGraphFactory.newGraph(TEST_CONFIG).getGraph();
        TitanManagement management = graph.openManagement();

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

        graph.close();
    }

    @Test
    public void testSingleton(){
        Graph graph1 = titanGraphFactory.newGraph("../conf/titan/titan-cassandra-batch-test.properties").getGraph();
        Graph graph2 = titanGraphFactory.newGraph("../conf/titan/titan-cassandra-es-test.properties").getGraph();
        Graph graph3 = titanGraphFactory.newGraph("../conf/titan/titan-cassandra-es.properties").getGraph();
        Graph graph4 = titanGraphFactory.newGraph("../conf/titan/titan-cassandra-batch-test.properties").getGraph();
        Graph graph5 = titanGraphFactory.newGraph("../conf/titan/titan-cassandra-es-test.properties").getGraph();
        Graph graph6 = titanGraphFactory.newGraph("../conf/titan/titan-cassandra-es.properties").getGraph();
        Graph graph7 = titanGraphFactory.newGraph("../conf/titan/titan-cassandra-test.properties").getGraph();

        assertEquals(graph1, graph4);
        assertEquals(graph2, graph5);
        assertEquals(graph3, graph6);
        assertNotEquals(graph2, graph7);

        assertNotSame(graph1, graph2);
        assertNotSame(graph1, graph3);
        assertNotSame(graph1, graph5);
        assertNotSame(graph1, graph6);

        assertNotSame(graph2, graph1);
        assertNotSame(graph2, graph3);
        assertNotSame(graph2, graph4);
        assertNotSame(graph2, graph6);

        assertNotSame(graph3, graph2);
        assertNotSame(graph3, graph1);
        assertNotSame(graph3, graph5);
        assertNotSame(graph3, graph4);
    }

    @Test
    public void testIndexedEdgesFasterThanStandardReverseOrder() throws InterruptedException {

        Integer max = 10000; // set size of test graph
        int nTimes = 100; // number of times to run specific traversal

        // Indexed Lookup /////////////////////////////////////////////////////
        clearGraph();
        Graph graph = titanGraphFactory.newGraph(TEST_CONFIG).getGraph();
        createGraphTestVertexCentricIndex("",graph, max);

        // time the same query multiple times
        Vertex first = graph.traversal().V().has(DataType.ConceptProperty.VALUE_STRING.name(), String.valueOf(0)).next();
        List<Object> indexResult = new ArrayList<>();
        double startTime = System.nanoTime();
        for (int i=0; i<nTimes; i++) {
            indexResult = graph.traversal().V(first).
                    outE(DataType.EdgeLabel.SHORTCUT.getLabel()).
                    has(DataType.EdgeProperty.TO_ROLE.name(), String.valueOf(1)).inV().
                    values(DataType.ConceptProperty.VALUE_STRING.name()).toList();
        }
        double endTime = System.nanoTime();
        double indexDuration = (endTime - startTime);  // this is the difference (divide by 1000000 to get milliseconds).

        // Non-Indexed Lookup /////////////////////////////////////////////////////
        clearGraph();
        graph = titanGraphFactory.newGraph(TEST_CONFIG).getGraph();
        createGraphTestNoIndex("", graph, max);

        // time the same query multiple times
        first = graph.traversal().V().has(DataType.ConceptPropertyUnique.ITEM_IDENTIFIER.name(),String.valueOf(0)).next();
        List<Object> result = new ArrayList<>();
        startTime = System.nanoTime();
        for (int i=0; i<nTimes; i++) {
            result = graph.traversal().V(first).
                    outE(DataType.EdgeLabel.ISA.getLabel()).
                    has(DataType.ConceptProperty.TYPE.name(), String.valueOf(1)).inV().
                    values(DataType.ConceptPropertyUnique.ITEM_IDENTIFIER.name()).toList();
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

        Integer max = 10000; // set size of test graph
        int nTimes = 100; // number of times to run specific traversal

        // Gremlin Indexed Lookup ////////////////////////////////////////////////////
        clearGraph();
        Graph graph = titanGraphFactory.newGraph(TEST_CONFIG).getGraph();
        createGraphTestVertexCentricIndex("",graph, max);

        // time the same query multiple times
        Vertex first = graph.traversal().V().has(DataType.ConceptProperty.VALUE_STRING.name(),String.valueOf(0)).next();
        List<Object> gremlinIndexedTraversalResult = new ArrayList<>();
        double startTime = System.nanoTime();
        for (int i=0; i<nTimes; i++) {
            gremlinIndexedTraversalResult = graph.traversal().V(first).
                    local(__.outE(DataType.EdgeLabel.SHORTCUT.getLabel()).order().by(DataType.EdgeProperty.TO_ROLE.name(), Order.decr).range(0, 10)).
                    inV().values(DataType.ConceptProperty.VALUE_STRING.name()).toList();
        }
        double endTime = System.nanoTime();
        double gremlinIndexedTraversalDuration = (endTime - startTime);  // this is the difference (divide by 1000000 to get milliseconds).

        // Non-Indexed Gremlin Lookup ////////////////////////////////////////////////////
        clearGraph();
        graph = titanGraphFactory.newGraph(TEST_CONFIG).getGraph();
        createGraphTestNoIndex("",graph,max);

        // time the same query multiple times
        first = graph.traversal().V().has(DataType.ConceptPropertyUnique.ITEM_IDENTIFIER.name(), String.valueOf(0)).next();
        List<Object> gremlinTraversalResult = new ArrayList<>();
        startTime = System.nanoTime();
        for (int i=0; i < nTimes; i++) {
            gremlinTraversalResult = graph.traversal().V(first).
                    local(__.outE(DataType.EdgeLabel.ISA.getLabel()).order().by(DataType.ConceptProperty.TYPE.name(), Order.decr).range(0, 10)).
                    inV().values(DataType.ConceptPropertyUnique.ITEM_IDENTIFIER.name()).toList();
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
        clearGraph();
        Graph graph = titanGraphFactory.newGraph(TEST_CONFIG).getGraph();
        createGraphTestVertexCentricIndex("rand",graph, max);

        Vertex first = graph.traversal().V().has(DataType.ConceptProperty.VALUE_STRING.name(),String.valueOf(0)).next();
        List<Object> result, oldResult = new ArrayList<>();
        for (int i=0; i<nTimes; i++) {
            // confirm every iteration fetches exactly the same results
            result = graph.traversal().V(first).
                    local(__.outE(DataType.EdgeLabel.SHORTCUT.getLabel()).order().by(DataType.EdgeProperty.TO_ROLE.name(), Order.decr).range(0, 10)).
                    inV().values(DataType.ConceptProperty.VALUE_STRING.name()).toList();
            if (i>0) assertEquals(result,oldResult);
            oldResult = result;

            // confirm paging works
            List allNodes = graph.traversal().V(first).
                    local(__.outE(DataType.EdgeLabel.SHORTCUT.getLabel()).order().by(DataType.EdgeProperty.TO_ROLE.name(), Order.decr)).
                    inV().values(DataType.ConceptProperty.VALUE_STRING.name()).toList();

            for (int j=0;j<max-1;j++) {
                List currentNode = graph.traversal().V(first).
                        local(__.outE(DataType.EdgeLabel.SHORTCUT.getLabel()).order().by(DataType.EdgeProperty.TO_ROLE.name(), Order.decr).range(j, j + 1)).
                        inV().values(DataType.ConceptProperty.VALUE_STRING.name()).toList();
                assertEquals(currentNode.get(0),allNodes.get(j));
            }
        }

    }

    private void clearGraph() {
        Graph graph = titanGraphFactory.newGraph(TEST_CONFIG).getGraph();
        try {
            graph.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        TitanCleanup.clear((TitanGraph) graph);

    }

    private void addConcepts(Graph graph) {
        Vertex vertex1 = graph.addVertex();
        vertex1.property("ITEM_IDENTIFIER", "www.mindmaps.com/action-movie/");
        vertex1.property(DataType.ConceptProperty.VALUE_STRING.name(), "hi there");

        Vertex vertex2 = graph.addVertex();
        vertex2.property(DataType.ConceptProperty.VALUE_STRING.name(), "hi there");
    }

    private void assertIndexCorrect(Graph graph) {
        assertTrue(graph.traversal().V().has(DataType.ConceptPropertyUnique.ITEM_IDENTIFIER.name(), "www.mindmaps.com/action-movie/").hasNext());
        assertEquals(2, graph.traversal().V().has(DataType.ConceptProperty.VALUE_STRING.name(), "hi there").count().next().longValue());
        assertFalse(graph.traversal().V().has(DataType.ConceptPropertyUnique.ITEM_IDENTIFIER.name(), "mind").hasNext());
        assertFalse(graph.traversal().V().has(DataType.ConceptPropertyUnique.ITEM_IDENTIFIER.name(), "www").hasNext());
        assertFalse(graph.traversal().V().has(DataType.ConceptProperty.VALUE_STRING.name(), "hi").hasNext());
    }

    private void createGraphTestNoIndex(String indexProp,Graph graph, int max) throws InterruptedException {
        createGraphGeneric(indexProp, graph, max, "ITEM_IDENTIFIER", DataType.EdgeLabel.ISA.getLabel(), "TYPE");
    }

    private void createGraphTestVertexCentricIndex(String indexProp,Graph graph, int max) throws InterruptedException {
        createGraphGeneric(indexProp,graph,max,DataType.ConceptProperty.VALUE_STRING.name(), DataType.EdgeLabel.SHORTCUT.getLabel(), DataType.EdgeProperty.TO_ROLE.name());
    }

    private void createGraphGeneric(String indexProp,Graph graph,int max,String nodeProp,String edgeLabel,String edgeProp) throws InterruptedException {
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
            pLoad.submit(() -> addSpecificNodes(indexProp,graph,start.get(j),end.get(j),nodeProp,edgeLabel,edgeProp));
        }
        pLoad.shutdown();
        pLoad.awaitTermination(100, TimeUnit.SECONDS);
    }

    private void addSpecificNodes(String indexProp, Graph graph, int start, int end,String nodeProp,String edgeLabel,String edgeProp) {
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