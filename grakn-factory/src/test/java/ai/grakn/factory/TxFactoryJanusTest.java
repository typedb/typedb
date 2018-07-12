/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.factory;

import ai.grakn.Grakn;
import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.exception.InvalidKBException;
import ai.grakn.kb.internal.GraknTxJanus;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.ResourceBundle;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

public class TxFactoryJanusTest extends JanusTestBase {
    private static JanusGraph sharedGraph;

    @BeforeClass
    public static void setupClass() throws InterruptedException {
        sharedGraph = janusGraphFactory.open(GraknTxType.WRITE).getTinkerPopGraph();
        when(session.uri()).thenReturn(Grakn.IN_MEMORY);
        when(session.config()).thenReturn(TEST_CONFIG);
    }

    @Test
    public void testGraphConfig() throws InterruptedException {
        JanusGraphManagement management = sharedGraph.openManagement();

        //Test Composite Indices
        String byId = "by" + Schema.VertexProperty.ID.name();
        String byIndex = "by" + Schema.VertexProperty.INDEX.name();
        String byValueString = "by" + Schema.VertexProperty.VALUE_STRING.name();
        String byValueLong = "by" + Schema.VertexProperty.VALUE_LONG.name();
        String byValueDouble = "by" + Schema.VertexProperty.VALUE_DOUBLE.name();
        String byValueBoolean = "by" + Schema.VertexProperty.VALUE_BOOLEAN.name();

        assertEquals(byId, management.getGraphIndex(byId).toString());
        assertEquals(byIndex, management.getGraphIndex(byIndex).toString());
        assertEquals(byValueString, management.getGraphIndex(byValueString).toString());
        assertEquals(byValueLong, management.getGraphIndex(byValueLong).toString());
        assertEquals(byValueDouble, management.getGraphIndex(byValueDouble).toString());
        assertEquals(byValueBoolean, management.getGraphIndex(byValueBoolean).toString());

        //Text Edge Indices
        ResourceBundle keys = ResourceBundle.getBundle("indices-edges");
        Set<String> keyString = keys.keySet();
        for(String label : keyString){
            assertNotNull(management.getEdgeLabel(label));
        }

        //Test Properties
        Arrays.stream(Schema.VertexProperty.values()).forEach(property ->
                assertNotNull(management.getPropertyKey(property.name())));
        Arrays.stream(Schema.EdgeProperty.values()).forEach(property ->
                assertNotNull(management.getPropertyKey(property.name())));

        //Test Labels
        Arrays.stream(Schema.BaseType.values()).forEach(label -> assertNotNull(management.getVertexLabel(label.name())));
    }

    @Test
    public void testSingleton(){
        when(session.keyspace()).thenReturn(Keyspace.of("anothertest"));
        TxFactoryJanus factory = new TxFactoryJanus(session);
        GraknTxJanus mg1 = factory.open(GraknTxType.BATCH);
        JanusGraph tinkerGraphMg1 = mg1.getTinkerPopGraph();
        mg1.close();
        GraknTxJanus mg2 = factory.open(GraknTxType.WRITE);
        JanusGraph tinkerGraphMg2 = mg2.getTinkerPopGraph();
        mg2.close();
        GraknTxJanus mg3 = factory.open(GraknTxType.BATCH);

        assertEquals(mg1, mg3);
        assertEquals(tinkerGraphMg1, mg3.getTinkerPopGraph());

        assertTrue(mg1.isBatchTx());
        assertFalse(mg2.isBatchTx());

        assertNotEquals(mg1, mg2);
        assertNotEquals(tinkerGraphMg1, tinkerGraphMg2);

        StandardJanusGraph standardJanusGraph1 = (StandardJanusGraph) tinkerGraphMg1;
        StandardJanusGraph standardJanusGraph2 = (StandardJanusGraph) tinkerGraphMg2;

        assertTrue(standardJanusGraph1.getConfiguration().isBatchLoading());
        assertFalse(standardJanusGraph2.getConfiguration().isBatchLoading());
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
    public void testMultithreadedRetrievalOfGraphs(){
        Set<Future> futures = new HashSet<>();
        ExecutorService pool = Executors.newFixedThreadPool(10);

        when(session.keyspace()).thenReturn(Keyspace.of("simplekeyspace"));
        TxFactoryJanus factory = new TxFactoryJanus(session);

        for(int i = 0; i < 200; i ++) {
            futures.add(pool.submit(() -> {
                GraknTxJanus graph = factory.open(GraknTxType.WRITE);
                assertFalse("Grakn graph is closed", graph.isClosed());
                assertFalse("Internal tinkerpop graph is closed", graph.getTinkerPopGraph().isClosed());
                graph.putEntityType("A Thing");
                try {
                    graph.close();
                } catch (InvalidKBException e) {
                    e.printStackTrace();
                }
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
    public void testGraphNotClosed() throws InvalidKBException {
        when(session.keyspace()).thenReturn(Keyspace.of("stuff"));
        TxFactoryJanus factory = new TxFactoryJanus(session);
        GraknTxJanus graph = factory.open(GraknTxType.WRITE);
        assertFalse(graph.getTinkerPopGraph().isClosed());
        graph.putEntityType("A Thing");
        graph.close();

        graph = factory.open(GraknTxType.WRITE);
        assertFalse(graph.getTinkerPopGraph().isClosed());
        graph.putEntityType("A Thing");
    }

    @Test
    public void whenSeveralFactoriesCreateTheSameKeyspace_NoErrorOccurs() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(10);

        Runnable createGraph = () -> {
            TxFactoryJanus factory = new TxFactoryJanus(session);
            factory.buildTinkerPopGraph(false);
        };

        Set<Future<?>> futures = Stream.generate(() -> createGraph)
                .limit(10)
                .map(executor::submit)
                .collect(Collectors.toSet());

        try {
            for (Future<?> future : futures) {
                future.get();
            }
        } finally {
            executor.shutdown();
        }
    }


    private static JanusGraph getGraph() {
        Keyspace name = Keyspace.of("hehe" + UUID.randomUUID().toString().replaceAll("-", ""));
        when(session.keyspace()).thenReturn(name);
        janusGraphFactory = new TxFactoryJanus(session);
        Graph graph = janusGraphFactory.open(GraknTxType.WRITE).getTinkerPopGraph();
        assertThat(graph, instanceOf(JanusGraph.class));
        return (JanusGraph) graph;
    }

    private void addConcepts(Graph graph) {
        Vertex vertex1 = graph.addVertex();
        vertex1.property("ITEM_IDENTIFIER", "www.grakn.com/action-movie/");
        vertex1.property(Schema.VertexProperty.VALUE_STRING.name(), "hi there");

        Vertex vertex2 = graph.addVertex();
        vertex2.property(Schema.VertexProperty.VALUE_STRING.name(), "hi there");
    }

    private void assertIndexCorrect(Graph graph) {
        assertEquals(2, graph.traversal().V().has(Schema.VertexProperty.VALUE_STRING.name(), "hi there").count().next().longValue());
        assertFalse(graph.traversal().V().has(Schema.VertexProperty.VALUE_STRING.name(), "hi").hasNext());
    }
}