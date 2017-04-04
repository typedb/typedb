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

import ai.grakn.Grakn;
import ai.grakn.GraknTxType;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.graph.internal.GraknTitanGraph;
import ai.grakn.util.Schema;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
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

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class TitanInternalFactoryTest extends TitanTestBase{
    private static TitanGraph sharedGraph;

    @BeforeClass
    public static void setupClass() throws InterruptedException {
        sharedGraph = titanGraphFactory.open(GraknTxType.WRITE).getTinkerPopGraph();
    }

    @Test
    public void testGraphConfig() throws InterruptedException {
        TitanManagement management = sharedGraph.openManagement();

        //Test Composite Indices
        String byId = "by" + Schema.ConceptProperty.ID.name();
        String byIndex = "by" + Schema.ConceptProperty.INDEX.name();
        String byValueString = "by" + Schema.ConceptProperty.VALUE_STRING.name();
        String byValueLong = "by" + Schema.ConceptProperty.VALUE_LONG.name();
        String byValueDouble = "by" + Schema.ConceptProperty.VALUE_DOUBLE.name();
        String byValueBoolean = "by" + Schema.ConceptProperty.VALUE_BOOLEAN.name();

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
        Arrays.stream(Schema.ConceptProperty.values()).forEach(property ->
                assertNotNull(management.getPropertyKey(property.name())));
        Arrays.stream(Schema.EdgeProperty.values()).forEach(property ->
                assertNotNull(management.getPropertyKey(property.name())));

        //Test Labels
        Arrays.stream(Schema.BaseType.values()).forEach(label -> assertNotNull(management.getVertexLabel(label.name())));
    }

    @Test
    public void testSingleton(){
        TitanInternalFactory factory = new TitanInternalFactory("anothertest", Grakn.IN_MEMORY, TEST_PROPERTIES);
        GraknTitanGraph mg1 = factory.open(GraknTxType.BATCH);
        TitanGraph tinkerGraphMg1 = mg1.getTinkerPopGraph();
        mg1.close();
        GraknTitanGraph mg2 = factory.open(GraknTxType.WRITE);
        TitanGraph tinkerGraphMg2 = mg2.getTinkerPopGraph();
        mg2.close();
        GraknTitanGraph mg3 = factory.open(GraknTxType.BATCH);

        assertEquals(mg1, mg3);
        assertEquals(tinkerGraphMg1, mg3.getTinkerPopGraph());

        assertTrue(mg1.isBatchLoadingEnabled());
        assertFalse(mg2.isBatchLoadingEnabled());

        assertNotEquals(mg1, mg2);
        assertNotEquals(tinkerGraphMg1, tinkerGraphMg2);

        StandardTitanGraph standardTitanGraph1 = (StandardTitanGraph) tinkerGraphMg1;
        StandardTitanGraph standardTitanGraph2 = (StandardTitanGraph) tinkerGraphMg2;

        assertTrue(standardTitanGraph1.getConfiguration().isBatchLoading());
        assertFalse(standardTitanGraph2.getConfiguration().isBatchLoading());
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
        TitanInternalFactory factory = new TitanInternalFactory("simplekeyspace", Grakn.IN_MEMORY, TEST_PROPERTIES);

        for(int i = 0; i < 200; i ++) {
            futures.add(pool.submit(() -> {
                GraknTitanGraph graph = factory.open(GraknTxType.WRITE);
                assertFalse("Grakn graph is closed", graph.isClosed());
                assertFalse("Internal tinkerpop graph is closed", graph.getTinkerPopGraph().isClosed());
                graph.putEntityType("A Thing");
                try {
                    graph.close();
                } catch (GraknValidationException e) {
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
    public void testGraphNotClosed() throws GraknValidationException {
        TitanInternalFactory factory = new TitanInternalFactory("stuff", Grakn.IN_MEMORY, TEST_PROPERTIES);
        GraknTitanGraph graph = factory.open(GraknTxType.WRITE);
        assertFalse(graph.getTinkerPopGraph().isClosed());
        graph.putEntityType("A Thing");
        graph.close();

        graph = factory.open(GraknTxType.WRITE);
        assertFalse(graph.getTinkerPopGraph().isClosed());
        graph.putEntityType("A Thing");
    }


    private static TitanGraph getGraph() {
        String name = UUID.randomUUID().toString().replaceAll("-", "");
        titanGraphFactory = new TitanInternalFactory(name, Grakn.IN_MEMORY, TEST_PROPERTIES);
        Graph graph = titanGraphFactory.open(GraknTxType.WRITE).getTinkerPopGraph();
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
}