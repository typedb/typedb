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
import ai.grakn.GraknGraph;
import ai.grakn.GraknTxType;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Relation;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.exception.GraphOperationException;
import ai.grakn.graph.internal.AbstractGraknGraph;
import ai.grakn.graph.internal.GraknJanusGraph;
import com.google.common.collect.Iterators;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static ai.grakn.util.ErrorMessage.GRAPH_CLOSED_ON_ACTION;
import static ai.grakn.util.ErrorMessage.IS_ABSTRACT;
import static junit.framework.TestCase.assertNotNull;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class GraknJanusGraphTest extends JanusTestBase {
    private GraknGraph graknGraph;

    @Before
    public void setup(){
        if(graknGraph == null || graknGraph.isClosed()) {
            graknGraph = janusGraphFactory.open(GraknTxType.WRITE);
        }
    }

    @After
    public void cleanup(){
        if(!graknGraph.isClosed()) {
            graknGraph.close();
        }
    }

    @Test
    public void whenCreatingIndependentMutatingTransactionsConcurrently_TheGraphIsUpdatedSafely() throws ExecutionException, InterruptedException {
        Set<Future> futures = new HashSet<>();
        ExecutorService pool = Executors.newFixedThreadPool(40);

        EntityType type = graknGraph.putEntityType("A Type");
        graknGraph.commit();

        for(int i = 0; i < 100; i ++){
            futures.add(pool.submit(() -> addEntity(type)));
        }

        for (Future future : futures) {
            future.get();
        }

        graknGraph = janusGraphFactory.open(GraknTxType.WRITE);
        assertEquals(100, graknGraph.admin().getMetaEntityType().instances().size());
    }
    private void addEntity(EntityType type){
        GraknJanusGraph graph = janusGraphFactory.open(GraknTxType.WRITE);
        type.addEntity();
        graph.commit();
    }

    @Test
    public void whenAbortingTransaction_ChangesNotCommitted(){
        String label = "My New Type";
        graknGraph.putEntityType(label);
        graknGraph.abort();
        graknGraph = janusGraphFactory.open(GraknTxType.WRITE);
        assertNull(graknGraph.getEntityType(label));
    }

    @Test
    public void whenAbortingTransaction_GraphIsClosedBecauseOfAbort(){
        graknGraph.abort();
        assertTrue("Aborting transaction did not close the graph", graknGraph.isClosed());
        expectedException.expect(GraphOperationException.class);
        expectedException.expectMessage(GRAPH_CLOSED_ON_ACTION.getMessage("closed", graknGraph.getKeyspace()));
        graknGraph.putEntityType("This should fail");
    }

    @Test
    public void whenCreatingGraphsWithDifferentKeyspace_EnsureCaseIsIgnored(){
        JanusInternalFactory factory1 =  new JanusInternalFactory("case", Grakn.IN_MEMORY, TEST_PROPERTIES);
        JanusInternalFactory factory2 = new JanusInternalFactory("Case", Grakn.IN_MEMORY, TEST_PROPERTIES);
        GraknJanusGraph case1 = factory1.open(GraknTxType.WRITE);
        GraknJanusGraph case2 = factory2.open(GraknTxType.WRITE);

        assertEquals(case1.getKeyspace(), case2.getKeyspace());
    }

    @Test
    public void whenClosingTheGraph_EnsureTheTransactionIsClosed(){
        GraknJanusGraph graph = new JanusInternalFactory("test", Grakn.IN_MEMORY, TEST_PROPERTIES).open(GraknTxType.WRITE);

        String entityTypeLabel = "Hello";

        graph.putEntityType(entityTypeLabel);
        assertNotNull(graph.getEntityType(entityTypeLabel));

        graph.close();

        expectedException.expect(GraphOperationException.class);
        expectedException.expectMessage(GRAPH_CLOSED_ON_ACTION.getMessage("closed", graph.getKeyspace()));

        //noinspection ResultOfMethodCallIgnored
        graph.getEntityType(entityTypeLabel);
    }

    @Test
    public void whenCreatingDateResource_EnsureDateCanBeRetrieved(){
        GraknJanusGraph graph = new JanusInternalFactory("case", Grakn.IN_MEMORY, TEST_PROPERTIES).open(GraknTxType.WRITE);
        ResourceType<LocalDateTime> dateType = graph.putResourceType("date", ResourceType.DataType.DATE);
        LocalDateTime now = LocalDateTime.now();
        Resource<LocalDateTime> date = dateType.putResource(now);
        assertEquals(now, date.getValue());
    }

    @Test
    public void whenLookingUpRelationEdgeViaConceptId_EnsureTheRelationEdgeIsReturned(){
        ResourceType<String> resourceType = graknGraph.putResourceType("Looky a resource type", ResourceType.DataType.STRING);
        Resource<String> resource = resourceType.putResource("A Resource Thing");

        EntityType entityType = graknGraph.putEntityType("My entity").resource(resourceType);
        Relation relation = Iterators.getOnlyElement(entityType.addEntity().resource(resource).relations().iterator());

        //Closing so the cache is not accessed when doing the lookup
        graknGraph.commit();
        graknGraph = janusGraphFactory.open(GraknTxType.WRITE);

        assertEquals(relation, graknGraph.getConcept(relation.getId()));
    }

    @Test //This test is performed here because it depends on actual transaction behaviour which tinker does not exhibit
    public void whenClosingTransaction_EnsureConceptTransactionCachesAreCleared(){
        JanusInternalFactory factory = newFactory();
        GraknGraph graph = factory.open(GraknTxType.WRITE);

        EntityType entityType = graph.admin().getMetaEntityType();
        EntityType newEntityType = graph.putEntityType("New Entity Type");
        assertThat(entityType.subs(), containsInAnyOrder(entityType, newEntityType));

        graph.close();

        graph = factory.open(GraknTxType.WRITE);
        assertThat(entityType.subs(), containsInAnyOrder(entityType));

        graph.close();
    }

    @Test
    public void whenCommitting_EnsureGraphTransactionIsClosed() throws Exception {
        JanusInternalFactory factory = newFactory();
        GraknGraph graph = factory.open(GraknTxType.WRITE);
        graph.putEntityType("thingy");
        graph.commit();
        assertTrue(graph.isClosed());

        HashSet<Future> futures = new HashSet<>();
        futures.add(Executors.newCachedThreadPool().submit(() -> addThingToBatch(factory)));

        for (Future future : futures) {
            future.get();
        }

        assertTrue(graph.isClosed());
    }

    private void addThingToBatch(JanusInternalFactory factory){
        try(GraknGraph graphBatchLoading = factory.open(GraknTxType.WRITE)) {
            graphBatchLoading.getEntityType("thingy").addEntity();
            graphBatchLoading.commit();
        } catch (Exception e){
            throw new RuntimeException(e);
        }
    }

    @Test
    public void checkNumberOfOpenTransactionsChangesAsExpected() throws ExecutionException, InterruptedException {
        JanusInternalFactory factory = newFactory();
        GraknGraph graph = factory.open(GraknTxType.WRITE);
        graph.close();
        GraknGraph batchGraph = factory.open(GraknTxType.BATCH);

        for(int i = 0; i < 6; i ++){
            Executors.newSingleThreadExecutor().submit(() -> factory.open(GraknTxType.WRITE)).get();
        }

        for(int i = 0; i < 2; i ++){
            Executors.newSingleThreadExecutor().submit(() -> factory.open(GraknTxType.BATCH)).get();
        }

        assertEquals(6, openTransactions(graph));
        assertEquals(3, openTransactions(batchGraph));

        graph.close();
        batchGraph.close();
    }

    @Test
    public void afterCommitting_NumberOfOpenTransactionsDecrementsOnce() {
        JanusInternalFactory factory = newFactory();
        GraknGraph graph = factory.open(GraknTxType.READ);
        assertEquals(1, openTransactions(graph));
        graph.commit();
        assertEquals(0, openTransactions(graph));
    }

    private int openTransactions(GraknGraph graph){
        if(graph == null) return 0;
        return ((AbstractGraknGraph) graph).numOpenTx();
    }

    @Test
    public void whenAddingEntitiesToAbstractTypeCreatedInDifferentTransaction_Throw(){
        JanusInternalFactory factory = newFactory();

        String label = "An Abstract thingy";

        try(GraknGraph graph = factory.open(GraknTxType.WRITE)){
            graph.putEntityType(label).setAbstract(true);
            graph.commit();
        }

        expectedException.expect(GraphOperationException.class);
        expectedException.expectMessage(IS_ABSTRACT.getMessage(label));

        try(GraknGraph graph = factory.open(GraknTxType.WRITE)){
            graph.getEntityType(label).addEntity();
        }
    }
}