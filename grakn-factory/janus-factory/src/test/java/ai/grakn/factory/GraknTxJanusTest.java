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
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Relation;
import ai.grakn.concept.Attribute;
import ai.grakn.exception.GraphOperationException;
import ai.grakn.graph.internal.GraknTxAbstract;
import ai.grakn.graph.internal.GraknTxJanus;
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
import java.util.stream.Collectors;

import static ai.grakn.util.ErrorMessage.GRAPH_CLOSED_ON_ACTION;
import static ai.grakn.util.ErrorMessage.IS_ABSTRACT;
import static junit.framework.TestCase.assertNotNull;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class GraknTxJanusTest extends JanusTestBase {
    private GraknTx graknTx;

    @Before
    public void setup(){
        if(graknTx == null || graknTx.isClosed()) {
            graknTx = janusGraphFactory.open(GraknTxType.WRITE);
        }
    }

    @After
    public void cleanup(){
        if(!graknTx.isClosed()) {
            graknTx.close();
        }
    }

    @Test
    public void whenCreatingIndependentMutatingTransactionsConcurrently_TheGraphIsUpdatedSafely() throws ExecutionException, InterruptedException {
        Set<Future> futures = new HashSet<>();
        ExecutorService pool = Executors.newFixedThreadPool(40);

        EntityType type = graknTx.putEntityType("A Type");
        graknTx.commit();

        for(int i = 0; i < 100; i ++){
            futures.add(pool.submit(() -> addEntity(type)));
        }

        for (Future future : futures) {
            future.get();
        }

        graknTx = janusGraphFactory.open(GraknTxType.WRITE);
        assertEquals(100L, graknTx.admin().getMetaEntityType().instances().count());
    }
    private void addEntity(EntityType type){
        GraknTxJanus graph = janusGraphFactory.open(GraknTxType.WRITE);
        type.addEntity();
        graph.commit();
    }

    @Test
    public void whenAbortingTransaction_ChangesNotCommitted(){
        String label = "My New Type";
        graknTx.putEntityType(label);
        graknTx.abort();
        graknTx = janusGraphFactory.open(GraknTxType.WRITE);
        assertNull(graknTx.getEntityType(label));
    }

    @Test
    public void whenAbortingTransaction_GraphIsClosedBecauseOfAbort(){
        graknTx.abort();
        assertTrue("Aborting transaction did not close the graph", graknTx.isClosed());
        expectedException.expect(GraphOperationException.class);
        expectedException.expectMessage(GRAPH_CLOSED_ON_ACTION.getMessage("closed", graknTx.getKeyspace()));
        graknTx.putEntityType("This should fail");
    }

    @Test
    public void whenCreatingGraphsWithDifferentKeyspace_EnsureCaseIsIgnored(){
        TxFactoryJanus factory1 =  new TxFactoryJanus("case", Grakn.IN_MEMORY, TEST_PROPERTIES);
        TxFactoryJanus factory2 = new TxFactoryJanus("Case", Grakn.IN_MEMORY, TEST_PROPERTIES);
        GraknTxJanus case1 = factory1.open(GraknTxType.WRITE);
        GraknTxJanus case2 = factory2.open(GraknTxType.WRITE);

        assertEquals(case1.getKeyspace(), case2.getKeyspace());
    }

    @Test
    public void whenClosingTheGraph_EnsureTheTransactionIsClosed(){
        GraknTxJanus graph = new TxFactoryJanus("test", Grakn.IN_MEMORY, TEST_PROPERTIES).open(GraknTxType.WRITE);

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
        GraknTxJanus graph = new TxFactoryJanus("case", Grakn.IN_MEMORY, TEST_PROPERTIES).open(GraknTxType.WRITE);
        AttributeType<LocalDateTime> dateType = graph.putAttributeType("date", AttributeType.DataType.DATE);
        LocalDateTime now = LocalDateTime.now();
        Attribute<LocalDateTime> date = dateType.putAttribute(now);
        assertEquals(now, date.getValue());
    }

    @Test
    public void whenLookingUpRelationEdgeViaConceptId_EnsureTheRelationEdgeIsReturned(){
        AttributeType<String> attributeType = graknTx.putAttributeType("Looky a attribute type", AttributeType.DataType.STRING);
        Attribute<String> attribute = attributeType.putAttribute("A Attribute Thing");

        EntityType entityType = graknTx.putEntityType("My entity").resource(attributeType);
        Relation relation = Iterators.getOnlyElement(entityType.addEntity().resource(attribute).relations().iterator());

        //Closing so the cache is not accessed when doing the lookup
        graknTx.commit();
        graknTx = janusGraphFactory.open(GraknTxType.WRITE);

        assertEquals(relation, graknTx.getConcept(relation.getId()));
    }

    @Test //This test is performed here because it depends on actual transaction behaviour which tinker does not exhibit
    public void whenClosingTransaction_EnsureConceptTransactionCachesAreCleared(){
        TxFactoryJanus factory = newFactory();
        GraknTx graph = factory.open(GraknTxType.WRITE);

        EntityType entityType = graph.admin().getMetaEntityType();
        EntityType newEntityType = graph.putEntityType("New Entity Type");
        assertThat(entityType.subs().collect(Collectors.toSet()), containsInAnyOrder(entityType, newEntityType));

        graph.close();

        graph = factory.open(GraknTxType.WRITE);
        assertThat(entityType.subs().collect(Collectors.toSet()), containsInAnyOrder(entityType));

        graph.close();
    }

    @Test
    public void whenCommitting_EnsureGraphTransactionIsClosed() throws Exception {
        TxFactoryJanus factory = newFactory();
        GraknTx graph = factory.open(GraknTxType.WRITE);
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

    private void addThingToBatch(TxFactoryJanus factory){
        try(GraknTx graphBatchLoading = factory.open(GraknTxType.WRITE)) {
            graphBatchLoading.getEntityType("thingy").addEntity();
            graphBatchLoading.commit();
        } catch (Exception e){
            throw new RuntimeException(e);
        }
    }

    @Test
    public void checkNumberOfOpenTransactionsChangesAsExpected() throws ExecutionException, InterruptedException {
        TxFactoryJanus factory = newFactory();
        GraknTx graph = factory.open(GraknTxType.WRITE);
        graph.close();
        GraknTx batchGraph = factory.open(GraknTxType.BATCH);

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
        TxFactoryJanus factory = newFactory();
        GraknTx graph = factory.open(GraknTxType.READ);
        assertEquals(1, openTransactions(graph));
        graph.commit();
        assertEquals(0, openTransactions(graph));
    }

    private int openTransactions(GraknTx graph){
        if(graph == null) return 0;
        return ((GraknTxAbstract) graph).numOpenTx();
    }

    @Test
    public void whenAddingEntitiesToAbstractTypeCreatedInDifferentTransaction_Throw(){
        TxFactoryJanus factory = newFactory();

        String label = "An Abstract thingy";

        try(GraknTx graph = factory.open(GraknTxType.WRITE)){
            graph.putEntityType(label).setAbstract(true);
            graph.commit();
        }

        expectedException.expect(GraphOperationException.class);
        expectedException.expectMessage(IS_ABSTRACT.getMessage(label));

        try(GraknTx graph = factory.open(GraknTxType.WRITE)){
            graph.getEntityType(label).addEntity();
        }
    }
}