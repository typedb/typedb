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
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.concept.Attribute;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Relationship;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.exception.GraknTxOperationException;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import ai.grakn.kb.internal.GraknTxJanus;
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

import static ai.grakn.util.ErrorMessage.IS_ABSTRACT;
import static ai.grakn.util.ErrorMessage.TX_CLOSED_ON_ACTION;
import static junit.framework.TestCase.assertNotNull;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.emptyArray;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

public class GraknTxJanusTest extends JanusTestBase {
    private EmbeddedGraknTx<?> graknTx;

    @Before
    public void setup(){
        if(graknTx == null || graknTx.isClosed()) {
            graknTx = janusGraphFactory.open(GraknTxType.WRITE);
        }

        when(session.uri()).thenReturn(Grakn.IN_MEMORY);
        when(session.config()).thenReturn(TEST_CONFIG);
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
        type.create();
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
        expectedException.expect(GraknTxOperationException.class);
        expectedException.expectMessage(TX_CLOSED_ON_ACTION.getMessage("closed", graknTx.keyspace()));
        graknTx.putEntityType("This should fail");
    }

    @Test
    public void whenClosingTheGraph_EnsureTheTransactionIsClosed(){
        when(session.keyspace()).thenReturn(Keyspace.of("test"));
        GraknTxJanus graph = new TxFactoryJanus(session).open(GraknTxType.WRITE);

        String entityTypeLabel = "Hello";

        graph.putEntityType(entityTypeLabel);
        assertNotNull(graph.getEntityType(entityTypeLabel));

        graph.close();

        expectedException.expect(GraknTxOperationException.class);
        expectedException.expectMessage(TX_CLOSED_ON_ACTION.getMessage("closed", graph.keyspace()));

        //noinspection ResultOfMethodCallIgnored
        graph.getEntityType(entityTypeLabel);
    }

    @Test
    public void whenCreatingDateResource_EnsureDateCanBeRetrieved(){
        when(session.keyspace()).thenReturn(Keyspace.of("case"));
        GraknTxJanus graph = new TxFactoryJanus(session).open(GraknTxType.WRITE);
        AttributeType<LocalDateTime> dateType = graph.putAttributeType("date", AttributeType.DataType.DATE);
        LocalDateTime now = LocalDateTime.now();
        Attribute<LocalDateTime> date = dateType.create(now);
        assertEquals(now, date.value());
    }

    @Test
    public void whenLookingUpRelationEdgeViaConceptId_EnsureTheRelationEdgeIsReturned(){
        AttributeType<String> attributeType = graknTx.putAttributeType("Looky a attribute type", AttributeType.DataType.STRING);
        Attribute<String> attribute = attributeType.create("A Attribute Thing");

        EntityType entityType = graknTx.putEntityType("My entity").has(attributeType);
        Relationship relationship = Iterators.getOnlyElement(entityType.create().has(attribute).relationships().iterator());

        //Closing so the cache is not accessed when doing the lookup
        graknTx.commit();
        graknTx = janusGraphFactory.open(GraknTxType.WRITE);

        assertEquals(relationship, graknTx.getConcept(relationship.id()));
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
            graphBatchLoading.getEntityType("thingy").create();
            graphBatchLoading.commit();
        } catch (Exception e){
            throw new RuntimeException(e);
        }
    }

    @Test
    public void checkNumberOfOpenTransactionsChangesAsExpected() throws ExecutionException, InterruptedException {
        TxFactoryJanus factory = newFactory();
        EmbeddedGraknTx<?> graph = factory.open(GraknTxType.WRITE);
        graph.close();
        EmbeddedGraknTx<?> batchGraph = factory.open(GraknTxType.BATCH);

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
        EmbeddedGraknTx<?> graph = factory.open(GraknTxType.READ);
        assertEquals(1, openTransactions(graph));
        graph.commit();
        assertEquals(0, openTransactions(graph));
    }

    private int openTransactions(EmbeddedGraknTx<?> graph){
        if(graph == null) return 0;
        return graph.numOpenTx();
    }

    @Test
    public void whenAddingEntitiesToAbstractTypeCreatedInDifferentTransaction_Throw(){
        TxFactoryJanus factory = newFactory();

        String label = "An Abstract thingy";

        try(GraknTx graph = factory.open(GraknTxType.WRITE)){
            graph.putEntityType(label).isAbstract(true);
            graph.commit();
        }

        expectedException.expect(GraknTxOperationException.class);
        expectedException.expectMessage(IS_ABSTRACT.getMessage(label));

        try(GraknTx graph = factory.open(GraknTxType.WRITE)){
            graph.getEntityType(label).create();
        }
    }

    @Test
    public void whenDeletingAConcept_TheConceptIsDeleted() {
        Concept sacrifice = graknTx.putEntityType("sacrifice");

        assertFalse(sacrifice.isDeleted());

        sacrifice.delete();

        assertTrue(sacrifice.isDeleted());
    }

    @Test
    public void whenDeletingARelationshipTypeWithRoles_TheRelationshipTypeIsDeleted() {
        Role murderer = graknTx.putRole("murderer");
        Role victim = graknTx.putRole("victim");
        RelationshipType murder = graknTx.putRelationshipType("murder").relate(murderer).relate(victim);

        murder.delete();

        assertTrue(murder.isDeleted());
        assertThat(murderer.relationships().toArray(), emptyArray());
        assertThat(victim.relationships().toArray(), emptyArray());
    }

    @Test
    public void whenDefiningAndUndefiningType_EnsureTransactionAfterCommitCanBeReopened(){
        String label = "My Entity Type";
        graknTx.putEntityType(label);
        graknTx.commit();

        graknTx = janusGraphFactory.open(GraknTxType.WRITE);
        graknTx.getEntityType(label).delete();
        graknTx.commit();

        graknTx = janusGraphFactory.open(GraknTxType.WRITE);
        assertNull(graknTx.getEntityType(label));
    }
}