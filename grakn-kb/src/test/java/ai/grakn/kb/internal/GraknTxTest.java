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
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.kb.internal;

import ai.grakn.Grakn;
import ai.grakn.GraknSession;
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.concept.Attribute;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Label;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.concept.Type;
import ai.grakn.exception.GraknTxOperationException;
import ai.grakn.exception.InvalidKBException;
import ai.grakn.factory.EmbeddedGraknSession;
import ai.grakn.kb.internal.concept.EntityTypeImpl;
import ai.grakn.kb.internal.structure.Shard;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.VerificationException;
import org.hamcrest.core.IsInstanceOf;
import org.junit.Test;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toSet;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class GraknTxTest extends TxTestBase {

    @Test
    public void whenGettingConceptById_ReturnTheConcept(){
        EntityType entityType = tx.putEntityType("test-name");
        assertEquals(entityType, tx.getConcept(entityType.getId()));
    }

    @Test
    public void whenAttemptingToMutateViaTraversal_Throw(){
        expectedException.expect(VerificationException.class);
        expectedException.expectMessage("not read only");
        tx.getTinkerTraversal().V().drop().iterate();
    }

    @Test
    public void whenGettingResourcesByValue_ReturnTheMatchingResources(){
        String targetValue = "Geralt";
        assertThat(tx.getAttributesByValue(targetValue), is(empty()));

        AttributeType<String> t1 = tx.putAttributeType("Parent 1", AttributeType.DataType.STRING);
        AttributeType<String> t2 = tx.putAttributeType("Parent 2", AttributeType.DataType.STRING);

        Attribute<String> r1 = t1.putAttribute(targetValue);
        Attribute<String> r2 = t2.putAttribute(targetValue);
        t2.putAttribute("Dragon");

        assertThat(tx.getAttributesByValue(targetValue), containsInAnyOrder(r1, r2));
    }

    @Test
    public void whenGettingTypesByName_ReturnTypes(){
        String entityTypeLabel = "My Entity Type";
        String relationTypeLabel = "My Relationship Type";
        String roleTypeLabel = "My Role Type";
        String resourceTypeLabel = "My Attribute Type";
        String ruleTypeLabel = "My Rule Type";

        assertNull(tx.getEntityType(entityTypeLabel));
        assertNull(tx.getRelationshipType(relationTypeLabel));
        assertNull(tx.getRole(roleTypeLabel));
        assertNull(tx.getAttributeType(resourceTypeLabel));
        assertNull(tx.getRule(ruleTypeLabel));

        EntityType entityType = tx.putEntityType(entityTypeLabel);
        RelationshipType relationshipType = tx.putRelationshipType(relationTypeLabel);
        Role role = tx.putRole(roleTypeLabel);
        AttributeType attributeType = tx.putAttributeType(resourceTypeLabel, AttributeType.DataType.STRING);

        assertEquals(entityType, tx.getEntityType(entityTypeLabel));
        assertEquals(relationshipType, tx.getRelationshipType(relationTypeLabel));
        assertEquals(role, tx.getRole(roleTypeLabel));
        assertEquals(attributeType, tx.getAttributeType(resourceTypeLabel));
    }

    @Test
    public void whenGettingSubTypesFromRootMeta_IncludeAllTypes(){
        EntityType sampleEntityType = tx.putEntityType("Sample Entity Type");
        RelationshipType sampleRelationshipType = tx.putRelationshipType("Sample Relationship Type");

        assertThat(tx.getMetaConcept().subs().collect(toSet()), containsInAnyOrder(
                tx.getMetaConcept(),
                tx.getMetaRelationType(),
                tx.getMetaEntityType(),
                tx.getMetaAttributeType(),
                sampleEntityType,
                sampleRelationshipType
        ));
    }

    @Test
    public void whenGettingTheShardingThreshold_TheCorrectValueIsReturned(){
        assertEquals(100000L, tx.shardingThreshold());
    }

    @Test
    public void whenClosingReadOnlyGraph_EnsureTypesAreCached(){
        assertCacheOnlyContainsMetaTypes();
        //noinspection ResultOfMethodCallIgnored
        tx.getMetaConcept().subs(); //This loads some types into transaction cache
        tx.abort();
        assertCacheOnlyContainsMetaTypes(); //Ensure central cache is empty

        tx = EmbeddedGraknSession.create(tx.keyspace(), Grakn.IN_MEMORY).open(GraknTxType.READ);

        Set<SchemaConcept> finalTypes = new HashSet<>();
        finalTypes.addAll(tx.getMetaConcept().subs().collect(toSet()));
        finalTypes.add(tx.getMetaRole());
        finalTypes.add(tx.getMetaRule());

        tx.abort();

        for (SchemaConcept type : tx.getGlobalCache().getCachedTypes().values()) {
            assertTrue("Type [" + type + "] is missing from central cache after closing read only graph", finalTypes.contains(type));
        }
    }
    private void assertCacheOnlyContainsMetaTypes(){
        Set<Label> metas = Stream.of(Schema.MetaSchema.values()).map(Schema.MetaSchema::getLabel).collect(toSet());
        tx.getGlobalCache().getCachedTypes().keySet().forEach(cachedLabel -> assertTrue("Type [" + cachedLabel + "] is missing from central cache", metas.contains(cachedLabel)));
    }

    @Test
    public void whenBuildingAConceptFromAVertex_ReturnConcept(){
        EntityTypeImpl et = (EntityTypeImpl) tx.putEntityType("Sample Entity Type");
        assertEquals(et, tx.factory().buildConcept(et.vertex()));
    }

    @Test
    public void whenPassingGraphToAnotherThreadWithoutOpening_Throw() throws ExecutionException, InterruptedException {
        ExecutorService pool = Executors.newSingleThreadExecutor();
        GraknTx graph = Grakn.session(Grakn.IN_MEMORY, "testing").open(GraknTxType.WRITE);

        expectedException.expectCause(IsInstanceOf.instanceOf(GraknTxOperationException.class));
        expectedException.expectMessage(GraknTxOperationException.transactionClosed(graph, null).getMessage());

        Future future = pool.submit(() -> {
            graph.putEntityType("A Thing");
        });
        future.get();
    }

    @Test
    public void attemptingToUseClosedGraphFailingThenOpeningGraph_EnsureGraphIsUsable() throws InvalidKBException {
        GraknTx graph = Grakn.session(Grakn.IN_MEMORY, "testingagain").open(GraknTxType.WRITE);
        graph.close();

        boolean errorThrown = false;
        try{
            graph.putEntityType("A Thing");
        } catch (GraknTxOperationException e){
            if(e.getMessage().equals(ErrorMessage.TX_CLOSED_ON_ACTION.getMessage("closed", graph.keyspace()))){
                errorThrown = true;
            }
        }
        assertTrue("Graph not correctly closed", errorThrown);

        graph = Grakn.session(Grakn.IN_MEMORY, "testingagain").open(GraknTxType.WRITE);
        graph.putEntityType("A Thing");
    }

    @Test
    public void checkThatMainCentralCacheIsNotAffectedByTransactionModifications() throws InvalidKBException, ExecutionException, InterruptedException {
        //Check Central cache is empty
        assertCacheOnlyContainsMetaTypes();

        Role r1 = tx.putRole("r1");
        Role r2 = tx.putRole("r2");
        EntityType e1 = tx.putEntityType("e1").plays(r1).plays(r2);
        RelationshipType rel1 = tx.putRelationshipType("rel1").relates(r1).relates(r2);

        //Purge the above concepts into the main cache
        tx.commit();
        tx = EmbeddedGraknSession.create(tx.keyspace(), Grakn.IN_MEMORY).open(GraknTxType.WRITE);

        //Check cache is in good order
        Collection<SchemaConcept> cachedValues = tx.getGlobalCache().getCachedTypes().values();
        assertTrue("Type [" + r1 + "] was not cached", cachedValues.contains(r1));
        assertTrue("Type [" + r2 + "] was not cached", cachedValues.contains(r2));
        assertTrue("Type [" + e1 + "] was not cached", cachedValues.contains(e1));
        assertTrue("Type [" + rel1 + "] was not cached", cachedValues.contains(rel1));

        assertThat(e1.plays().collect(toSet()), containsInAnyOrder(r1, r2));

        ExecutorService pool = Executors.newSingleThreadExecutor();
        //Mutate Schema in a separate thread
        pool.submit(() -> {
            GraknTx innerGraph = Grakn.session(Grakn.IN_MEMORY, tx.keyspace()).open(GraknTxType.WRITE);
            EntityType entityType = innerGraph.getEntityType("e1");
            Role role = innerGraph.getRole("r1");
            entityType.deletePlays(role);
        }).get();

        //Check the above mutation did not affect central repo
        SchemaConcept foundE1 = tx.getGlobalCache().getCachedTypes().get(e1.getLabel());
        assertTrue("Main cache was affected by transaction", foundE1.asType().plays().anyMatch(role -> role.equals(r1)));
    }

    @Test
    public void whenClosingAGraphWhichWasJustCommitted_DoNothing(){
        tx.commit();
        assertTrue("Graph is still open after commit", tx.isClosed());
        tx.close();
        assertTrue("Graph is somehow open after close", tx.isClosed());
    }

    @Test
    public void whenCommittingAGraphWhichWasJustCommitted_DoNothing(){
        tx.commit();
        assertTrue("Graph is still open after commit", tx.isClosed());
        tx.commit();
        assertTrue("Graph is somehow open after 2nd commit", tx.isClosed());
    }

    @Test
    public void whenAttemptingToMutateReadOnlyGraph_Throw(){
        Keyspace keyspace = Keyspace.of("myreadonlygraph");
        String entityType = "My Entity Type";
        String roleType1 = "My Role Type 1";
        String roleType2 = "My Role Type 2";
        String relationType1 = "My Relationship Type 1";
        String relationType2 = "My Relationship Type 2";
        String resourceType = "My Attribute Type";

        //Fail Some Mutations
        tx = EmbeddedGraknSession.create(keyspace, Grakn.IN_MEMORY).open(GraknTxType.READ);
        failMutation(tx, () -> tx.putEntityType(entityType));
        failMutation(tx, () -> tx.putRole(roleType1));
        failMutation(tx, () -> tx.putRelationshipType(relationType1));

        //Pass some mutations
        tx.close();
        tx = EmbeddedGraknSession.create(keyspace, Grakn.IN_MEMORY).open(GraknTxType.WRITE);
        EntityType entityT = tx.putEntityType(entityType);
        entityT.addEntity();
        Role roleT1 = tx.putRole(roleType1);
        Role roleT2 = tx.putRole(roleType2);
        RelationshipType relationT1 = tx.putRelationshipType(relationType1).relates(roleT1);
        RelationshipType relationT2 = tx.putRelationshipType(relationType2).relates(roleT2);
        AttributeType<String> resourceT = tx.putAttributeType(resourceType, AttributeType.DataType.STRING);
        tx.commit();

        //Fail some mutations again
        tx = EmbeddedGraknSession.create(keyspace, Grakn.IN_MEMORY).open(GraknTxType.READ);
        failMutation(tx, entityT::addEntity);
        failMutation(tx, () -> resourceT.putAttribute("A resource"));
        failMutation(tx, () -> tx.putEntityType(entityType));
        failMutation(tx, () -> entityT.plays(roleT1));
        failMutation(tx, () -> relationT1.relates(roleT2));
        failMutation(tx, () -> relationT2.relates(roleT1));
    }
    private void failMutation(EmbeddedGraknTx<?> graph, Runnable mutator){
        int vertexCount = graph.getTinkerTraversal().V().toList().size();
        int eddgeCount = graph.getTinkerTraversal().E().toList().size();

        Exception caughtException = null;
        try{
            mutator.run();
        } catch (Exception e){
            caughtException = e;
        }

        assertNotNull("No exception thrown when attempting to mutate a read only graph", caughtException);
        assertThat(caughtException, instanceOf(GraknTxOperationException.class));
        assertEquals(caughtException.getMessage(), ErrorMessage.TRANSACTION_READ_ONLY.getMessage(graph.keyspace()));
        assertEquals("A concept was added/removed using a read only graph", vertexCount, graph.getTinkerTraversal().V().toList().size());
        assertEquals("An edge was added/removed using a read only graph", eddgeCount, graph.getTinkerTraversal().E().toList().size());
    }

    @Test
    public void whenOpeningDifferentTypesOfGraphsOnTheSameThread_Throw(){
        String keyspace = "akeyspacewithkeys";
        GraknSession session = Grakn.session(Grakn.IN_MEMORY, keyspace);

        GraknTx graph = session.open(GraknTxType.READ);
        failAtOpeningTx(session, GraknTxType.WRITE, keyspace);
        failAtOpeningTx(session, GraknTxType.BATCH, keyspace);
        graph.close();

        //noinspection ResultOfMethodCallIgnored
        session.open(GraknTxType.BATCH);
        failAtOpeningTx(session, GraknTxType.WRITE, keyspace);
        failAtOpeningTx(session, GraknTxType.READ, keyspace);
    }

    private void failAtOpeningTx(GraknSession session, GraknTxType txType, String keyspace){
        Exception exception = null;
        try{
            //noinspection ResultOfMethodCallIgnored
            session.open(txType);
        } catch (GraknTxOperationException e){
            exception = e;
        }
        assertNotNull(exception);
        assertThat(exception, instanceOf(GraknTxOperationException.class));
        assertEquals(exception.getMessage(), ErrorMessage.TRANSACTION_ALREADY_OPEN.getMessage(keyspace));
    }

    @Test
    public void whenShardingSuperNode_EnsureNewInstancesGoToNewShard(){
        EntityTypeImpl entityType = (EntityTypeImpl) tx.putEntityType("The Special Type");
        Shard s1 = entityType.currentShard();

        //Add 3 instances to first shard
        Entity s1_e1 = entityType.addEntity();
        Entity s1_e2 = entityType.addEntity();
        Entity s1_e3 = entityType.addEntity();
        tx.shard(entityType.getId());

        Shard s2 = entityType.currentShard();

        //Add 5 instances to second shard
        Entity s2_e1 = entityType.addEntity();
        Entity s2_e2 = entityType.addEntity();
        Entity s2_e3 = entityType.addEntity();
        Entity s2_e4 = entityType.addEntity();
        Entity s2_e5 = entityType.addEntity();

        tx.shard(entityType.getId());
        Shard s3 = entityType.currentShard();

        //Add 2 instances to 3rd shard
        Entity s3_e1 = entityType.addEntity();
        Entity s3_e2 = entityType.addEntity();

        //Check Type was sharded correctly
        assertThat(entityType.shards().collect(toSet()), containsInAnyOrder(s1, s2, s3));

        //Check shards have correct instances
        assertThat(s1.links().collect(toSet()), containsInAnyOrder(s1_e1, s1_e2, s1_e3));
        assertThat(s2.links().collect(toSet()), containsInAnyOrder(s2_e1, s2_e2, s2_e3, s2_e4, s2_e5));
        assertThat(s3.links().collect(toSet()), containsInAnyOrder(s3_e1, s3_e2));
    }

    @Test
    public void whenCreatingAValidSchemaInSeparateThreads_EnsureValidationRulesHold() throws ExecutionException, InterruptedException {
        GraknSession session = Grakn.session(Grakn.IN_MEMORY, "hi");

        ExecutorService executor = Executors.newCachedThreadPool();

        executor.submit(() -> {
            //Resources
            try (GraknTx graph = session.open(GraknTxType.WRITE)) {
                AttributeType<Long> int_ = graph.putAttributeType("int", AttributeType.DataType.LONG);
                AttributeType<Long> foo = graph.putAttributeType("foo", AttributeType.DataType.LONG).sup(int_);
                graph.putAttributeType("bar", AttributeType.DataType.LONG).sup(int_);
                graph.putEntityType("FOO").attribute(foo);

                graph.commit();
            }
        }).get();

        //Relationship Which Has Resources
        try (GraknTx graph = session.open(GraknTxType.WRITE)) {
            graph.putEntityType("BAR").attribute(graph.getAttributeType("bar"));
            graph.commit();
        }
    }

    @Test
    public void whenShardingConcepts_EnsureCountsAreUpdated(){
        EntityType entity = tx.putEntityType("my amazing entity type");
        assertEquals(1L, tx.getShardCount(entity));

        tx.shard(entity.getId());
        assertEquals(2L, tx.getShardCount(entity));
    }

    @Test
    public void whenGettingSupsOfASchemaConcept_ResultIncludesMetaThing() {
        EntityType yes = tx.putEntityType("yes");
        EntityType entity = tx.getMetaEntityType();
        Type thing = tx.getMetaConcept();

        assertThat(tx.sups(yes).collect(toSet()), containsInAnyOrder(yes, entity, thing));
        assertThat(tx.sups(entity).collect(toSet()), containsInAnyOrder(entity, thing));
        assertThat(tx.sups(thing).collect(toSet()), containsInAnyOrder(thing));
    }
}

