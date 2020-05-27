/*
 * Copyright (C) 2020 Grakn Labs
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 */

package grakn.core.keyspace;

import grakn.client.GraknClient;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.core.Schema;
import grakn.core.kb.concept.api.Attribute;
import grakn.core.kb.concept.api.AttributeType;
import grakn.core.kb.concept.api.Entity;
import grakn.core.kb.concept.api.EntityType;
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.concept.api.RelationType;
import grakn.core.kb.concept.api.Role;
import grakn.core.kb.keyspace.KeyspaceStatistics;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.test.rule.GraknTestServer;
import grakn.core.test.rule.SessionUtil;
import grakn.core.test.rule.TestTransactionProvider;
import graql.lang.Graql;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class KeyspaceStatisticsIT {

    private GraknClient graknClient;
    private Session localSession;
    private GraknClient.Session remoteSession;

    @ClassRule
    public static final GraknTestServer server = new GraknTestServer();

    @Before
    public void setUp() {
        graknClient = new GraknClient(server.grpcUri());
        localSession = SessionUtil.serverlessSessionWithNewKeyspace(server.serverConfig());
        remoteSession = graknClient.session(localSession.keyspace().toString());
    }

    @After
    public void closeSession() {
        remoteSession.close();
        localSession.close();
        graknClient.close();
    }

    @Test
    public void newKeyspaceHasZeroCounts() {
        KeyspaceStatistics statistics = localSession.keyspaceStatistics();
        TestTransactionProvider.TestTransaction tx = (TestTransactionProvider.TestTransaction)localSession.transaction(Transaction.Type.WRITE);
        long entityCount = statistics.count(tx.conceptManager(), Label.of("entity"));
        long relationCount = statistics.count(tx.conceptManager(), Label.of("relation"));
        long attributeCount = statistics.count(tx.conceptManager(), Label.of("attribute"));
        tx.close();

        assertEquals(0, entityCount);
        assertEquals(0, relationCount);
        assertEquals(0, attributeCount);
    }

    @Test
    public void sessionsToSameKeyspaceShareStatistics() {
        Session session2  = server.session(localSession.keyspace());
        Session session3 = server.session(localSession.keyspace());
        assertSame(session2.keyspaceStatistics(), session3.keyspaceStatistics());
    }

    @Test
    public void keyspaceStatisticsUpdatedOnCommit() {
        Transaction tx = localSession.transaction(Transaction.Type.WRITE);
        AttributeType ageType = tx.putAttributeType("age", AttributeType.ValueType.LONG);
        Role friend = tx.putRole("friend");
        EntityType personType = tx.putEntityType("person").plays(friend).putHas(ageType);
        RelationType friendshipType = tx.putRelationType("friendship").relates(friend);
        tx.commit();

        tx = localSession.transaction(Transaction.Type.WRITE);
        ageType = tx.getAttributeType("age");
        Attribute age = ageType.create(1);
        personType = tx.getEntityType("person");
        Entity person1 = personType.create().has(age).has(age);
        Entity person2 = personType.create().has(age);
        friendshipType = tx.getRelationType("friendship");
        friend = tx.getRole("friend");
        friendshipType.create().assign(friend, person1).assign(friend, person2);
        tx.commit();

        TestTransactionProvider.TestTransaction testTx = (TestTransactionProvider.TestTransaction)localSession.transaction(Transaction.Type.WRITE);
        long personCount = localSession.keyspaceStatistics().count(testTx.conceptManager(), Label.of("person"));
        long ageCount = localSession.keyspaceStatistics().count(testTx.conceptManager(), Label.of("age"));
        long friendshipCount = localSession.keyspaceStatistics().count(testTx.conceptManager(), Label.of("friendship"));
        long ageOwnershipCount = localSession.keyspaceStatistics().countOwnerships(testTx.conceptManager(), Label.of("age"));
        long thingCount = localSession.keyspaceStatistics().count(testTx.conceptManager(), Label.of("thing"));
        long entityCount = localSession.keyspaceStatistics().count(testTx.conceptManager(), Label.of("entity"));
        long relationCount = localSession.keyspaceStatistics().count(testTx.conceptManager(), Label.of("relation"));
        long attributeCount = localSession.keyspaceStatistics().count(testTx.conceptManager(), Label.of("attribute"));
        testTx.close();

        assertEquals(2, personCount);
        assertEquals(1, ageCount);
        assertEquals(1, friendshipCount);
        assertEquals(3, ageOwnershipCount);
        assertEquals(2, entityCount);
        assertEquals(1, relationCount);
        assertEquals(1, attributeCount);
        assertEquals(personCount + ageCount + friendshipCount, thingCount);

        tx = localSession.transaction(Transaction.Type.WRITE);
        tx.execute(Graql.parse("match $x isa friendship; delete $x isa friendship;").asDelete());
        tx.commit();

        testTx = (TestTransactionProvider.TestTransaction)localSession.transaction(Transaction.Type.WRITE);
        personCount = localSession.keyspaceStatistics().count(testTx.conceptManager(), Label.of("person"));
        ageCount = localSession.keyspaceStatistics().count(testTx.conceptManager(), Label.of("age"));
        friendshipCount = localSession.keyspaceStatistics().count(testTx.conceptManager(), Label.of("friendship"));
        ageOwnershipCount = localSession.keyspaceStatistics().countOwnerships(testTx.conceptManager(), Label.of("age"));
        thingCount = localSession.keyspaceStatistics().count(testTx.conceptManager(), Label.of("thing"));
        entityCount = localSession.keyspaceStatistics().count(testTx.conceptManager(), Label.of("entity"));
        relationCount = localSession.keyspaceStatistics().count(testTx.conceptManager(), Label.of("relation"));
        attributeCount = localSession.keyspaceStatistics().count(testTx.conceptManager(), Label.of("attribute"));
        testTx.close();

        assertEquals(2, personCount);
        assertEquals(1, ageCount);
        assertEquals(0, friendshipCount);
        assertEquals(3, ageOwnershipCount);
        assertEquals(2, entityCount);
        assertEquals(0, relationCount);
        assertEquals(1, attributeCount);
        assertEquals(personCount + ageCount + friendshipCount, thingCount);

        tx = localSession.transaction(Transaction.Type.WRITE);
        tx.execute(Graql.parse("match $x isa thing; delete $x isa thing;").asDelete());
        tx.commit();
        testTx = (TestTransactionProvider.TestTransaction)localSession.transaction(Transaction.Type.WRITE);
        personCount = localSession.keyspaceStatistics().count(testTx.conceptManager(), Label.of("person"));
        ageCount = localSession.keyspaceStatistics().count(testTx.conceptManager(), Label.of("age"));
        friendshipCount = localSession.keyspaceStatistics().count(testTx.conceptManager(), Label.of("friendship"));
        ageOwnershipCount = localSession.keyspaceStatistics().countOwnerships(testTx.conceptManager(), Label.of("age"));
        thingCount = localSession.keyspaceStatistics().count(testTx.conceptManager(), Label.of("thing"));
        entityCount = localSession.keyspaceStatistics().count(testTx.conceptManager(), Label.of("entity"));
        relationCount = localSession.keyspaceStatistics().count(testTx.conceptManager(), Label.of("relation"));
        attributeCount = localSession.keyspaceStatistics().count(testTx.conceptManager(), Label.of("attribute"));
        tx.close();

        assertEquals(0, personCount);
        assertEquals(0, ageCount);
        assertEquals(0, friendshipCount);
        assertEquals(0, ageOwnershipCount);
        assertEquals(0, entityCount);
        assertEquals(0, relationCount);
        assertEquals(0, attributeCount);
        assertEquals(0, thingCount);
    }

    @Test
    public void keyspaceStatisticsNotUpdatedIfNotCommitted() {
        Transaction tx = localSession.transaction(Transaction.Type.WRITE);
        AttributeType ageType = tx.putAttributeType("age", AttributeType.ValueType.LONG);
        Role friend = tx.putRole("friend");
        EntityType personType = tx.putEntityType("person").plays(friend).putHas(ageType);
        RelationType friendshipType = tx.putRelationType("friendship").relates(friend);
        tx.commit();

        tx = localSession.transaction(Transaction.Type.WRITE);
        ageType = tx.getAttributeType("age");
        Attribute age = ageType.create(1);
        personType = tx.getEntityType("person");
        Entity person1 = personType.create().has(age).has(age);
        Entity person2 = personType.create().has(age);
        friendshipType = tx.getRelationType("friendship");
        friend = tx.getRole("friend");
        friendshipType.create().assign(friend, person1).assign(friend, person2);
        tx.close();

        TestTransactionProvider.TestTransaction testTx = (TestTransactionProvider.TestTransaction)localSession.transaction(Transaction.Type.WRITE);
        long personCount = localSession.keyspaceStatistics().count(testTx.conceptManager(), Label.of("person"));
        long ageCount = localSession.keyspaceStatistics().count(testTx.conceptManager(), Label.of("age"));
        long friendshipCount = localSession.keyspaceStatistics().count(testTx.conceptManager(), Label.of("friendship"));
        long ageOwnershipCount = localSession.keyspaceStatistics().countOwnerships(testTx.conceptManager(), Label.of("age"));
        long entityCount = localSession.keyspaceStatistics().count(testTx.conceptManager(), Label.of("entity"));
        long relationCount = localSession.keyspaceStatistics().count(testTx.conceptManager(), Label.of("relation"));
        long attributeCount = localSession.keyspaceStatistics().count(testTx.conceptManager(), Label.of("attribute"));
        testTx.close();

        assertEquals(0, personCount);
        assertEquals(0, ageCount);
        assertEquals(0, friendshipCount);
        assertEquals(0, entityCount);
        assertEquals(0, relationCount);
        assertEquals(0, attributeCount);
        assertEquals(0, ageOwnershipCount);
    }

    /**
     * Tests persistence
     */
    @Test
    public void reopeningSessionRetrievesStatistics() {
        Transaction tx = localSession.transaction(Transaction.Type.WRITE);
        AttributeType ageType = tx.putAttributeType("age", AttributeType.ValueType.LONG);
        Role friend = tx.putRole("friend");
        EntityType personType = tx.putEntityType("person").plays(friend).putHas(ageType);
        RelationType friendshipType = tx.putRelationType("friendship").relates(friend);

        ageType = tx.getAttributeType("age");
        Attribute age = ageType.create(1);
        personType = tx.getEntityType("person");
        Entity person1 = personType.create().has(age).has(age);
        Entity person2 = personType.create().has(age);
        friendshipType = tx.getRelationType("friendship");
        friend = tx.getRole("friend");
        friendshipType.create().assign(friend, person1).assign(friend, person2);
        tx.commit();

        TestTransactionProvider.TestTransaction testTx = (TestTransactionProvider.TestTransaction)localSession.transaction(Transaction.Type.WRITE);
        long personCount = localSession.keyspaceStatistics().count(testTx.conceptManager(), Label.of("person"));
        long ageCount = localSession.keyspaceStatistics().count(testTx.conceptManager(), Label.of("age"));
        long friendshipCount = localSession.keyspaceStatistics().count(testTx.conceptManager(), Label.of("friendship"));
        long ageOwnershipCount = localSession.keyspaceStatistics().countOwnerships(testTx.conceptManager(), Label.of("age"));
        long thingCount = localSession.keyspaceStatistics().count(testTx.conceptManager(), Label.of("thing"));
        long entityCount = localSession.keyspaceStatistics().count(testTx.conceptManager(), Label.of("entity"));
        long relationCount = localSession.keyspaceStatistics().count(testTx.conceptManager(), Label.of("relation"));
        long attributeCount = localSession.keyspaceStatistics().count(testTx.conceptManager(), Label.of("attribute"));
        testTx.close();

        localSession.close();
        remoteSession.close();

        // at this point, the graph and keyspace should be deleted from Grakn server cache
        localSession = SessionUtil.serverlessSession(server.serverConfig(), remoteSession.keyspace().name());

        testTx = (TestTransactionProvider.TestTransaction)localSession.transaction(Transaction.Type.WRITE);
        long personCountReopened = localSession.keyspaceStatistics().count(testTx.conceptManager(), Label.of("person"));
        long ageCountReopened = localSession.keyspaceStatistics().count(testTx.conceptManager(), Label.of("age"));
        long friendshipCountReopened = localSession.keyspaceStatistics().count(testTx.conceptManager(), Label.of("friendship"));
        long ageOwnershipCountReopened = localSession.keyspaceStatistics().countOwnerships(testTx.conceptManager(), Label.of("age"));
        long thingCountReopened = localSession.keyspaceStatistics().count(testTx.conceptManager(), Label.of("thing"));
        long entityCountReopened = localSession.keyspaceStatistics().count(testTx.conceptManager(), Label.of("entity"));
        long relationCountReopened = localSession.keyspaceStatistics().count(testTx.conceptManager(), Label.of("relation"));
        long attributeCountReopened = localSession.keyspaceStatistics().count(testTx.conceptManager(), Label.of("attribute"));
        testTx.close();

        assertEquals(personCount, personCountReopened);
        assertEquals(ageCount, ageCountReopened);
        assertEquals(friendshipCount, friendshipCountReopened);
        assertEquals(ageOwnershipCount, ageOwnershipCountReopened);
        assertEquals(thingCount, thingCountReopened);
        assertEquals(entityCount, entityCountReopened);
        assertEquals(relationCount, relationCountReopened);
        assertEquals(attributeCount, attributeCountReopened);
    }

    @Test
    public void nonexistentLabelStatisticsReturnMinusOne() {
        TestTransactionProvider.TestTransaction testTx = (TestTransactionProvider.TestTransaction)localSession.transaction(Transaction.Type.WRITE);
        long personCount = localSession.keyspaceStatistics().count(testTx.conceptManager(), Label.of("person"));
        testTx.close();
        assertEquals(-1L, personCount);
    }


    @Test
    public void concurrentTransactionsUpdateStatisticsCorrectly() throws InterruptedException, ExecutionException {
        TestTransactionProvider.TestTransaction testTx = (TestTransactionProvider.TestTransaction)localSession.transaction(Transaction.Type.WRITE);
        AttributeType ageType = testTx.putAttributeType("age", AttributeType.ValueType.LONG);
        Role friend = testTx.putRole("friend");
        EntityType personType = testTx.putEntityType("person").plays(friend).putHas(ageType);
        RelationType friendshipType = testTx.putRelationType("friendship").relates(friend);

        long personCountStart = localSession.keyspaceStatistics().count(testTx.conceptManager(), Label.of("person"));
        long ageCountStart = localSession.keyspaceStatistics().count(testTx.conceptManager(), Label.of("age"));
        testTx.commit();

        // TODO fix the schema label cache to be a read-through, meanwhile close and reopen session
        remoteSession.close();
        remoteSession = graknClient.session(localSession.keyspace().toString());

        ExecutorService parallelExecutor = Executors.newFixedThreadPool(2);

        CompletableFuture<Void> future1 = CompletableFuture.supplyAsync(() -> {
            GraknClient.Transaction tx1 = remoteSession.transaction().write();
            /*

            To debug this: set a breakpoint in `getSchemaConcept` in the server
            Somehow the age isn't being looked up properly because the label is not in the schema label cache?


             */
            grakn.client.concept.type.AttributeType.Remote<Integer> ageT = tx1.<Integer>getAttributeType("age").asRemote(tx1);
            grakn.client.concept.type.EntityType.Remote personT = tx1.getEntityType("person").asRemote(tx1);
            ageT.create(2);
            ageT.create(3);
            personT.create();
            tx1.commit();
            return null;
        }, parallelExecutor);

        CompletableFuture<Void> future2 = CompletableFuture.supplyAsync(() -> {
            GraknClient.Transaction tx2 = remoteSession.transaction().write();
            grakn.client.concept.type.AttributeType.Remote<Integer> ageT = tx2.<Integer>getAttributeType("age").asRemote(tx2);
            grakn.client.concept.type.EntityType.Remote personT = tx2.getEntityType("person").asRemote(tx2);
            ageT.create(3); // tricky case - this will be merge
            ageT.create(4);
            personT.create();
            tx2.commit();
            return null;
        }, parallelExecutor);

        future1.get();
        future2.get();
        parallelExecutor.shutdownNow();
        parallelExecutor.awaitTermination(5, TimeUnit.SECONDS);

        // because we haven't solved statistics beyond a single node, refresh the session here to clear cached counts
        localSession = SessionUtil.serverlessSession(server.serverConfig(), localSession.keyspace().name());
        testTx = (TestTransactionProvider.TestTransaction)localSession.transaction(Transaction.Type.WRITE);
        long personCount = localSession.keyspaceStatistics().count(testTx.conceptManager(), Label.of("person"));
        long ageCount = localSession.keyspaceStatistics().count(testTx.conceptManager(), Label.of("age"));
        long thingCount = localSession.keyspaceStatistics().count(testTx.conceptManager(), Label.of("thing"));
        long entityCount = localSession.keyspaceStatistics().count(testTx.conceptManager(), Label.of("entity"));
        long relationCount = localSession.keyspaceStatistics().count(testTx.conceptManager(), Label.of("relation"));
        long attributeCount = localSession.keyspaceStatistics().count(testTx.conceptManager(), Label.of("attribute"));

        testTx.close();

        assertEquals(2L, personCount - personCountStart);
        assertEquals(3L, ageCount - ageCountStart);
        assertEquals(personCount, entityCount);
        assertEquals(ageCount, attributeCount);
        assertEquals(personCount + ageCount, thingCount);
    }

    @Test
    public void attachingAttributesViaRulesDoesntAlterConceptCountsAfterCommit() {
        // test concept API insertion
        Transaction tx = localSession.transaction(Transaction.Type.WRITE);
        AttributeType<Long> age = tx.putAttributeType("age", AttributeType.ValueType.LONG);
        EntityType person = tx.putEntityType("person").putHas(age);

        person.create();
        person.create();
        tx.putRule(
                "someRule",
                Graql.parsePattern("{$x isa person;};"),
                Graql.parsePattern("{$x has age 27;};"));

        tx.commit();

        tx = localSession.transaction(Transaction.Type.WRITE);

        List<ConceptMap> answers = tx.execute(Graql.parse("match $x has age $r;get;").asGet());

        tx.commit();

        TestTransactionProvider.TestTransaction testTx = (TestTransactionProvider.TestTransaction)localSession.transaction(Transaction.Type.WRITE);

        Label ageLabel = Label.of("age");
        assertEquals(0, localSession.keyspaceStatistics().count(testTx.conceptManager(), ageLabel));
        assertEquals(0, localSession.keyspaceStatistics().countOwnerships(testTx.conceptManager(), ageLabel));

        tx.close();
    }
}
