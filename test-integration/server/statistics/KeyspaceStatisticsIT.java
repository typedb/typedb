/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2019 Grakn Labs Ltd
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
 */

package grakn.core.server.statistics;

import grakn.client.GraknClient;
import grakn.core.concept.Label;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concept.thing.Attribute;
import grakn.core.concept.thing.Entity;
import grakn.core.concept.type.AttributeType;
import grakn.core.concept.type.EntityType;
import grakn.core.concept.type.RelationType;
import grakn.core.concept.type.Role;
import grakn.core.rule.GraknTestServer;
import grakn.core.server.kb.Schema;
import grakn.core.server.keyspace.KeyspaceImpl;
import grakn.core.server.session.SessionImpl;
import grakn.core.server.session.TransactionOLTP;
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
    private SessionImpl localSession;
    private GraknClient.Session remoteSession;

    @ClassRule
    public static final GraknTestServer server = new GraknTestServer();

    @Before
    public void setUp() {
        graknClient = new GraknClient(server.grpcUri());
        localSession = server.sessionWithNewKeyspace();
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
        TransactionOLTP tx = localSession.transaction().write();
        long entityCount = statistics.count(tx, Label.of("entity"));
        long relationCount = statistics.count(tx, Label.of("relation"));
        long attributeCount = statistics.count(tx, Label.of("attribute"));
        tx.close();

        assertEquals(0, entityCount);
        assertEquals(0, relationCount);
        assertEquals(0, attributeCount);
    }

    @Test
    public void sessionsToSameKeyspaceShareStatistics() {
        SessionImpl session2 = server.session(localSession.keyspace());
        assertSame(localSession.keyspaceStatistics(), session2.keyspaceStatistics());
    }

    @Test
    public void keyspaceStatisticsUpdatedOnCommit() {
        TransactionOLTP tx = localSession.transaction().write();
        AttributeType ageType = tx.putAttributeType("age", AttributeType.DataType.LONG);
        Role friend = tx.putRole("friend");
        EntityType personType = tx.putEntityType("person").plays(friend).has(ageType);
        RelationType friendshipType = tx.putRelationType("friendship").relates(friend);
        tx.commit();

        tx = localSession.transaction().write();
        ageType = tx.getAttributeType("age");
        Attribute age = ageType.create(1);
        personType = tx.getEntityType("person");
        Entity person1 = personType.create().has(age).has(age);
        Entity person2 = personType.create().has(age);
        friendshipType = tx.getRelationType("friendship");
        friend = tx.getRole("friend");
        friendshipType.create().assign(friend, person1).assign(friend, person2);
        tx.commit();

        tx = localSession.transaction().write();
        long personCount = localSession.keyspaceStatistics().count(tx, Label.of("person"));
        long ageCount = localSession.keyspaceStatistics().count(tx, Label.of("age"));
        long friendshipCount = localSession.keyspaceStatistics().count(tx, Label.of("friendship"));
        long implicitAgeCount = localSession.keyspaceStatistics().count(tx, Label.of("@has-age"));
        long thingCount = localSession.keyspaceStatistics().count(tx, Label.of("thing"));
        tx.close();

        assertEquals(2, personCount);
        assertEquals(1, ageCount);
        assertEquals(1, friendshipCount);
        assertEquals(3, implicitAgeCount);
        assertEquals(personCount + ageCount + friendshipCount + implicitAgeCount, thingCount);

        tx = localSession.transaction().write();
        tx.execute(Graql.parse("match $x isa friendship; delete $x;").asDelete());
        tx.commit();

        tx = localSession.transaction().write();
        personCount = localSession.keyspaceStatistics().count(tx, Label.of("person"));
        ageCount = localSession.keyspaceStatistics().count(tx, Label.of("age"));
        friendshipCount = localSession.keyspaceStatistics().count(tx, Label.of("friendship"));
        implicitAgeCount = localSession.keyspaceStatistics().count(tx, Label.of("@has-age"));
        thingCount = localSession.keyspaceStatistics().count(tx, Label.of("thing"));
        tx.close();

        assertEquals(2, personCount);
        assertEquals(1, ageCount);
        assertEquals(0, friendshipCount);
        assertEquals(3, implicitAgeCount);
        assertEquals(personCount + ageCount + friendshipCount + implicitAgeCount, thingCount);

        tx = localSession.transaction().write();
        tx.execute(Graql.parse("match $x isa thing; delete $x;").asDelete());
        tx.commit();
        tx = localSession.transaction().write();
        personCount = localSession.keyspaceStatistics().count(tx, Label.of("person"));
        ageCount = localSession.keyspaceStatistics().count(tx, Label.of("age"));
        friendshipCount = localSession.keyspaceStatistics().count(tx, Label.of("friendship"));
        implicitAgeCount = localSession.keyspaceStatistics().count(tx, Label.of("@has-age"));
        thingCount = localSession.keyspaceStatistics().count(tx, Label.of("thing"));
        tx.close();

        assertEquals(0, personCount);
        assertEquals(0, ageCount);
        assertEquals(0, friendshipCount);
        assertEquals(0, implicitAgeCount);
        assertEquals(0, thingCount);
    }

    @Test
    public void keyspaceStatisticsNotUpdatedIfNotcommitted() {
        TransactionOLTP tx = localSession.transaction().write();
        AttributeType ageType = tx.putAttributeType("age", AttributeType.DataType.LONG);
        Role friend = tx.putRole("friend");
        EntityType personType = tx.putEntityType("person").plays(friend).has(ageType);
        RelationType friendshipType = tx.putRelationType("friendship").relates(friend);
        tx.commit();

        tx = localSession.transaction().write();
        ageType = tx.getAttributeType("age");
        Attribute age = ageType.create(1);
        personType = tx.getEntityType("person");
        Entity person1 = personType.create().has(age).has(age);
        Entity person2 = personType.create().has(age);
        friendshipType = tx.getRelationType("friendship");
        friend = tx.getRole("friend");
        friendshipType.create().assign(friend, person1).assign(friend, person2);
        tx.close();

        tx = localSession.transaction().write();
        long personCount = localSession.keyspaceStatistics().count(tx, Label.of("person"));
        long ageCount = localSession.keyspaceStatistics().count(tx, Label.of("age"));
        long friendshipCount = localSession.keyspaceStatistics().count(tx, Label.of("friendship"));
        long implicitAgeCount = localSession.keyspaceStatistics().count(tx, Label.of("@has-age"));
        tx.close();

        assertEquals(0, personCount);
        assertEquals(0, ageCount);
        assertEquals(0, friendshipCount);
        assertEquals(0, implicitAgeCount);
    }

    /**
     * Tests persistence
     */
    @Test
    public void reopeningSessionRetrievesStatistics() {
        TransactionOLTP tx = localSession.transaction().write();
        AttributeType ageType = tx.putAttributeType("age", AttributeType.DataType.LONG);
        Role friend = tx.putRole("friend");
        EntityType personType = tx.putEntityType("person").plays(friend).has(ageType);
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

        tx = localSession.transaction().write();
        long personCount = localSession.keyspaceStatistics().count(tx, Label.of("person"));
        long ageCount = localSession.keyspaceStatistics().count(tx, Label.of("age"));
        long friendshipCount = localSession.keyspaceStatistics().count(tx, Label.of("friendship"));
        long implicitAgeCount = localSession.keyspaceStatistics().count(tx, Label.of("@has-age"));
        long thingCount = localSession.keyspaceStatistics().count(tx, Label.of("thing"));
        tx.close();

        localSession.close();
        remoteSession.close();

        // at this point, the graph and keyspace should be deleted from Grakn server cache
        localSession = server.session(KeyspaceImpl.of(remoteSession.keyspace().name()));

        tx = localSession.transaction().write();
        long personCountReopened = localSession.keyspaceStatistics().count(tx, Label.of("person"));
        long ageCountReopened = localSession.keyspaceStatistics().count(tx, Label.of("age"));
        long friendshipCountReopened = localSession.keyspaceStatistics().count(tx, Label.of("friendship"));
        long implicitAgeCountReopened = localSession.keyspaceStatistics().count(tx, Label.of("@has-age"));
        long thingCountReopened = localSession.keyspaceStatistics().count(tx, Label.of("thing"));
        tx.close();

        assertEquals(personCount, personCountReopened);
        assertEquals(ageCount, ageCountReopened);
        assertEquals(friendshipCount, friendshipCountReopened);
        assertEquals(implicitAgeCount, implicitAgeCountReopened);
        assertEquals(thingCount, thingCountReopened);
    }

    @Test
    public void nonexistentLabelStatisticsReturnMinusOne() {
        TransactionOLTP tx = localSession.transaction().write();
        long personCount = localSession.keyspaceStatistics().count(tx, Label.of("person"));
        tx.close();
        assertEquals(-1L, personCount);
    }


    @Test
    public void concurrentTransactionsUpdateStatisticsCorrectly() throws InterruptedException, ExecutionException {
        TransactionOLTP tx = localSession.transaction().write();
        AttributeType ageType = tx.putAttributeType("age", AttributeType.DataType.LONG);
        Role friend = tx.putRole("friend");
        EntityType personType = tx.putEntityType("person").plays(friend).has(ageType);
        RelationType friendshipType = tx.putRelationType("friendship").relates(friend);

        long personCountStart = localSession.keyspaceStatistics().count(tx, Label.of("person"));
        long ageCountStart = localSession.keyspaceStatistics().count(tx, Label.of("age"));
        tx.commit();

        ExecutorService parallelExecutor = Executors.newFixedThreadPool(2);

        CompletableFuture<Void> future1 = CompletableFuture.supplyAsync(() -> {
            GraknClient.Transaction tx1 = remoteSession.transaction().write();
            AttributeType ageT = tx1.getAttributeType("age");
            EntityType personT = tx1.getEntityType("person");
            ageT.create(2);
            ageT.create(3);
            personT.create();
            tx1.commit();
            return null;
        }, parallelExecutor);

        CompletableFuture<Void> future2 = CompletableFuture.supplyAsync(() -> {
            GraknClient.Transaction tx2 = remoteSession.transaction().write();
            AttributeType ageT = tx2.getAttributeType("age");
            EntityType personT = tx2.getEntityType("person");
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

        tx = localSession.transaction().write();
        long personCount = localSession.keyspaceStatistics().count(tx, Label.of("person"));
        long ageCount = localSession.keyspaceStatistics().count(tx, Label.of("age"));
        long thingCount = localSession.keyspaceStatistics().count(tx, Label.of("thing"));
        tx.close();

        assertEquals(2L, personCount - personCountStart);
        assertEquals(3L, ageCount - ageCountStart);
        assertEquals(personCount + ageCount, thingCount);
    }

    @Test
    public void attachingAttributesViaRulesDoesntAlterConceptCountsAfterCommit() {
        // test concept API insertion
        TransactionOLTP tx = localSession.transaction().write();
        AttributeType<Long> age = tx.putAttributeType("age", AttributeType.DataType.LONG);
        EntityType person = tx.putEntityType("person").has(age);

        person.create();
        person.create();
        tx.putRule(
                "someRule",
                Graql.parsePattern("{$x isa person;};"),
                Graql.parsePattern("{$x has age 27;};"));

        tx.commit();

        tx = localSession.transaction().write();

        List<ConceptMap> answers = tx.execute(Graql.parse("match $x has age $r;get;").asGet());

        tx.commit();

        tx = localSession.transaction().write();

        Label ageLabel = Label.of("age");
        assertEquals(0, localSession.keyspaceStatistics().count(tx, ageLabel));
        assertEquals(0, localSession.keyspaceStatistics().count(tx, Schema.ImplicitType.HAS.getLabel(ageLabel)));

        tx.close();
    }
}
