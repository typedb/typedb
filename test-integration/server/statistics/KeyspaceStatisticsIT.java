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
import grakn.core.concept.answer.ConceptMap;
import grakn.core.core.Schema;
import grakn.core.kb.concept.api.Attribute;
import grakn.core.kb.concept.api.AttributeType;
import grakn.core.kb.concept.api.Entity;
import grakn.core.kb.concept.api.EntityType;
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.concept.api.RelationType;
import grakn.core.kb.concept.api.Role;
import grakn.core.kb.server.statistics.KeyspaceStatistics;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.rule.GraknTestServer;
import grakn.core.server.keyspace.KeyspaceImpl;
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
        Transaction tx = localSession.writeTransaction();
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
        Session session2 = server.session(localSession.keyspace());
        assertSame(localSession.keyspaceStatistics(), session2.keyspaceStatistics());
    }

    @Test
    public void keyspaceStatisticsUpdatedOnCommit() {
        Transaction tx = localSession.writeTransaction();
        AttributeType ageType = tx.putAttributeType("age", AttributeType.DataType.LONG);
        Role friend = tx.putRole("friend");
        EntityType personType = tx.putEntityType("person").plays(friend).has(ageType);
        RelationType friendshipType = tx.putRelationType("friendship").relates(friend);
        tx.commit();

        tx = localSession.writeTransaction();
        ageType = tx.getAttributeType("age");
        Attribute age = ageType.create(1);
        personType = tx.getEntityType("person");
        Entity person1 = personType.create().has(age).has(age);
        Entity person2 = personType.create().has(age);
        friendshipType = tx.getRelationType("friendship");
        friend = tx.getRole("friend");
        friendshipType.create().assign(friend, person1).assign(friend, person2);
        tx.commit();

        tx = localSession.writeTransaction();
        long personCount = localSession.keyspaceStatistics().count(tx, Label.of("person"));
        long ageCount = localSession.keyspaceStatistics().count(tx, Label.of("age"));
        long friendshipCount = localSession.keyspaceStatistics().count(tx, Label.of("friendship"));
        long implicitAgeCount = localSession.keyspaceStatistics().count(tx, Label.of("@has-age"));
        long thingCount = localSession.keyspaceStatistics().count(tx, Label.of("thing"));
        long entityCount = localSession.keyspaceStatistics().count(tx, Label.of("entity"));
        long relationCount = localSession.keyspaceStatistics().count(tx, Label.of("relation"));
        long attributeCount = localSession.keyspaceStatistics().count(tx, Label.of("attribute"));
        tx.close();

        assertEquals(2, personCount);
        assertEquals(1, ageCount);
        assertEquals(1, friendshipCount);
        assertEquals(3, implicitAgeCount);
        assertEquals(2, entityCount);
        assertEquals(4, relationCount);
        assertEquals(1, attributeCount);
        assertEquals(personCount + ageCount + friendshipCount + implicitAgeCount, thingCount);

        tx = localSession.writeTransaction();
        tx.execute(Graql.parse("match $x isa friendship; delete $x;").asDelete());
        tx.commit();

        tx = localSession.writeTransaction();
        personCount = localSession.keyspaceStatistics().count(tx, Label.of("person"));
        ageCount = localSession.keyspaceStatistics().count(tx, Label.of("age"));
        friendshipCount = localSession.keyspaceStatistics().count(tx, Label.of("friendship"));
        implicitAgeCount = localSession.keyspaceStatistics().count(tx, Label.of("@has-age"));
        thingCount = localSession.keyspaceStatistics().count(tx, Label.of("thing"));
        entityCount = localSession.keyspaceStatistics().count(tx, Label.of("entity"));
        relationCount = localSession.keyspaceStatistics().count(tx, Label.of("relation"));
        attributeCount = localSession.keyspaceStatistics().count(tx, Label.of("attribute"));
        tx.close();

        assertEquals(2, personCount);
        assertEquals(1, ageCount);
        assertEquals(0, friendshipCount);
        assertEquals(3, implicitAgeCount);
        assertEquals(2, entityCount);
        assertEquals(3, relationCount);
        assertEquals(1, attributeCount);
        assertEquals(personCount + ageCount + friendshipCount + implicitAgeCount, thingCount);

        tx = localSession.writeTransaction();
        tx.execute(Graql.parse("match $x isa thing; delete $x;").asDelete());
        tx.commit();
        tx = localSession.writeTransaction();
        personCount = localSession.keyspaceStatistics().count(tx, Label.of("person"));
        ageCount = localSession.keyspaceStatistics().count(tx, Label.of("age"));
        friendshipCount = localSession.keyspaceStatistics().count(tx, Label.of("friendship"));
        implicitAgeCount = localSession.keyspaceStatistics().count(tx, Label.of("@has-age"));
        thingCount = localSession.keyspaceStatistics().count(tx, Label.of("thing"));
        entityCount = localSession.keyspaceStatistics().count(tx, Label.of("entity"));
        relationCount = localSession.keyspaceStatistics().count(tx, Label.of("relation"));
        attributeCount = localSession.keyspaceStatistics().count(tx, Label.of("attribute"));
        tx.close();

        assertEquals(0, personCount);
        assertEquals(0, ageCount);
        assertEquals(0, friendshipCount);
        assertEquals(0, implicitAgeCount);
        assertEquals(0, entityCount);
        assertEquals(0, relationCount);
        assertEquals(0, attributeCount);
        assertEquals(0, thingCount);
    }

    @Test
    public void keyspaceStatisticsNotUpdatedIfNotcommitted() {
        Transaction tx = localSession.writeTransaction();
        AttributeType ageType = tx.putAttributeType("age", AttributeType.DataType.LONG);
        Role friend = tx.putRole("friend");
        EntityType personType = tx.putEntityType("person").plays(friend).has(ageType);
        RelationType friendshipType = tx.putRelationType("friendship").relates(friend);
        tx.commit();

        tx = localSession.writeTransaction();
        ageType = tx.getAttributeType("age");
        Attribute age = ageType.create(1);
        personType = tx.getEntityType("person");
        Entity person1 = personType.create().has(age).has(age);
        Entity person2 = personType.create().has(age);
        friendshipType = tx.getRelationType("friendship");
        friend = tx.getRole("friend");
        friendshipType.create().assign(friend, person1).assign(friend, person2);
        tx.close();

        tx = localSession.writeTransaction();
        long personCount = localSession.keyspaceStatistics().count(tx, Label.of("person"));
        long ageCount = localSession.keyspaceStatistics().count(tx, Label.of("age"));
        long friendshipCount = localSession.keyspaceStatistics().count(tx, Label.of("friendship"));
        long implicitAgeCount = localSession.keyspaceStatistics().count(tx, Label.of("@has-age"));
        long entityCount = localSession.keyspaceStatistics().count(tx, Label.of("entity"));
        long relationCount = localSession.keyspaceStatistics().count(tx, Label.of("relation"));
        long attributeCount = localSession.keyspaceStatistics().count(tx, Label.of("attribute"));
        tx.close();

        assertEquals(0, personCount);
        assertEquals(0, ageCount);
        assertEquals(0, friendshipCount);
        assertEquals(0, entityCount);
        assertEquals(0, relationCount);
        assertEquals(0, attributeCount);
        assertEquals(0, implicitAgeCount);
    }

    /**
     * Tests persistence
     */
    @Test
    public void reopeningSessionRetrievesStatistics() {
        Transaction tx = localSession.writeTransaction();
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

        tx = localSession.writeTransaction();
        long personCount = localSession.keyspaceStatistics().count(tx, Label.of("person"));
        long ageCount = localSession.keyspaceStatistics().count(tx, Label.of("age"));
        long friendshipCount = localSession.keyspaceStatistics().count(tx, Label.of("friendship"));
        long implicitAgeCount = localSession.keyspaceStatistics().count(tx, Label.of("@has-age"));
        long thingCount = localSession.keyspaceStatistics().count(tx, Label.of("thing"));
        long entityCount = localSession.keyspaceStatistics().count(tx, Label.of("entity"));
        long relationCount = localSession.keyspaceStatistics().count(tx, Label.of("relation"));
        long attributeCount = localSession.keyspaceStatistics().count(tx, Label.of("attribute"));
        tx.close();

        localSession.close();
        remoteSession.close();

        // at this point, the graph and keyspace should be deleted from Grakn server cache
        localSession = server.session(new KeyspaceImpl(remoteSession.keyspace().name()));

        tx = localSession.writeTransaction();
        long personCountReopened = localSession.keyspaceStatistics().count(tx, Label.of("person"));
        long ageCountReopened = localSession.keyspaceStatistics().count(tx, Label.of("age"));
        long friendshipCountReopened = localSession.keyspaceStatistics().count(tx, Label.of("friendship"));
        long implicitAgeCountReopened = localSession.keyspaceStatistics().count(tx, Label.of("@has-age"));
        long thingCountReopened = localSession.keyspaceStatistics().count(tx, Label.of("thing"));
        long entityCountReopened = localSession.keyspaceStatistics().count(tx, Label.of("entity"));
        long relationCountReopened = localSession.keyspaceStatistics().count(tx, Label.of("relation"));
        long attributeCountReopened = localSession.keyspaceStatistics().count(tx, Label.of("attribute"));
        tx.close();

        assertEquals(personCount, personCountReopened);
        assertEquals(ageCount, ageCountReopened);
        assertEquals(friendshipCount, friendshipCountReopened);
        assertEquals(implicitAgeCount, implicitAgeCountReopened);
        assertEquals(thingCount, thingCountReopened);
        assertEquals(entityCount, entityCountReopened);
        assertEquals(relationCount, relationCountReopened);
        assertEquals(attributeCount, attributeCountReopened);
    }

    @Test
    public void nonexistentLabelStatisticsReturnMinusOne() {
        Transaction tx = localSession.writeTransaction();
        long personCount = localSession.keyspaceStatistics().count(tx, Label.of("person"));
        tx.close();
        assertEquals(-1L, personCount);
    }


    @Test
    public void concurrentTransactionsUpdateStatisticsCorrectly() throws InterruptedException, ExecutionException {
        Transaction tx = localSession.writeTransaction();
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
            grakn.client.concept.AttributeType ageT = tx1.getAttributeType("age");
            grakn.client.concept.EntityType personT = tx1.getEntityType("person");
            ageT.create(2);
            ageT.create(3);
            personT.create();
            tx1.commit();
            return null;
        }, parallelExecutor);

        CompletableFuture<Void> future2 = CompletableFuture.supplyAsync(() -> {
            GraknClient.Transaction tx2 = remoteSession.transaction().write();
            grakn.client.concept.AttributeType ageT = tx2.getAttributeType("age");
            grakn.client.concept.EntityType personT = tx2.getEntityType("person");
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

        tx = localSession.writeTransaction();
        long personCount = localSession.keyspaceStatistics().count(tx, Label.of("person"));
        long ageCount = localSession.keyspaceStatistics().count(tx, Label.of("age"));
        long thingCount = localSession.keyspaceStatistics().count(tx, Label.of("thing"));
        long entityCount = localSession.keyspaceStatistics().count(tx, Label.of("entity"));
        long relationCount = localSession.keyspaceStatistics().count(tx, Label.of("relation"));
        long attributeCount = localSession.keyspaceStatistics().count(tx, Label.of("attribute"));

        tx.close();

        assertEquals(2L, personCount - personCountStart);
        assertEquals(3L, ageCount - ageCountStart);
        assertEquals(personCount, entityCount);
        assertEquals(0L, relationCount);
        assertEquals(ageCount, attributeCount);
        assertEquals(personCount + ageCount, thingCount);
    }

    @Test
    public void attachingAttributesViaRulesDoesntAlterConceptCountsAfterCommit() {
        // test concept API insertion
        Transaction tx = localSession.writeTransaction();
        AttributeType<Long> age = tx.putAttributeType("age", AttributeType.DataType.LONG);
        EntityType person = tx.putEntityType("person").has(age);

        person.create();
        person.create();
        tx.putRule(
                "someRule",
                Graql.parsePattern("{$x isa person;};"),
                Graql.parsePattern("{$x has age 27;};"));

        tx.commit();

        tx = localSession.writeTransaction();

        List<ConceptMap> answers = tx.execute(Graql.parse("match $x has age $r;get;").asGet());

        tx.commit();

        tx = localSession.writeTransaction();

        Label ageLabel = Label.of("age");
        assertEquals(0, localSession.keyspaceStatistics().count(tx, ageLabel));
        assertEquals(0, localSession.keyspaceStatistics().count(tx, Schema.ImplicitType.HAS.getLabel(ageLabel)));

        tx.close();
    }
}
