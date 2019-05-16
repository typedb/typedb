/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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

import grakn.core.concept.thing.Attribute;
import grakn.core.concept.thing.Entity;
import grakn.core.concept.type.AttributeType;
import grakn.core.concept.type.EntityType;
import grakn.core.concept.type.RelationType;
import grakn.core.concept.type.Role;
import grakn.core.rule.GraknTestServer;
import grakn.core.server.session.SessionImpl;
import grakn.core.server.session.TransactionOLTP;
import graql.lang.Graql;
import junit.framework.TestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class KeyspaceStatisticsIT {

    private SessionImpl session;

    @ClassRule
    public static final GraknTestServer server = new GraknTestServer();

    @Before
    public void setUp() {
        session = server.sessionWithNewKeyspace();
    }

    @After
    public void closeSession() {
        session.close();
    }


    @Test
    public void newKeyspaceHasZeroCounts() {
        KeyspaceStatistics statistics = session.keyspaceStatistics();
        TransactionOLTP tx = session.transaction().write();
        long entityCount = statistics.count(tx, "entity");
        long relationCount = statistics.count(tx, "relation");
        long attributeCount = statistics.count(tx, "attribute");
        tx.close();

        assertEquals(0, entityCount);
        assertEquals(0, relationCount);
        assertEquals(0, attributeCount);
    }

    @Test
    public void sessionsToSameKeyspaceShareStatistics() {
        SessionImpl session2 = server.session(session.keyspace().name());
        assertSame(session.keyspaceStatistics(), session2.keyspaceStatistics());
    }

    @Test
    public void keyspaceStatisticsUpdatedOnCommit() {
        TransactionOLTP tx = session.transaction().write();
        AttributeType ageType = tx.putAttributeType("age", AttributeType.DataType.LONG);
        Role friend = tx.putRole("friend");
        EntityType personType = tx.putEntityType("person").plays(friend).has(ageType);
        RelationType friendshipType = tx.putRelationType("friendship").relates(friend);
        tx.commit();

        tx = session.transaction().write();
        ageType = tx.getAttributeType("age");
        Attribute age = ageType.create(1);
        personType = tx.getEntityType("person");
        Entity person1 = personType.create().has(age).has(age);
        Entity person2 = personType.create().has(age);
        friendshipType = tx.getRelationType("friendship");
        friend = tx.getRole("friend");
        friendshipType.create().assign(friend, person1).assign(friend, person2);
        tx.commit();

        tx = session.transaction().write();
        long personCount = session.keyspaceStatistics().count(tx, "person");
        long ageCount = session.keyspaceStatistics().count(tx, "age");
        long friendshipCount = session.keyspaceStatistics().count(tx, "friendship");
        long implicitAgeCount = session.keyspaceStatistics().count(tx, "@has-age");
        tx.close();

        assertEquals(2, personCount);
        assertEquals(1, ageCount);
        assertEquals(1, friendshipCount);
        assertEquals(3, implicitAgeCount);

        tx = session.transaction().write();
        tx.execute(Graql.parse("match $x isa friendship; delete $x;").asDelete());
        tx.commit();

        tx = session.transaction().write();
        personCount = session.keyspaceStatistics().count(tx, "person");
        ageCount = session.keyspaceStatistics().count(tx, "age");
        friendshipCount = session.keyspaceStatistics().count(tx, "friendship");
        implicitAgeCount = session.keyspaceStatistics().count(tx, "@has-age");
        tx.close();

        assertEquals(2, personCount);
        assertEquals(1, ageCount);
        assertEquals(0, friendshipCount);
        assertEquals(3, implicitAgeCount);

        tx = session.transaction().write();
        tx.execute(Graql.parse("match $x isa thing; delete $x;").asDelete());
        tx.commit();
        tx = session.transaction().write();
        personCount = session.keyspaceStatistics().count(tx, "person");
        ageCount = session.keyspaceStatistics().count(tx, "age");
        friendshipCount = session.keyspaceStatistics().count(tx, "friendship");
        implicitAgeCount = session.keyspaceStatistics().count(tx, "@has-age");
        tx.close();

        assertEquals(0, personCount);
        assertEquals(0, ageCount);
        assertEquals(0, friendshipCount);
        assertEquals(0, implicitAgeCount);
    }

    @Test
    public void keyspaceStatisticsNotUpdatedIfNotcommitted() {
        TransactionOLTP tx = session.transaction().write();
        AttributeType ageType = tx.putAttributeType("age", AttributeType.DataType.LONG);
        Role friend = tx.putRole("friend");
        EntityType personType = tx.putEntityType("person").plays(friend).has(ageType);
        RelationType friendshipType = tx.putRelationType("friendship").relates(friend);
        tx.commit();

        tx = session.transaction().write();
        ageType = tx.getAttributeType("age");
        Attribute age = ageType.create(1);
        personType = tx.getEntityType("person");
        Entity person1 = personType.create().has(age).has(age);
        Entity person2 = personType.create().has(age);
        friendshipType = tx.getRelationType("friendship");
        friend = tx.getRole("friend");
        friendshipType.create().assign(friend, person1).assign(friend, person2);
        tx.close();

        tx = session.transaction().write();
        long personCount = session.keyspaceStatistics().count(tx, "person");
        long ageCount = session.keyspaceStatistics().count(tx, "age");
        long friendshipCount = session.keyspaceStatistics().count(tx, "friendship");
        long implicitAgeCount = session.keyspaceStatistics().count(tx, "@has-age");
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
        TransactionOLTP tx = session.transaction().write();
        AttributeType ageType = tx.putAttributeType("age", AttributeType.DataType.LONG);
        Role friend = tx.putRole("friend");
        EntityType personType = tx.putEntityType("person").plays(friend).has(ageType);
        RelationType friendshipType = tx.putRelationType("friendship").relates(friend);
        tx.commit();

        tx = session.transaction().write();
        ageType = tx.getAttributeType("age");
        Attribute age = ageType.create(1);
        personType = tx.getEntityType("person");
        Entity person1 = personType.create().has(age).has(age);
        Entity person2 = personType.create().has(age);
        friendshipType = tx.getRelationType("friendship");
        friend = tx.getRole("friend");
        friendshipType.create().assign(friend, person1).assign(friend, person2);
        tx.commit();

        tx = session.transaction().write();
        long personCount = session.keyspaceStatistics().count(tx, "person");
        long ageCount = session.keyspaceStatistics().count(tx, "age");
        long friendshipCount = session.keyspaceStatistics().count(tx, "friendship");
        long implicitAgeCount = session.keyspaceStatistics().count(tx, "@has-age");
        tx.close();

        session.close();

        // at this point, the graph and keyspace should be deleted from Grakn server
        session = server.session(session.keyspace().name());

        tx = session.transaction().write();
        long personCountReopened = session.keyspaceStatistics().count(tx, "person");
        long ageCountReopened = session.keyspaceStatistics().count(tx, "age");
        long friendshipCountReopened = session.keyspaceStatistics().count(tx, "friendship");
        long implicitAgeCountReopened = session.keyspaceStatistics().count(tx, "@has-age");
        tx.close();

        TestCase.assertEquals(personCount, personCountReopened);
        TestCase.assertEquals(ageCount, ageCountReopened);
        TestCase.assertEquals(friendshipCount, friendshipCountReopened);
        TestCase.assertEquals(implicitAgeCount, implicitAgeCountReopened);

    }
}
