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
    public void closeSession() { session.close(); }


    @Test
    public void newKeyspaceHasZeroCounts() {
        KeyspaceStatistics statistics = session.keyspaceStatistics();
        long entityCount = statistics.count("entity");
        long relationCount = statistics.count("relation");
        long attributeCount = statistics.count("attribute");

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

        long personCount = session.keyspaceStatistics().count("person");
        long ageCount = session.keyspaceStatistics().count("age");
        long friendshipCount = session.keyspaceStatistics().count("friendship");
        long implicitAgeCount = session.keyspaceStatistics().count("@has-age");

        assertEquals(2, personCount);
        assertEquals(1, ageCount);
        assertEquals(1, friendshipCount);
        assertEquals(3, implicitAgeCount);

        tx = session.transaction().write();
        tx.execute(Graql.parse("match $x isa friendship; delete $x;").asDelete());
        tx.commit();
        personCount = session.keyspaceStatistics().count("person");
        ageCount = session.keyspaceStatistics().count("age");
        friendshipCount = session.keyspaceStatistics().count("friendship");
        implicitAgeCount = session.keyspaceStatistics().count("@has-age");

        assertEquals(2, personCount);
        assertEquals(1, ageCount);
        assertEquals(0, friendshipCount);
        assertEquals(3, implicitAgeCount);

        tx = session.transaction().write();
        tx.execute(Graql.parse("match $x isa thing; delete $x;").asDelete());
        tx.commit();
        personCount = session.keyspaceStatistics().count("person");
        ageCount = session.keyspaceStatistics().count("age");
        friendshipCount = session.keyspaceStatistics().count("friendship");
        implicitAgeCount = session.keyspaceStatistics().count("@has-age");

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

        long personCount = session.keyspaceStatistics().count("person");
        long ageCount = session.keyspaceStatistics().count("age");
        long friendshipCount = session.keyspaceStatistics().count("friendship");
        long implicitAgeCount = session.keyspaceStatistics().count("@has-age");

        assertEquals(0, personCount);
        assertEquals(0, ageCount);
        assertEquals(0, friendshipCount);
        assertEquals(0, implicitAgeCount);
    }

    // ------- persistence is TODO
    // only work on tests that can be performed in-memory

}
