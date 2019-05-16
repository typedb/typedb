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

import static junit.framework.TestCase.assertEquals;

public class UncomittedStatisticsDeltaIT {
    private SessionImpl session;

    @ClassRule
    public static final GraknTestServer server = new GraknTestServer();

    @Before
    public void setUp() {
        session = server.sessionWithNewKeyspace();
        TransactionOLTP tx = session.transaction().write();
        AttributeType age = tx.putAttributeType("age", AttributeType.DataType.LONG);
        Role friend = tx.putRole("friend");
        EntityType personType = tx.putEntityType("person").plays(friend).has(age);
        RelationType friendshipType = tx.putRelationType("friendship").relates(friend);
        tx.commit();
    }

    @After
    public void closeSession() { session.close(); }


    @Test
    public void newTransactionsInitialisedWithEmptyStatsDelta() {
        TransactionOLTP tx = session.transaction().write();
        UncomittedStatisticsDelta statisticsDelta = tx.statisticsDelta();

        long entityDelta = statisticsDelta.delta("entity");
        long relationDelta = statisticsDelta.delta("relationDelta");
        long attributeDelta = statisticsDelta.delta("attribute");

        assertEquals(0, entityDelta);
        assertEquals(0, relationDelta);
        assertEquals(0, attributeDelta);
    }

    @Test
    public void addingEntitiesIncrementsCorrectly() {
        TransactionOLTP tx = session.transaction().write();

        // test concept API insertion
        EntityType personType = tx.getEntityType("person");
        personType.create();

        // test Graql insertion
        tx.execute(Graql.parse("insert $x isa person;").asInsert());

        UncomittedStatisticsDelta statisticsDelta = tx.statisticsDelta();
        long personDelta = statisticsDelta.delta("person");
        assertEquals(2, personDelta);
    }

    @Test
    public void addingRelationsIncrementsCorrectly() {
        TransactionOLTP tx = session.transaction().write();
        EntityType personType = tx.getEntityType("person");
        Entity person1 = personType.create();
        Entity person2 = personType.create();
        Entity person3 = personType.create();

        // test concept API insertion
        RelationType friendship = tx.getRelationType("friendship");
        Role friend = tx.getRole("friend");
        friendship.create().assign(friend, person1).assign(friend, person2);

        // test Graql insertion
        tx.execute(Graql.parse("match $x id " + person1.id() + "; $y id " + person2.id() + "; $z id " + person3.id() + ";" +
                "insert $r (friend: $x, friend: $y, friend: $z) isa friendship;").asInsert());

        UncomittedStatisticsDelta statisticsDelta = tx.statisticsDelta();
        long friendshipDelta = statisticsDelta.delta("friendship");
        assertEquals(2, friendshipDelta);
    }

    @Test
    public void addingAttributeValuesIncrementsCorrectly() {
        TransactionOLTP tx = session.transaction().write();

        // test concept API insertion
        AttributeType age = tx.getAttributeType("age");
        age.create(1);

        // test Graql insertion
        tx.execute(Graql.parse("insert $a 99 isa age;").asInsert());

        // test deduplication withing a single tx too
        tx.execute(Graql.parse("insert $a 1 isa age;").asInsert());

        UncomittedStatisticsDelta statisticsDelta = tx.statisticsDelta();
        long ageDelta = statisticsDelta.delta("age");
        assertEquals(2, ageDelta);
    }

    @Test
    public void addingAttributeOwnersIncrementsImplicitRelations() {
        TransactionOLTP tx = session.transaction().write();

        // test concept API insertion
        AttributeType ageType = tx.getAttributeType("age");
        Attribute age = ageType.create(1);
        EntityType personType = tx.getEntityType("person");
        Entity person = personType.create().has(age);

        // test Graql insertion with two ages, one of which is shared
        tx.execute(Graql.parse("insert $x isa person, has age 99, has age 1;").asInsert());

        UncomittedStatisticsDelta statisticsDelta = tx.statisticsDelta();
        long ageDelta = statisticsDelta.delta("age");
        assertEquals(2, ageDelta);
        long personDelta = statisticsDelta.delta("person");
        assertEquals(2, personDelta);
        long implicitAgeRelation = statisticsDelta.delta("@has-age");
        assertEquals(3, implicitAgeRelation);
    }


}
