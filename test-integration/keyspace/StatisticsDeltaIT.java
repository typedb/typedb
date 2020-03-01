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

import grakn.core.kb.concept.api.Attribute;
import grakn.core.kb.concept.api.AttributeType;
import grakn.core.kb.concept.api.ConceptId;
import grakn.core.kb.concept.api.Entity;
import grakn.core.kb.concept.api.EntityType;
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.concept.api.Relation;
import grakn.core.kb.concept.api.RelationType;
import grakn.core.kb.concept.api.Role;
import grakn.core.kb.keyspace.StatisticsDelta;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.rule.GraknTestStorage;
import grakn.core.rule.SessionUtil;
import grakn.core.rule.TestTransactionProvider;
import graql.lang.Graql;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;

public class StatisticsDeltaIT {
    private Session session;
    private Transaction tx;

    @ClassRule
    public static final GraknTestStorage storage = new GraknTestStorage();

    @Before
    public void setUp() {
        session = SessionUtil.serverlessSessionWithNewKeyspace(storage.createCompatibleServerConfig());
        tx = session.writeTransaction();
        AttributeType age = tx.putAttributeType("age", AttributeType.DataType.LONG);
        Role friend = tx.putRole("friend");
        EntityType personType = tx.putEntityType("person").plays(friend).has(age);
        RelationType friendshipType = tx.putRelationType("friendship").relates(friend);
        tx.commit();
        tx = session.writeTransaction();
    }

    @After
    public void closeSession() {
        session.close();
    }


    @Test
    public void newTransactionsInitialisedWithEmptyStatsDelta() {
        TestTransactionProvider.TestTransaction testTx = ((TestTransactionProvider.TestTransaction)tx);
        StatisticsDelta statisticsDelta = testTx.uncomittedStatisticsDelta();

        long entityDelta = statisticsDelta.delta(Label.of("entity"));
        long relationDelta = statisticsDelta.delta(Label.of("relationDelta"));
        long attributeDelta = statisticsDelta.delta(Label.of("attribute"));

        assertEquals(0, entityDelta);
        assertEquals(0, relationDelta);
        assertEquals(0, attributeDelta);
        tx.close();
    }

    @Test
    public void addingEntitiesIncrementsCorrectly() {
        TestTransactionProvider.TestTransaction testTx = ((TestTransactionProvider.TestTransaction)tx);
        StatisticsDelta statisticsDelta = testTx.uncomittedStatisticsDelta();

        // test concept API insertion
        EntityType personType = tx.getEntityType("person");
        personType.create();

        // test Graql insertion
        tx.execute(Graql.parse("insert $x isa person;").asInsert());

        long personDelta = statisticsDelta.delta(Label.of("person"));
        assertEquals(2, personDelta);
        tx.close();
}

    @Test
    public void addingRelationsIncrementsCorrectly() {
        TestTransactionProvider.TestTransaction testTx = ((TestTransactionProvider.TestTransaction)tx);
        StatisticsDelta statisticsDelta = testTx.uncomittedStatisticsDelta();

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

        long friendshipDelta = statisticsDelta.delta(Label.of("friendship"));
        assertEquals(2, friendshipDelta);
        tx.close();
    }

    @Test
    public void addingAttributeValuesIncrementsCorrectly() {
        TestTransactionProvider.TestTransaction testTx = ((TestTransactionProvider.TestTransaction)tx);
        StatisticsDelta statisticsDelta = testTx.uncomittedStatisticsDelta();

        // test concept API insertion
        AttributeType age = tx.getAttributeType("age");
        age.create(1);

        // test Graql insertion
        tx.execute(Graql.parse("insert $a 99 isa age;").asInsert());
        tx.execute(Graql.parse("insert $a 1 isa age;").asInsert());

        long ageDelta = statisticsDelta.delta(Label.of("age"));
        assertEquals(2, ageDelta);
        tx.close();
    }

    @Test
    public void addingAttributeOwnersIncrementsImplicitRelations() {
        TestTransactionProvider.TestTransaction testTx = ((TestTransactionProvider.TestTransaction)tx);
        StatisticsDelta statisticsDelta = testTx.uncomittedStatisticsDelta();

        // test concept API insertion
        AttributeType ageType = tx.getAttributeType("age");
        Attribute age = ageType.create(1);
        EntityType personType = tx.getEntityType("person");
        Entity person = personType.create().has(age);

        // test Graql insertion with two ages, one of which is shared
        tx.execute(Graql.parse("insert $x isa person, has age 99, has age 1;").asInsert());

        long ageDelta = statisticsDelta.delta(Label.of("age"));
        assertEquals(2, ageDelta);
        long personDelta = statisticsDelta.delta(Label.of("person"));
        assertEquals(2, personDelta);
        long implicitAgeRelation = statisticsDelta.delta(Label.of("@has-age"));
        assertEquals(3, implicitAgeRelation);
        tx.close();
    }


    @Test
    public void removingEntitiesDecrementsCorrectly() {
        // test concept API insertion
        EntityType personType = tx.getEntityType("person");
        ConceptId id1 = personType.create().id();
        ConceptId id2 = personType.create().id();
        ConceptId id3 = personType.create().id();
        tx.commit();

        tx = session.writeTransaction();
        TestTransactionProvider.TestTransaction testTx = ((TestTransactionProvider.TestTransaction)tx);
        StatisticsDelta statisticsDelta = testTx.uncomittedStatisticsDelta();

        // test concept API deletion
        tx.getConcept(id1).delete();

        // test Graql deletion
        tx.execute(Graql.parse("match $x id " + id2 + "; delete $x;").asDelete());

        long personDelta = statisticsDelta.delta(Label.of("person"));
        assertEquals(-2, personDelta);
        tx.close();
    }

    @Test
    public void removingRelationsDecrementsCorrectly() {
        EntityType personType = tx.getEntityType("person");
        Entity person1 = personType.create();
        Entity person2 = personType.create();
        Entity person3 = personType.create();
        RelationType friendship = tx.getRelationType("friendship");
        Role friend = tx.getRole("friend");
        ConceptId id1 = friendship.create().assign(friend, person1).assign(friend, person2).id();
        ConceptId id2 = friendship.create().assign(friend, person1).assign(friend, person3).id();
        ConceptId id3 = friendship.create().assign(friend, person2).assign(friend, person3).id();

        tx.commit();

        tx = session.writeTransaction();
        TestTransactionProvider.TestTransaction testTx = ((TestTransactionProvider.TestTransaction)tx);
        StatisticsDelta statisticsDelta = testTx.uncomittedStatisticsDelta();


        // test concept API deletion
        tx.getConcept(id1).delete();

        // test Graql deletion
        tx.execute(Graql.parse("match $x id " + id2 + "; delete $x;").asDelete());
        tx.execute(Graql.parse("match $x id " + id3 + "; delete $x;").asDelete());

        long friendshipDelta = statisticsDelta.delta(Label.of("friendship"));
        assertEquals(-3, friendshipDelta);
    }

    @Test
    public void removingAttributesDecrementsCorrectly() {
        AttributeType age = tx.getAttributeType("age");
        ConceptId lastAttributeId = null;
        for (int i = 0; i < 100; i++) {
            lastAttributeId = age.create(i).id();
        }
        tx.commit();

        tx = session.writeTransaction();
        TestTransactionProvider.TestTransaction testTx = ((TestTransactionProvider.TestTransaction)tx);
        StatisticsDelta statisticsDelta = testTx.uncomittedStatisticsDelta();

        // test ConceptAPI deletion
        tx.getConcept(lastAttributeId).delete();

        // test Graql deletion
        tx.execute(Graql.parse("match $x 50 isa age; $y 51 isa age; $z 52 isa age; delete $x, $y, $z;").asDelete());

        long ageDelta = statisticsDelta.delta(Label.of("age"));
        assertEquals(-4, ageDelta);
        tx.close();
    }

    @Test
    public void deletingAttributeDecrementsImplicitRelsAndAttribute() {

        // test concept API insertion
        AttributeType ageType = tx.getAttributeType("age");
        Attribute age1 = ageType.create(1);
        Attribute age2 = ageType.create(99);
        ConceptId age2Id  = age2.id();
        EntityType personType = tx.getEntityType("person");
        personType.create().has(age1);
        personType.create().has(age1);
        personType.create().has(age2);

        tx.commit();

        tx = session.writeTransaction();
        TestTransactionProvider.TestTransaction testTx = ((TestTransactionProvider.TestTransaction)tx);
        StatisticsDelta statisticsDelta = testTx.uncomittedStatisticsDelta();

        // test ConceptAPI deletion
        tx.getConcept(age2Id).delete();

        // test deletion
        tx.execute(Graql.parse("match $x 1 isa age; delete $x;").asDelete());

        long ageDelta = statisticsDelta.delta(Label.of("age"));
        assertEquals(-2, ageDelta);
        long personDelta = statisticsDelta.delta(Label.of("person"));
        assertEquals(0, personDelta);
        long implicitAgeRelation = statisticsDelta.delta(Label.of("@has-age"));
        assertEquals(-3, implicitAgeRelation);

        tx.close();
    }

    @Test
    public void whenAttributeAndOwnerIsCreatedAndDeleted_countIsUnaffected() {
        TestTransactionProvider.TestTransaction testTx = ((TestTransactionProvider.TestTransaction)tx);
        StatisticsDelta statisticsDelta = testTx.uncomittedStatisticsDelta();

        // test concept API insertion
        AttributeType ageType = tx.getAttributeType("age");
        Attribute age1 = ageType.create(1);
        EntityType personType = tx.getEntityType("person");

        Entity person = personType.create();
        person.has(age1);

        long ageDelta = statisticsDelta.delta(Label.of("age"));
        assertEquals(1, ageDelta);
        long implicitAgeRelation = statisticsDelta.delta(Label.of("@has-age"));
        assertEquals(1, implicitAgeRelation);

        // test ConceptAPI deletion
        age1.delete();
        person.delete();

        long personDeltaAfter = statisticsDelta.delta(Label.of("person"));
        assertEquals(0, personDeltaAfter);
        long ageDeltaAfter = statisticsDelta.delta(Label.of("age"));
        assertEquals(0, ageDeltaAfter);
        long implicitAgeRelationAfter = statisticsDelta.delta(Label.of("@has-age"));
        assertEquals(0, implicitAgeRelationAfter);

        tx.close();
    }

    @Test
    public void whenRelationWithRolePlayersIsCreatedAndDeleted_countIsUnaffected() {
        TestTransactionProvider.TestTransaction testTx = ((TestTransactionProvider.TestTransaction)tx);
        StatisticsDelta statisticsDelta = testTx.uncomittedStatisticsDelta();

        // test concept API insertion
        Role someRole = tx.putRole("someRole");
        Role anotherRole = tx.putRole("anotherRole");
        RelationType relationType = tx.putRelationType("someRelation")
                .relates(someRole)
                .relates(anotherRole);
        EntityType personType = tx.getEntityType("person")
                .plays(someRole)
                .plays(anotherRole);

        Entity person = personType.create();
        Entity anotherPerson = personType.create();
        Relation relation = relationType.create().assign(someRole, person).assign(anotherRole, person);

        relation.delete();
        person.delete();
        anotherPerson.delete();

        long personDelta = statisticsDelta.delta(Label.of("person"));
        long relationDelta = statisticsDelta.delta(Label.of("someRelation"));
        assertEquals(0, personDelta);
        assertEquals(0, relationDelta);

        tx.close();
    }
}
