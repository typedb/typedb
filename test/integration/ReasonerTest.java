/*
 * Copyright (C) 2021 Grakn Labs
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

package grakn.core.test.integration;

import grakn.core.common.parameters.Arguments;
import grakn.core.concept.ConceptManager;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concept.type.AttributeType;
import grakn.core.concept.type.EntityType;
import grakn.core.concept.type.RelationType;
import grakn.core.logic.LogicManager;
import grakn.core.rocks.RocksGrakn;
import grakn.core.rocks.RocksSession;
import grakn.core.rocks.RocksTransaction;
import grakn.core.test.integration.util.Util;
import graql.lang.Graql;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertEquals;

public class ReasonerTest {

    private static Path directory = Paths.get(System.getProperty("user.dir")).resolve("query-test");
    private static String database = "reasoner-test";
    private static RocksGrakn grakn;
    private static RocksSession session;
    private static RocksTransaction rocksTransaction;
    private static ConceptManager conceptMgr;
    private static LogicManager logicMgr;

    @Test
    public void test_has_explicit_rule() throws IOException {
        Util.resetDirectory(directory);
        grakn = RocksGrakn.open(directory);
        grakn.databases().create(database);
        session = grakn.session(database, Arguments.Session.Type.SCHEMA);
        rocksTransaction = session.transaction(Arguments.Transaction.Type.WRITE);
        conceptMgr = rocksTransaction.concepts();
        logicMgr = rocksTransaction.logic();

        final EntityType milk = conceptMgr.putEntityType("milk");
        final AttributeType ageInDays = conceptMgr.putAttributeType("age-in-days", AttributeType.ValueType.LONG);
        final AttributeType isStillGood = conceptMgr.putAttributeType("is-still-good", AttributeType.ValueType.BOOLEAN);
        milk.setOwns(ageInDays);
        milk.setOwns(isStillGood);
        logicMgr.putRule(
                "old-milk-is-not-good",
                Graql.parsePattern("{ $x isa milk, has age-in-days >= 10; }").asConjunction(),
//                Graql.parsePattern("{ $x isa milk; }").asConjunction(),
                Graql.parseVariable("$x has is-still-good false").asThing());
        rocksTransaction.commit();
        session.close();

        session = grakn.session(database, Arguments.Session.Type.DATA);

        rocksTransaction = session.transaction(Arguments.Transaction.Type.WRITE);
        rocksTransaction.query().insert(Graql.parseQuery("insert $x isa milk, has age-in-days 5;").asInsert());
        rocksTransaction.query().insert(Graql.parseQuery("insert $x isa milk, has age-in-days 10;").asInsert());
        rocksTransaction.query().insert(Graql.parseQuery("insert $x isa milk, has age-in-days 15;").asInsert());
        rocksTransaction.commit();

        rocksTransaction = session.transaction(Arguments.Transaction.Type.WRITE);
        List<ConceptMap> ans = rocksTransaction.query().match(Graql.parseQuery("match $x has is-still-good $a;").asMatch()).toList();
        System.out.println(ans);

        ans.iterator().forEachRemaining(a -> {
            assertFalse(a.get("a").asAttribute().asBoolean().getValue());
            assertEquals("is-still-good", a.get("a").asThing().getType().getLabel().scopedName());
            assertEquals("milk", a.get("x").asThing().getType().getLabel().scopedName());
        });

        assertEquals(2, ans.size());

        rocksTransaction.close();
        session.close();
    }

    @Test
    public void test_relation_rule() throws IOException {
        Util.resetDirectory(directory);

        try (RocksGrakn grakn = RocksGrakn.open(directory)) {
            grakn.databases().create(database);
            try (RocksSession session = grakn.session(database, Arguments.Session.Type.SCHEMA)) {
                try (RocksTransaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    final ConceptManager conceptMgr = txn.concepts();
                    final LogicManager logicMgr = txn.logic();

                    final EntityType person = conceptMgr.putEntityType("person");
                    final AttributeType name = conceptMgr.putAttributeType("name", AttributeType.ValueType.STRING);
                    person.setOwns(name);
                    final RelationType friendship = conceptMgr.putRelationType("friendship");
                    friendship.setRelates("friend");
                    final RelationType marriage = conceptMgr.putRelationType("marriage");
                    marriage.setRelates("spouse");
                    person.setPlays(friendship.getRelates("friend"));
                    person.setPlays(marriage.getRelates("spouse"));
                    logicMgr.putRule(
                            "marriage-is-friendship",
                            Graql.parsePattern("{ $x isa person; $y isa person; (spouse: $x, spouse: $y) isa marriage; }").asConjunction(),
                            Graql.parseVariable("(friend: $x, friend: $y) isa friendship").asThing());
                    txn.commit();
                }
            }
            try (RocksSession session = grakn.session(database, Arguments.Session.Type.DATA)) {
                try (RocksTransaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    txn.query().insert(Graql.parseQuery("insert $x isa person, has name 'Zack'; $y isa person, has name 'Yasmin'; (spouse: $x, spouse: $y) isa marriage;").asInsert());
                    txn.commit();
                }
                try (RocksTransaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    List<ConceptMap> ans = txn.query().match(Graql.parseQuery("match $r (friend: $x, friend: $y) isa friendship; $x has name $a;").asMatch()).toList();
                    System.out.println(ans);

                    ans.iterator().forEachRemaining(a -> {
                        assertEquals("friendship", a.get("r").asThing().getType().getLabel().scopedName());
                        assertEquals("person", a.get("x").asThing().getType().getLabel().scopedName());
                        assertEquals("person", a.get("y").asThing().getType().getLabel().scopedName());
                    });

                    assertEquals(2, ans.size());
                }
            }
        }
    }
}
