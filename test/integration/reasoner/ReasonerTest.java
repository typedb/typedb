/*
 * Copyright (C) 2021 Vaticle
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

package com.vaticle.typedb.core.reasoner;

import com.vaticle.typedb.common.concurrent.NamedThreadFactory;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.common.parameters.Options;
import com.vaticle.typedb.core.common.parameters.Options.Database;
import com.vaticle.typedb.core.concept.ConceptManager;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concept.type.AttributeType;
import com.vaticle.typedb.core.concept.type.EntityType;
import com.vaticle.typedb.core.concept.type.RelationType;
import com.vaticle.typedb.core.concurrent.actor.ActorExecutorGroup;
import com.vaticle.typedb.core.logic.LogicManager;
import com.vaticle.typedb.core.rocks.RocksDatabaseManager;
import com.vaticle.typedb.core.rocks.RocksSession;
import com.vaticle.typedb.core.rocks.RocksTransaction;
import com.vaticle.typedb.core.test.integration.util.Util;
import com.vaticle.typeql.lang.TypeQL;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static com.vaticle.typedb.core.common.collection.Bytes.MB;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Reasoner.RESOLUTION_TERMINATED;
import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ReasonerTest {

    private static final Path dataDir = Paths.get(System.getProperty("user.dir")).resolve("query-test");
    private static final Path logDir = dataDir.resolve("logs");
    private static final Database options = new Database().dataDir(dataDir).reasonerDebuggerDir(logDir)
            .storageDataCacheSize(MB).storageIndexCacheSize(MB);
    private static final String database = "reasoner-test";
    private static RocksDatabaseManager databaseManager;

    private RocksTransaction singleThreadElgTransaction(RocksSession session, Arguments.Transaction.Type transactionType) {
        RocksTransaction transaction = session.transaction(transactionType, new Options.Transaction().infer(true));
        ActorExecutorGroup service = new ActorExecutorGroup(1, new NamedThreadFactory("typedb-actor"));
        transaction.reasoner().resolverRegistry().setExecutorService(service);
        return transaction;
    }

    @Before
    public void setUp() throws IOException {
        Util.resetDirectory(dataDir);
        databaseManager = RocksDatabaseManager.open(options);
        databaseManager.create(database);
    }

    @After
    public void tearDown() {
        databaseManager.close();
    }
    @Test
    public void test_no_rules() {
        try (RocksSession session = databaseManager.session(database, Arguments.Session.Type.SCHEMA)) {
            try (RocksTransaction txn = singleThreadElgTransaction(session, Arguments.Transaction.Type.WRITE)) {
                ConceptManager conceptMgr = txn.concepts();

                EntityType milk = conceptMgr.putEntityType("milk");
                AttributeType ageInDays = conceptMgr.putAttributeType("age-in-days", AttributeType.ValueType.LONG);
                milk.setOwns(ageInDays);
                txn.commit();
            }
        }
        try (RocksSession session = databaseManager.session(database, Arguments.Session.Type.DATA)) {
            try (RocksTransaction txn = singleThreadElgTransaction(session, Arguments.Transaction.Type.WRITE)) {
                txn.query().insert(TypeQL.parseQuery("insert $x isa milk, has age-in-days 5;").asInsert());
                txn.query().insert(TypeQL.parseQuery("insert $x isa milk, has age-in-days 10;").asInsert());
                txn.commit();
            }
            try (RocksTransaction txn = singleThreadElgTransaction(session, Arguments.Transaction.Type.READ)) {
                List<ConceptMap> ans = txn.query().match(TypeQL.parseQuery("match $x has age-in-days $a;").asMatch()).toList();

                ans.iterator().forEachRemaining(a -> {
                    assertEquals("age-in-days", a.get("a").asThing().getType().getLabel().scopedName());
                    assertEquals("milk", a.get("x").asThing().getType().getLabel().scopedName());
                });
                assertEquals(2, ans.size());
            }
        }
    }

    @Test
    public void test_offset_limit() {
        try (RocksSession session = databaseManager.session(database, Arguments.Session.Type.SCHEMA)) {
            try (RocksTransaction txn = singleThreadElgTransaction(session, Arguments.Transaction.Type.WRITE)) {
                ConceptManager conceptMgr = txn.concepts();
                EntityType milk = conceptMgr.putEntityType("milk");
                AttributeType ageInDays = conceptMgr.putAttributeType("age-in-days", AttributeType.ValueType.LONG);
                milk.setOwns(ageInDays);
                txn.commit();
            }
        }
        try (RocksSession session = databaseManager.session(database, Arguments.Session.Type.DATA)) {
            try (RocksTransaction txn = singleThreadElgTransaction(session, Arguments.Transaction.Type.WRITE)) {
                txn.query().insert(TypeQL.parseQuery("insert $x isa milk, has age-in-days 5;").asInsert());
                txn.query().insert(TypeQL.parseQuery("insert $x isa milk, has age-in-days 10;").asInsert());
                txn.commit();
            }
            try (RocksTransaction txn = singleThreadElgTransaction(session, Arguments.Transaction.Type.READ)) {
                List<ConceptMap> ans = txn.query().match(TypeQL.parseQuery("match $x has age-in-days $a;").asMatch()).toList();

                ans.iterator().forEachRemaining(a -> {
                    assertEquals("age-in-days", a.get("a").asThing().getType().getLabel().scopedName());
                    assertEquals("milk", a.get("x").asThing().getType().getLabel().scopedName());
                });
                assertEquals(2, ans.size());

                List<ConceptMap> ansLimited = txn.query().match(TypeQL.parseQuery("match $x has age-in-days $a; limit 1;").asMatch()).toList();
                assertEquals(1, ansLimited.size());

                List<ConceptMap> ansLimitedOffsetted = txn.query().match(TypeQL.parseQuery("match $x has age-in-days $a; offset 1; limit 1;").asMatch()).toList();
                assertEquals(1, ansLimitedOffsetted.size());
            }
        }
    }

    @Test
    public void test_exception_kills_query() {
        try (RocksSession session = databaseManager.session(database, Arguments.Session.Type.SCHEMA)) {
            try (RocksTransaction txn = singleThreadElgTransaction(session, Arguments.Transaction.Type.WRITE)) {
                ConceptManager conceptMgr = txn.concepts();
                LogicManager logicMgr = txn.logic();

                EntityType milk = conceptMgr.putEntityType("milk");
                AttributeType ageInDays = conceptMgr.putAttributeType("age-in-days", AttributeType.ValueType.LONG);
                AttributeType isStillGood = conceptMgr.putAttributeType("is-still-good", AttributeType.ValueType.BOOLEAN);
                milk.setOwns(ageInDays);
                milk.setOwns(isStillGood);
                logicMgr.putRule(
                        "old-milk-is-not-good",
                        TypeQL.parsePattern("{ $x isa milk, has age-in-days >= 10; }").asConjunction(),
                        TypeQL.parseVariable("$x has is-still-good false").asThing());
                txn.commit();
            }
        }
        try (RocksSession session = databaseManager.session(database, Arguments.Session.Type.DATA)) {
            try (RocksTransaction txn = singleThreadElgTransaction(session, Arguments.Transaction.Type.WRITE)) {
                txn.query().insert(TypeQL.parseQuery("insert $x isa milk, has age-in-days 5;").asInsert());
                txn.query().insert(TypeQL.parseQuery("insert $x isa milk, has age-in-days 10;").asInsert());
                txn.commit();
            }
            try (RocksTransaction txn = singleThreadElgTransaction(session, Arguments.Transaction.Type.READ)) {
                txn.reasoner().resolverRegistry().terminateResolvers(new RuntimeException());
                try {
                    List<ConceptMap> ans = txn.query().match(TypeQL.parseQuery("match $x isa is-still-good;").asMatch()).toList();
                } catch (TypeDBException e) {
                    assertEquals(e.code().get(), RESOLUTION_TERMINATED.code());
                    return;
                }
                fail();
            }
        }
    }

    @Test
    public void test_has_explicit_rule() {
        try (RocksSession session = databaseManager.session(database, Arguments.Session.Type.SCHEMA)) {
            try (RocksTransaction txn = singleThreadElgTransaction(session, Arguments.Transaction.Type.WRITE)) {
                ConceptManager conceptMgr = txn.concepts();
                LogicManager logicMgr = txn.logic();

                EntityType milk = conceptMgr.putEntityType("milk");
                AttributeType ageInDays = conceptMgr.putAttributeType("age-in-days", AttributeType.ValueType.LONG);
                AttributeType isStillGood = conceptMgr.putAttributeType("is-still-good", AttributeType.ValueType.BOOLEAN);
                milk.setOwns(ageInDays);
                milk.setOwns(isStillGood);
                logicMgr.putRule(
                        "old-milk-is-not-good",
                        TypeQL.parsePattern("{ $x isa milk, has age-in-days >= 10; }").asConjunction(),
                        TypeQL.parseVariable("$x has is-still-good false").asThing());
                txn.commit();
            }
        }

        try (RocksSession session = databaseManager.session(database, Arguments.Session.Type.DATA)) {
            try (RocksTransaction txn = singleThreadElgTransaction(session, Arguments.Transaction.Type.WRITE)) {
                txn.query().insert(TypeQL.parseQuery("insert $x isa milk, has age-in-days 5;").asInsert());
                txn.query().insert(TypeQL.parseQuery("insert $x isa milk, has age-in-days 10;").asInsert());
                txn.query().insert(TypeQL.parseQuery("insert $x isa milk, has age-in-days 15;").asInsert());
                txn.commit();
            }
            try (RocksTransaction txn = singleThreadElgTransaction(session, Arguments.Transaction.Type.READ)) {
                List<ConceptMap> ans = txn.query().match(TypeQL.parseQuery("match $x has is-still-good $a;").asMatch()).toList();

                ans.iterator().forEachRemaining(a -> {
                    assertFalse(a.get("a").asAttribute().asBoolean().getValue());
                    assertEquals("is-still-good", a.get("a").asThing().getType().getLabel().scopedName());
                    assertEquals("milk", a.get("x").asThing().getType().getLabel().scopedName());
                });
                assertEquals(2, ans.size());
            }
        }
    }

    @Test
    public void test_relation_rule() {
        try (RocksSession session = databaseManager.session(database, Arguments.Session.Type.SCHEMA)) {
            try (RocksTransaction txn = singleThreadElgTransaction(session, Arguments.Transaction.Type.WRITE)) {
                ConceptManager conceptMgr = txn.concepts();
                LogicManager logicMgr = txn.logic();

                EntityType person = conceptMgr.putEntityType("person");
                AttributeType name = conceptMgr.putAttributeType("name", AttributeType.ValueType.STRING);
                person.setOwns(name);
                RelationType friendship = conceptMgr.putRelationType("friendship");
                friendship.setRelates("friend");
                RelationType marriage = conceptMgr.putRelationType("marriage");
                marriage.setRelates("husband");
                marriage.setRelates("wife");
                person.setPlays(friendship.getRelates("friend"));
                person.setPlays(marriage.getRelates("husband"));
                person.setPlays(marriage.getRelates("wife"));
                logicMgr.putRule(
                        "marriage-is-friendship",
                        TypeQL.parsePattern("{ $x isa person; $y isa person; (husband: $x, wife: $y) isa marriage; }").asConjunction(),
                        TypeQL.parseVariable("(friend: $x, friend: $y) isa friendship").asThing());
                txn.commit();
            }
        }
        try (RocksSession session = databaseManager.session(database, Arguments.Session.Type.DATA)) {
            try (RocksTransaction txn = singleThreadElgTransaction(session, Arguments.Transaction.Type.WRITE)) {
                txn.query().insert(TypeQL.parseQuery("insert $x isa person, has name 'Zack'; $y isa person, has name 'Yasmin'; (husband: $x, wife: $y) isa marriage;").asInsert());
                txn.commit();
            }
            try (RocksTransaction txn = singleThreadElgTransaction(session, Arguments.Transaction.Type.READ)) {
                List<ConceptMap> ans = txn.query().match(TypeQL.parseQuery("match $f (friend: $p1, friend: $p2) isa friendship; $p1 has name $na;").asMatch()).toList();

                ans.iterator().forEachRemaining(a -> {
                    assertEquals("friendship", a.get("f").asThing().getType().getLabel().scopedName());
                    assertEquals("person", a.get("p1").asThing().getType().getLabel().scopedName());
                    assertEquals("person", a.get("p2").asThing().getType().getLabel().scopedName());
                    assertEquals("name", a.get("na").asAttribute().getType().getLabel().scopedName());
                });

                assertEquals(2, ans.size());
            }
        }
    }
}
