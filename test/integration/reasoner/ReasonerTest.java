/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.reasoner;

import com.vaticle.typedb.common.concurrent.NamedThreadFactory;
import com.vaticle.typedb.core.common.diagnostics.Diagnostics;
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
import com.vaticle.typedb.core.database.CoreDatabaseManager;
import com.vaticle.typedb.core.database.CoreSession;
import com.vaticle.typedb.core.database.CoreTransaction;
import com.vaticle.typedb.core.logic.LogicManager;
import com.vaticle.typedb.core.test.integration.util.Util;
import com.vaticle.typeql.lang.TypeQL;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static com.vaticle.typedb.core.common.collection.Bytes.MB;
import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ReasonerTest {

    private static final Path dataDir = Paths.get(System.getProperty("user.dir")).resolve("query-test");
    private static final Path logDir = dataDir.resolve("logs");
    private static final Database options = new Database().dataDir(dataDir).reasonerDebuggerDir(logDir)
            .storageDataCacheSize(MB).storageIndexCacheSize(MB);
    private static final String database = "reasoner-test";
    private static CoreDatabaseManager databaseMgr;

    private CoreTransaction singleThreadElgTransaction(CoreSession session, Arguments.Transaction.Type transactionType) {
        CoreTransaction transaction = session.transaction(transactionType, new Options.Transaction().infer(true));
        ActorExecutorGroup service = new ActorExecutorGroup(1, new NamedThreadFactory("typedb-actor"));
        transaction.reasoner().controllerRegistry().setExecutorService(service);
        return transaction;
    }

    @BeforeClass
    public static void beforeClass() {
        Diagnostics.Noop.initialise();
    }

    @Before
    public void setUp() throws IOException {
        Util.resetDirectory(dataDir);
        databaseMgr = CoreDatabaseManager.open(options);
        databaseMgr.create(database);
    }

    @After
    public void tearDown() {
        databaseMgr.close();
    }
    @Test
    public void test_no_rules() {
        try (CoreSession session = databaseMgr.session(database, Arguments.Session.Type.SCHEMA)) {
            try (CoreTransaction txn = singleThreadElgTransaction(session, Arguments.Transaction.Type.WRITE)) {
                ConceptManager conceptMgr = txn.concepts();

                EntityType milk = conceptMgr.putEntityType("milk");
                AttributeType ageInDays = conceptMgr.putAttributeType("age-in-days", AttributeType.ValueType.LONG);
                milk.setOwns(ageInDays);
                txn.commit();
            }
        }
        try (CoreSession session = databaseMgr.session(database, Arguments.Session.Type.DATA)) {
            try (CoreTransaction txn = singleThreadElgTransaction(session, Arguments.Transaction.Type.WRITE)) {
                txn.query().insert(TypeQL.parseQuery("insert $x isa milk, has age-in-days 5;").asInsert());
                txn.query().insert(TypeQL.parseQuery("insert $x isa milk, has age-in-days 10;").asInsert());
                txn.commit();
            }
            try (CoreTransaction txn = singleThreadElgTransaction(session, Arguments.Transaction.Type.READ)) {
                List<? extends ConceptMap> ans = txn.query().get(TypeQL.parseQuery("match $x has age-in-days $a; get;").asGet()).toList();

                ans.iterator().forEachRemaining(a -> {
                    assertEquals("age-in-days", a.getConcept("a").asThing().getType().getLabel().scopedName());
                    assertEquals("milk", a.getConcept("x").asThing().getType().getLabel().scopedName());
                });
                assertEquals(2, ans.size());
            }
        }
    }

    @Test
    public void test_offset_limit() {
        try (CoreSession session = databaseMgr.session(database, Arguments.Session.Type.SCHEMA)) {
            try (CoreTransaction txn = singleThreadElgTransaction(session, Arguments.Transaction.Type.WRITE)) {
                ConceptManager conceptMgr = txn.concepts();
                EntityType milk = conceptMgr.putEntityType("milk");
                AttributeType ageInDays = conceptMgr.putAttributeType("age-in-days", AttributeType.ValueType.LONG);
                milk.setOwns(ageInDays);
                txn.commit();
            }
        }
        try (CoreSession session = databaseMgr.session(database, Arguments.Session.Type.DATA)) {
            try (CoreTransaction txn = singleThreadElgTransaction(session, Arguments.Transaction.Type.WRITE)) {
                txn.query().insert(TypeQL.parseQuery("insert $x isa milk, has age-in-days 5;").asInsert());
                txn.query().insert(TypeQL.parseQuery("insert $x isa milk, has age-in-days 10;").asInsert());
                txn.commit();
            }
            try (CoreTransaction txn = singleThreadElgTransaction(session, Arguments.Transaction.Type.READ)) {
                List<? extends ConceptMap> ans = txn.query().get(TypeQL.parseQuery("match $x has age-in-days $a; get;").asGet()).toList();

                ans.iterator().forEachRemaining(a -> {
                    assertEquals("age-in-days", a.getConcept("a").asThing().getType().getLabel().scopedName());
                    assertEquals("milk", a.getConcept("x").asThing().getType().getLabel().scopedName());
                });
                assertEquals(2, ans.size());

                List<? extends ConceptMap> ansLimited = txn.query().get(TypeQL.parseQuery("match $x has age-in-days $a; get; limit 1;").asGet()).toList();
                assertEquals(1, ansLimited.size());

                List<? extends ConceptMap> ansLimitedOffsetted = txn.query().get(TypeQL.parseQuery("match $x has age-in-days $a; get; offset 1; limit 1;").asGet()).toList();
                assertEquals(1, ansLimitedOffsetted.size());
            }
        }
    }

    @Test
    public void test_exception_kills_query() {
        try (CoreSession session = databaseMgr.session(database, Arguments.Session.Type.SCHEMA)) {
            try (CoreTransaction txn = singleThreadElgTransaction(session, Arguments.Transaction.Type.WRITE)) {
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
                        TypeQL.parseStatement("$x has is-still-good false").asThing());
                txn.commit();
            }
        }
        try (CoreSession session = databaseMgr.session(database, Arguments.Session.Type.DATA)) {
            try (CoreTransaction txn = singleThreadElgTransaction(session, Arguments.Transaction.Type.WRITE)) {
                txn.query().insert(TypeQL.parseQuery("insert $x isa milk, has age-in-days 5;").asInsert());
                txn.query().insert(TypeQL.parseQuery("insert $x isa milk, has age-in-days 10;").asInsert());
                txn.commit();
            }
            try (CoreTransaction txn = singleThreadElgTransaction(session, Arguments.Transaction.Type.READ)) {
                RuntimeException exception = new RuntimeException();
                txn.reasoner().controllerRegistry().exception(exception);
                try {
                    List<? extends ConceptMap> ans = txn.query().get(TypeQL.parseQuery("match $x isa is-still-good; get;").asGet()).toList();
                } catch (TypeDBException e) {
                    assertEquals(e.getCause(), exception);
                    return;
                }
                fail();
            }
        }
    }

    @Test
    public void test_has_explicit_rule() {
        try (CoreSession session = databaseMgr.session(database, Arguments.Session.Type.SCHEMA)) {
            try (CoreTransaction txn = singleThreadElgTransaction(session, Arguments.Transaction.Type.WRITE)) {
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
                        TypeQL.parseStatement("$x has is-still-good false").asThing());
                txn.commit();
            }
        }

        try (CoreSession session = databaseMgr.session(database, Arguments.Session.Type.DATA)) {
            try (CoreTransaction txn = singleThreadElgTransaction(session, Arguments.Transaction.Type.WRITE)) {
                txn.query().insert(TypeQL.parseQuery("insert $x isa milk, has age-in-days 5;").asInsert());
                txn.query().insert(TypeQL.parseQuery("insert $x isa milk, has age-in-days 10;").asInsert());
                txn.query().insert(TypeQL.parseQuery("insert $x isa milk, has age-in-days 15;").asInsert());
                txn.commit();
            }
            try (CoreTransaction txn = singleThreadElgTransaction(session, Arguments.Transaction.Type.READ)) {
                List<? extends ConceptMap> ans = txn.query().get(TypeQL.parseQuery("match $x has is-still-good $a; get;").asGet()).toList();

                ans.iterator().forEachRemaining(a -> {
                    assertFalse(a.getConcept("a").asAttribute().asBoolean().getValue());
                    assertEquals("is-still-good", a.getConcept("a").asThing().getType().getLabel().scopedName());
                    assertEquals("milk", a.getConcept("x").asThing().getType().getLabel().scopedName());
                });
                assertEquals(2, ans.size());
            }
        }
    }

    @Test
    public void test_relation_rule() {
        try (CoreSession session = databaseMgr.session(database, Arguments.Session.Type.SCHEMA)) {
            try (CoreTransaction txn = singleThreadElgTransaction(session, Arguments.Transaction.Type.WRITE)) {
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
                        TypeQL.parseStatement("(friend: $x, friend: $y) isa friendship").asThing());
                txn.commit();
            }
        }
        try (CoreSession session = databaseMgr.session(database, Arguments.Session.Type.DATA)) {
            try (CoreTransaction txn = singleThreadElgTransaction(session, Arguments.Transaction.Type.WRITE)) {
                txn.query().insert(TypeQL.parseQuery("insert $x isa person, has name 'Zack'; $y isa person, has name 'Yasmin'; (husband: $x, wife: $y) isa marriage;").asInsert());
                txn.commit();
            }
            try (CoreTransaction txn = singleThreadElgTransaction(session, Arguments.Transaction.Type.READ)) {
                List<? extends ConceptMap> ans = txn.query().get(TypeQL.parseQuery("match $f (friend: $p1, friend: $p2) isa friendship; $p1 has name $na; get;").asGet()).toList();

                ans.iterator().forEachRemaining(a -> {
                    assertEquals("friendship", a.getConcept("f").asThing().getType().getLabel().scopedName());
                    assertEquals("person", a.getConcept("p1").asThing().getType().getLabel().scopedName());
                    assertEquals("person", a.getConcept("p2").asThing().getType().getLabel().scopedName());
                    assertEquals("name", a.getConcept("na").asAttribute().getType().getLabel().scopedName());
                });

                assertEquals(2, ans.size());
            }
        }
    }

    @Test
    public void test_multiple_queries_single_transaction() {
        try (CoreSession session = databaseMgr.session(database, Arguments.Session.Type.SCHEMA)) {
            try (CoreTransaction txn = singleThreadElgTransaction(session, Arguments.Transaction.Type.WRITE)) {
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
                        TypeQL.parseStatement("(friend: $x, friend: $y) isa friendship").asThing());
                txn.commit();
            }
        }
        try (CoreSession session = databaseMgr.session(database, Arguments.Session.Type.DATA)) {
            try (CoreTransaction txn = singleThreadElgTransaction(session, Arguments.Transaction.Type.WRITE)) {
                txn.query().insert(TypeQL.parseQuery("insert $x isa person, has name 'Zack'; $y isa person, has name 'Yasmin'; (husband: $x, wife: $y) isa marriage;").asInsert());
                txn.commit();
            }
            try (CoreTransaction txn = singleThreadElgTransaction(session, Arguments.Transaction.Type.READ)) {
                String queryString = "match $f (friend: $p1, friend: $p2) isa friendship; $p1 has name $na; get;";
                List<? extends ConceptMap> q1Ans = txn.query().get(TypeQL.parseQuery(queryString).asGet()).toList();
                List<? extends ConceptMap> q2Ans = txn.query().get(TypeQL.parseQuery(queryString).asGet()).toList();
                assertEquals(2, q1Ans.size());
                assertEquals(2, q2Ans.size());
            }
        }
    }
}
