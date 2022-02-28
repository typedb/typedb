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
 *
 */

package com.vaticle.typedb.core.database;

import com.vaticle.typedb.core.TypeDB;
import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.common.parameters.Label;
import com.vaticle.typedb.core.common.parameters.Options.Database;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concept.thing.Attribute;
import com.vaticle.typedb.core.graph.ThingGraph;
import com.vaticle.typedb.core.test.integration.util.Util;
import com.vaticle.typeql.lang.TypeQL;
import com.vaticle.typeql.lang.query.TypeQLQuery;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static com.vaticle.typedb.core.common.collection.Bytes.MB;
import static junit.framework.TestCase.assertEquals;

public class StatisticsTest {

    private static Path dataDir = Paths.get(System.getProperty("user.dir")).resolve("statistics-test");
    private static final Path logDir = dataDir.resolve("logs");
    private static final Database options = new Database().dataDir(dataDir).reasonerDebuggerDir(logDir)
            .storageDataCacheSize(MB).storageIndexCacheSize(MB);
    private static final String database = "statistics-test";

    private CoreDatabaseManager databaseMgr;

    @Before
    public void setup() throws IOException {
        Util.resetDirectory(dataDir);
        databaseMgr = CoreDatabaseManager.open(options);
        databaseMgr.create(database);
        try (TypeDB.Session session = databaseMgr.session(database, Arguments.Session.Type.SCHEMA)) {
            try (TypeDB.Transaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
                txn.query().define(TypeQL.parseQuery("define " +
                        "person sub entity, owns name, plays friendship:friend, plays employment:employee;" +
                        "friendship sub relation, relates friend;" +
                        "name sub attribute, value string;" +
                        "company sub entity, owns address @key, plays employment:employer;" +
                        "employment sub relation, relates employer, relates employee;" +
                        "address sub attribute, value string;").asDefine());
                txn.commit();
            }
        }
    }

    @After
    public void tearDown() {
        databaseMgr.close();
    }

    @Test
    public void nonconcurrent_statistics() {
        int batches = 10;
        try (CoreSession session = databaseMgr.session(database, Arguments.Session.Type.DATA)) {
            for (int i = 0; i < batches; i++) {
                try (CoreTransaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    txn.query().insert(TypeQL.parseQuery("insert " +
                            "$x isa person, has name 'name-" + i + "';" +
                            "$clone isa person, has name 'name-" + i + "';" +
                            "$y isa person, has name 'name-" + (i + batches) + "';" +
                            "(friend: $x, friend: $y) isa friendship;" +
                            "$c isa company, has address 'Margaret Street Nr. " + i + "';" +
                            "(employee: $x, employer: $c) isa employment;"));
                    txn.commit();
                }
            }

            try (CoreTransaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
                ThingGraph.Statistics statistics = txn.graphMgr.data().stats();

                assertEquals(batches * 3, statistics.thingVertexCount(Label.of("person")));
                assertEquals(batches * 3, statistics.hasEdgeCount(Label.of("person"), Label.of("name")));
                assertEquals(batches * 2, statistics.thingVertexCount(Label.of("name"))); // because of clone person
                assertEquals(batches, statistics.thingVertexCount(Label.of("friendship")));
                assertEquals(batches * 2, statistics.thingVertexCount(Label.of("friend", "friendship")));
                assertEquals(batches, statistics.thingVertexCount(Label.of("company")));
                assertEquals(batches, statistics.thingVertexCount(Label.of("address")));
                assertEquals(batches, statistics.hasEdgeCount(Label.of("company"), Label.of("address")));
                assertEquals(batches, statistics.thingVertexCount(Label.of("employment")));
                assertEquals(batches, statistics.thingVertexCount(Label.of("employee", "employment")));
                assertEquals(batches, statistics.thingVertexCount(Label.of("employer", "employment")));
            }
        }
    }
//
//    @Test
//    public void test_statistics() throws IOException {
//        Util.resetDirectory(dataDir);
//        try (CoreDatabaseManager databaseMgr = CoreDatabaseManager.open(options)) {
//            databaseMgr.create(database);
//            loadSchema(databaseMgr);
//            int personCount = 1000;
//            Set<Long> ages = new HashSet<>();
//            Random random = new Random(0);
//            insertPersonAndAges(databaseMgr, personCount, ages, random);
//            assertStatistics(databaseMgr, personCount, ages);
//            updateAges(databaseMgr, ages);
//            assertStatistics(databaseMgr, personCount, ages);
//        }
//    }
//
//    private void updateAges(CoreDatabaseManager databaseMgr, Set<Long> ages) {
//        try (CoreSession session = databaseMgr.session(database, Arguments.Session.Type.DATA)) {
//            try (CoreTransaction tx = session.transaction(Arguments.Transaction.Type.WRITE)) {
//                TypeQLQuery query = TypeQL.parseQuery("match $x isa person, has age $y;");
//                List<ConceptMap> conceptMaps = tx.query().match(query.asMatch()).toList();
//                conceptMaps.forEach(cm -> {
//                    Attribute.Long attribute = cm.get("y").asAttribute().asLong();
//                    cm.get("x").asEntity().unsetHas(attribute);
//                    long newAge = attribute.getValue() + 1;
//                    Attribute.Long newAttribute = attribute.getType().asLong().put(newAge);
//                    cm.get("x").asEntity().setHas(newAttribute);
//                    cm.get("x").asEntity().setHas(newAttribute);
//                    ages.add(newAge);
//                });
//                tx.commit();
//            }
//        }
//    }
//
//    private void insertPersonAndAges(CoreDatabaseManager databaseMgr, int personCount, Set<Long> ages, Random random) {
//        try (CoreSession session = databaseMgr.session(database, Arguments.Session.Type.DATA)) {
//            try (CoreTransaction tx = session.transaction(Arguments.Transaction.Type.WRITE)) {
//                for (int i = 0; i < personCount; i++) {
//                    long age = random.nextInt(personCount);
//                    ages.add(age);
//                    TypeQLQuery query = TypeQL.parseQuery("insert $x isa person, has age " + age + ";").asInsert();
//                    tx.query().insert(query.asInsert());
//                }
//                tx.commit();
//            }
//        }
//    }
//
//    private void assertStatistics(CoreDatabaseManager databaseMgr, int personCount, Set<Long> ages) {
//        waitForStatisticsCounter();
//        try (CoreSession session = databaseMgr.session(database, Arguments.Session.Type.DATA)) {
//            try (CoreTransaction tx = session.transaction(Arguments.Transaction.Type.READ)) {
//                assertEquals(personCount, tx.graphMgr.data().stats().thingVertexCount(Label.of("person")));
//                assertEquals(ages.size(), tx.graphMgr.data().stats().thingVertexCount(Label.of("age")));
//                assertEquals(personCount, tx.graphMgr.data().stats().hasEdgeCount(Label.of("person"), Label.of("age")));
//            }
//        }
//    }
//
//    private void loadSchema(CoreDatabaseManager databaseMgr) {
//        try (TypeDB.Session session = databaseMgr.session(database, Arguments.Session.Type.SCHEMA)) {
//            try (TypeDB.Transaction tx = session.transaction(Arguments.Transaction.Type.WRITE)) {
//                TypeQLQuery query = TypeQL.parseQuery("define " +
//                        "person sub entity, owns age; " +
//                        "age sub attribute, value long; " +
//                        "");
//                tx.query().define(query.asDefine());
//                tx.commit();
//            }
//        }
//    }
//
//    private void waitForStatisticsCounter() {
//        try {
//            Thread.sleep(1000);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//    }
}
