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

package com.vaticle.typedb.core.rocks;

import com.vaticle.typedb.core.TypeDB;
import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.common.parameters.Label;
import com.vaticle.typedb.core.common.parameters.Options.Database;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concept.thing.Attribute;
import com.vaticle.typedb.core.test.integration.util.Util;
import com.vaticle.typeql.lang.TypeQL;
import com.vaticle.typeql.lang.query.TypeQLQuery;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static junit.framework.TestCase.assertEquals;

public class StatisticsTest {

    private static Path dataDir = Paths.get(System.getProperty("user.dir")).resolve("statistics-test");
    private static final Path logDir = dataDir.resolve("logs");
    private static final Database options = new Database().dataDir(dataDir).reasonerDebuggerDir(logDir);
    private static String database = "statistics-test";

    @Test
    public void test_statistics() throws IOException {
        Util.resetDirectory(dataDir);
        try (RocksTypeDB typedb = RocksTypeDB.open(options)) {
            typedb.databases().create(database);
            setupSchema(typedb);
            int personCount = 1000;
            Set<Long> ages = new HashSet<>();
            Random random = new Random(0);
            insertPersonAndAges(typedb, personCount, ages, random);
            assertStatistics(typedb, personCount, ages);
            updateAges(typedb, ages);
            assertStatistics(typedb, personCount, ages);
        }
    }

    private void updateAges(RocksTypeDB typedb, Set<Long> ages) {
        try (RocksSession session = typedb.session(database, Arguments.Session.Type.DATA)) {
            try (RocksTransaction tx = session.transaction(Arguments.Transaction.Type.WRITE)) {
                TypeQLQuery query = TypeQL.parseQuery("match $x isa person, has age $y;");
                List<ConceptMap> conceptMaps = tx.query().match(query.asMatch()).toList();
                conceptMaps.forEach(cm -> {
                    Attribute.Long attribute = cm.get("y").asAttribute().asLong();
                    cm.get("x").asEntity().unsetHas(attribute);
                    long newAge = attribute.getValue() + 1;
                    Attribute.Long newAttribute = attribute.getType().asLong().put(newAge);
                    cm.get("x").asEntity().setHas(newAttribute);
                    cm.get("x").asEntity().setHas(newAttribute);
                    ages.add(newAge);
                });
                tx.commit();
            }
        }
    }

    private void insertPersonAndAges(RocksTypeDB typedb, int personCount, Set<Long> ages, Random random) {
        try (RocksSession session = typedb.session(database, Arguments.Session.Type.DATA)) {
            try (RocksTransaction tx = session.transaction(Arguments.Transaction.Type.WRITE)) {
                for (int i = 0; i < personCount; i++) {
                    long age = random.nextInt(personCount);
                    ages.add(age);
                    TypeQLQuery query = TypeQL.parseQuery("insert $x isa person, has age " + age + ";").asInsert();
                    tx.query().insert(query.asInsert());
                }
                tx.commit();
            }
        }
    }

    private void assertStatistics(RocksTypeDB typedb, int personCount, Set<Long> ages) {
        waitForStatisticsCounter();
        try (RocksSession session = typedb.session(database, Arguments.Session.Type.DATA)) {
            try (RocksTransaction tx = session.transaction(Arguments.Transaction.Type.READ)) {
                assertEquals(personCount, tx.graphMgr.data().stats().thingVertexCount(Label.of("person")));
                assertEquals(ages.size(), tx.graphMgr.data().stats().thingVertexCount(Label.of("age")));
                assertEquals(personCount, tx.graphMgr.data().stats().hasEdgeCount(Label.of("person"), Label.of("age")));
            }
        }
    }

    private void setupSchema(RocksTypeDB typedb) {
        try (TypeDB.Session session = typedb.session(database, Arguments.Session.Type.SCHEMA)) {
            try (TypeDB.Transaction tx = session.transaction(Arguments.Transaction.Type.WRITE)) {
                TypeQLQuery query = TypeQL.parseQuery("" +
                                                              "define " +
                                                              "person sub entity, owns age; " +
                                                              "age sub attribute, value long; " +
                                                              "");
                tx.query().define(query.asDefine());
                tx.commit();
            }
        }
    }

    private void waitForStatisticsCounter() {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
