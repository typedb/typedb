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

package grakn.core.rocks;

import grakn.core.Grakn;
import grakn.core.common.parameters.Arguments;
import grakn.core.common.parameters.Label;
import grakn.core.test.integration.util.Util;
import graql.lang.Graql;
import graql.lang.query.GraqlDefine;
import graql.lang.query.GraqlInsert;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import static junit.framework.TestCase.assertEquals;

public class StatisticsTest {

    private static Path directory = Paths.get(System.getProperty("user.dir")).resolve("statistics-test");
    private static String database = "statistics-test";

    @Test
    public void test_statistics() throws IOException {
        Util.resetDirectory(directory);
        try (RocksGrakn grakn = RocksGrakn.open(directory)) {
            grakn.databases().create(database);
            try (Grakn.Session session = grakn.session(database, Arguments.Session.Type.SCHEMA)) {
                try (Grakn.Transaction tx = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    GraqlDefine query = Graql.parseQuery("" +
                            "define " +
                            "person sub entity, owns age; " +
                            "age sub attribute, value long; " +
                            "");
                    tx.query().define(query);
                    tx.commit();
                }
            }
            int personCount = 1000;
            Random random = new Random(0);
            Set<Long> ages = new HashSet<>();
            try (RocksSession session = grakn.session(database, Arguments.Session.Type.DATA)) {
                try (RocksTransaction tx = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    for (int i = 0; i < personCount; i++) {
                        long age = random.nextLong() % personCount;
                        ages.add(age);
                        GraqlInsert query = Graql.parseQuery("insert $x isa person, has age " + age + ";").asInsert();
                        tx.query().insert(query);
                    }
                    tx.commit();
                }
            }
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            try (RocksSession session = grakn.session(database, Arguments.Session.Type.DATA)) {
                try (RocksTransaction tx = session.transaction(Arguments.Transaction.Type.READ)) {
                    assertEquals(tx.graphMgr.data().stats().thingVertexCount(Label.of("person")), personCount);
                    assertEquals(tx.graphMgr.data().stats().thingVertexCount(Label.of("age")), ages.size());
                    assertEquals(tx.graphMgr.data().stats().hasEdgeCount(Label.of("person"), Label.of("age")), personCount);
                }
            }
        }
    }
}
