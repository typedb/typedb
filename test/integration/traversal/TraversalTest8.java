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

package grakn.core.traversal;

import grakn.core.common.iterator.ResourceIterator;
import grakn.core.common.parameters.Arguments;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.rocks.RocksGrakn;
import grakn.core.rocks.RocksSession;
import grakn.core.rocks.RocksTransaction;
import grakn.core.server.migrator.Importer;
import grakn.core.test.integration.util.Util;
import graql.lang.query.GraqlDefine;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;

import static grakn.common.collection.Collections.list;
import static grakn.core.common.parameters.Arguments.Session.Type.DATA;
import static grakn.core.common.parameters.Arguments.Transaction.Type.READ;
import static grakn.core.common.parameters.Arguments.Transaction.Type.WRITE;
import static graql.lang.Graql.parseQuery;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertTrue;

public class TraversalTest8 {
    private static Path directory = Paths.get(System.getProperty("user.dir")).resolve("world2");
    private static String database = "world2";

    @BeforeClass
    public static void before() throws IOException {
        Util.resetDirectory(directory);
        try (RocksGrakn grakn = RocksGrakn.open(directory)) {
            grakn.databases().create(database);

            try (RocksSession session = grakn.session(database, Arguments.Session.Type.SCHEMA)) {
                try (RocksTransaction transaction = session.transaction(WRITE)) {
                    final GraqlDefine query = parseQuery(new String(Files.readAllBytes(Paths.get("test/integration/traversal/TraversalTest8/world2.gql")), UTF_8));
                    transaction.query().define(query);
                    transaction.commit();
                }
            }

            Importer importer = new Importer(grakn, database, Paths.get("test/integration/traversal/TraversalTest8/world2.data"), new HashMap<>());
            importer.run();
        }
    }

    @Test
    public void test_repeat() {
        int rep = 30, success = 0, fail = 0;
        List<String> cities = list("London", "New York", "Berlin", "Brasilia", "Cape Town", "Beijing", "Canberra");
        for (int i = 0; i < rep; i++) {
            Instant start = Instant.now();
            try {
                test();
                success++;
                Instant end = Instant.now();
                System.out.println(String.format("Duration: %s (ms)", Duration.between(start, end).toMillis()));
            } catch (AssertionError e) {
                fail++;
                System.out.println("FAILED!");
                e.printStackTrace();
                break;
            }
        }
        System.out.println(String.format("Success: %s, Fail: %s", success, fail));
    }

    @Test
    public void test() {
        try (RocksGrakn grakn = RocksGrakn.open(directory)) {
            try (RocksSession session = grakn.session(database, DATA)) {
                try (RocksTransaction transaction = session.transaction(READ)) {
                    String query = "match " +
                            "$city isa city, has location-name 'Canberra'; " +
                            "$m (husband: $husband, wife: $wife) isa marriage, has marriage-id $marriage-id; " +
                            "$husband isa person, has email $husband-email; " +
                            "$wife isa person, has email $wife-email; " +
                            "(located: $m, location: $city) isa locates;";
                    ResourceIterator<ConceptMap> answers = transaction.query().match(parseQuery(query).asMatch());
                    assertTrue(!answers.hasNext());
                }
            }
        }
    }

}
