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

import grakn.core.common.parameters.Arguments;
import grakn.core.rocks.RocksGrakn;
import grakn.core.rocks.RocksSession;
import grakn.core.rocks.RocksTransaction;
import grakn.core.server.migrator.Importer;
import grakn.core.test.integration.util.Util;
import graql.lang.Graql;
import graql.lang.query.GraqlDefine;
import graql.lang.query.GraqlMatch;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;

import static grakn.common.collection.Collections.list;
import static grakn.core.common.parameters.Arguments.Session.Type.DATA;
import static grakn.core.common.parameters.Arguments.Transaction.Type.READ;
import static grakn.core.common.parameters.Arguments.Transaction.Type.WRITE;
import static graql.lang.Graql.parseQuery;
import static java.nio.charset.StandardCharsets.UTF_8;

public class TraversalTest8 {
    private static Path directory = Paths.get(System.getProperty("user.dir")).resolve("world2");
    private static String database = "world2";
    private static RocksGrakn grakn;
    private static RocksSession session;

    @Test
    public void test() throws IOException {
        for (int i = 0; i < 30; i++)
        {
            Util.resetDirectory(directory);
            grakn = RocksGrakn.open(directory);
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

            session = grakn.session(database, DATA);

            long start = System.currentTimeMillis();
            List<String> cities = list(
                    "London",
                    "New York",
                    "Berlin",
                    "Brasilia",
                    "Cape Town",
                    "Beijing",
                    "Canberra"
            );
            cities.parallelStream().forEach(city -> {
                try (RocksTransaction transaction = session.transaction(READ)) {
                    GraqlMatch query = Graql.match(
                            Graql.var("city").isa("city")
                                    .has("location-name", city),
                            Graql.var("m")
                                    .rel("husband", Graql.var("husband"))
                                    .rel("wife", Graql.var("wife"))
                                    .isa("marriage")
                                    .has("marriage-id", Graql.var("marriage-id")),
                            Graql.not(
                                    Graql.var("par")
                                            .rel("parent", "husband")
                                            .rel("parent", "wife")
                                            .isa("parentship")
                            ),
                            Graql.var("husband").isa("person")
                                    .has("email", Graql.var("husband-email")),
                            Graql.var("wife").isa("person")
                                    .has("email", Graql.var("wife-email")),
                            Graql.var()
                                    .rel("located", Graql.var("m"))
                                    .rel("location", Graql.var("city"))
                                    .isa("locates")
                    ).sort("marriage-id");
                    transaction.query().match(query).toList();
                }
            });

            System.out.println(System.currentTimeMillis() - start);

            session.close();
            grakn.close();
        }
    }
}
