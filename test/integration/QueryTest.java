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

package grakn.core.test.integration;

import grakn.core.Grakn;
import grakn.core.common.parameters.Arguments;
import grakn.core.rocks.RocksGrakn;
import graql.lang.Graql;
import graql.lang.query.GraqlInsert;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.assertNotNull;

public class QueryTest {

    private static Path directory = Paths.get(System.getProperty("user.dir")).resolve("query-test");
    private static String database = "query-test";

    @Test
    public void hello_world() throws IOException {
        Util.resetDirectory(directory);

        try (Grakn grakn = RocksGrakn.open(directory)) {
            grakn.databases().create(database);

            try (Grakn.Session session = grakn.session(database, Arguments.Session.Type.SCHEMA)) {

                try (Grakn.Transaction transaction = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    String queryStr = "define " +
                            "person sub entity, has email @key, has name, has age, plays friend; " +
                            "friendship sub relation, relates friend;";
                    GraqlInsert query = Graql.parse(queryStr);
                    transaction.query().insert(query);
                    transaction.commit();
                }

                try (Grakn.Transaction tx = session.transaction(Arguments.Transaction.Type.READ)) {
                    assertNotNull(tx.concepts().getEntityType("person"));
                    assertNotNull(tx.concepts().getRelationType("friendship"));
                }
            }
        }
    }
}
