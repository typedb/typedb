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
 */

package grakn.core.docs;

import grakn.core.common.parameters.Arguments;
import grakn.core.rocks.RocksGrakn;
import grakn.core.rocks.RocksSession;
import grakn.core.rocks.RocksTransaction;
import grakn.core.test.integration.util.Util;
import graql.lang.query.GraqlDefine;
import graql.lang.query.GraqlMatch;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import static grakn.core.common.parameters.Arguments.Session.Type.DATA;
import static grakn.core.common.parameters.Arguments.Transaction.Type.WRITE;
import static graql.lang.Graql.parseQuery;

public class Test1 {

    private static Path directory = Paths.get(System.getProperty("user.dir")).resolve("traversal-test-2");
    private static String database = "traversal-test-2";

    private static RocksGrakn grakn;
    private static RocksSession session;

    @BeforeClass
    public static void before() throws IOException {
        Util.resetDirectory(directory);
        grakn = RocksGrakn.open(directory);
        grakn.databases().create(database);

        try (RocksSession session = grakn.session(database, Arguments.Session.Type.SCHEMA)) {
            try (RocksTransaction transaction = session.transaction(WRITE)) {
                final String queryString = "define\n" +
                        "\n" +
                        "    name sub attribute,\n" +
                        "      value string;\n" +
                        "    started-at sub attribute,\n" +
                        "      value datetime;\n" +
                        "    duration sub attribute,\n" +
                        "      value long;\n" +
                        "    first-name sub attribute,\n" +
                        "      value string;\n" +
                        "    last-name sub attribute,\n" +
                        "      value string;\n" +
                        "    phone-number sub attribute,\n" +
                        "      value string;\n" +
                        "    city sub attribute,\n" +
                        "      value string;\n" +
                        "    age sub attribute,\n" +
                        "      value long;\n" +
                        "    is-customer sub attribute,\n" +
                        "      value boolean;\n" +
                        "\n" +
                        "    contract sub relation,\n" +
                        "        relates provider,\n" +
                        "        relates customer;\n" +
                        "\n" +
                        "    call sub relation,\n" +
                        "        relates caller,\n" +
                        "        relates callee,\n" +
                        "        owns started-at,\n" +
                        "        owns duration;\n" +
                        "\n" +
                        "    company sub entity,\n" +
                        "        plays contract:provider,\n" +
                        "        owns name;\n" +
                        "\n" +
                        "    person sub entity,\n" +
                        "        plays contract:customer,\n" +
                        "        plays call:caller,\n" +
                        "        plays call:callee,\n" +
                        "        owns first-name,\n" +
                        "        owns last-name,\n" +
                        "        owns phone-number,\n" +
                        "        owns city,\n" +
                        "        owns age,\n" +
                        "        owns is-customer;\n";
                final GraqlDefine query = parseQuery(queryString);
                transaction.query().define(query);
                transaction.commit();
            }
        }
        session = grakn.session(database, DATA);
    }

    @AfterClass
    public static void after() {
        session.close();
        grakn.close();
    }

    @Test
    public void test_match_that_stalls_under_java11_1() {
        try (RocksTransaction transaction = session.transaction(WRITE)) {
            final String queryString = "match\n" +
                    "$suspect isa person, has city \"London\", has age > 50;\n" +
                    "$company isa company, has name \"Telecom\";\n" +
                    "(customer: $suspect, provider: $company) isa contract;\n" +
                    "$pattern-callee isa person, has age < 20;\n" +
                    "(caller: $suspect, callee: $pattern-callee) isa call, has started-at $pattern-call-date;\n" +
                    "$target isa person, has phone-number $phone-number, has is-customer false;\n" +
                    "(caller: $suspect, callee: $target) isa call, has started-at $target-call-date;\n" +
                    "$target-call-date > $pattern-call-date;\n" +
                    "get $phone-number;";
            final GraqlMatch query = parseQuery(queryString);
            transaction.query().match(query);
            transaction.commit();
        }
    }

}