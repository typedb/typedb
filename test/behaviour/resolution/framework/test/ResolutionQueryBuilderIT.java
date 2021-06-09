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

package com.vaticle.typedb.core.test.behaviour.resolution.framework.test;

import com.vaticle.typedb.core.TypeDB;
import com.vaticle.typedb.core.TypeDB.Transaction;
import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.common.parameters.Options;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.pattern.variable.Variable;
import com.vaticle.typedb.core.rocks.RocksTypeDB;
import com.vaticle.typedb.core.test.behaviour.resolution.framework.common.TypeQLHelpers;
import com.vaticle.typedb.core.test.integration.util.Util;
import com.vaticle.typeql.lang.TypeQL;
import com.vaticle.typeql.lang.pattern.variable.BoundVariable;
import com.vaticle.typeql.lang.query.TypeQLMatch;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import java.util.stream.Collectors;

import static com.vaticle.typedb.core.test.behaviour.resolution.framework.test.LoadTest.loadComplexRecursionTest;
import static org.junit.Assert.assertEquals;


public class ResolutionQueryBuilderIT {

    private static final String database = "complex-recursion";
    private static final Path dataDir = Paths.get(System.getProperty("user.dir")).resolve(database);
    private static final Path logDir = dataDir.resolve("logs");
    private static final Options.Database options = new Options.Database().dataDir(dataDir).logsDir(logDir);

    @Test
    public void testKeysVariablesAreGeneratedCorrectly() throws IOException {
        Util.resetDirectory(dataDir);
        TypeQLMatch inferenceQuery = TypeQL.parseQuery("match $transaction isa transaction, has currency $currency;").asMatch();

        try (TypeDB typedb = RocksTypeDB.open(options)) {
            typedb.databases().create(database);

            Set<Variable> keyVariables;

            loadComplexRecursionTest(typedb, database);

            try (TypeDB.Session session = typedb.session(database, Arguments.Session.Type.DATA)) {
                try (Transaction tx = session.transaction(Arguments.Transaction.Type.READ)) {
                    ConceptMap answer = tx.query().match(inferenceQuery).next();
                    keyVariables = TypeQLHelpers.generateKeyVariables(answer.concepts());
                }
            }

            Set<BoundVariable> expectedVariables = TypeQL.parsePattern(
                    "$currency \"GBP\" isa currency;\n" +
                            "$transaction has currency \"GBP\";\n"
            ).asConjunction().variables().collect(Collectors.toSet());

            assertEquals(expectedVariables, keyVariables);
        }
    }
}