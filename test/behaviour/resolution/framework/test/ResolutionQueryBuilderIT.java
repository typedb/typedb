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

import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.TypeDB.Session;
import com.vaticle.typedb.core.TypeDB.Transaction;;
import com.vaticle.typedb.core.test.behaviour.resolution.framework.common.TypeQLHelpers;
import com.vaticle.typedb.core.test.rule.GraknTestServer;
import com.vaticle.typeql.lang.TypeQL;
import com.vaticle.typeql.lang.query.TypeQLMatch;
import com.vaticle.typeql.lang.statement.Statement;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Set;

import static com.vaticle.typedb.core.test.behaviour.resolution.framework.common.Utils.getStatements;
import static com.vaticle.typedb.core.test.behaviour.resolution.framework.test.LoadTest.loadTestStub;
import static org.junit.Assert.assertEquals;

public class ResolutionQueryBuilderIT {

    @ClassRule
    public static final GraknTestServer graknTestServer = new GraknTestServer();

    @Test
    public void testKeysStatementsAreGeneratedCorrectly() {
        TypeQLMatch inferenceQuery = TypeQL.parse("match $transaction isa transaction, has currency $currency;");

        Set<Statement> keyStatements;

        try (Session session = graknTestServer.sessionWithNewKeyspace()) {

            loadTestStub(session, "complex_recursion");

            try (Transaction tx = session.transaction(Arguments.Transaction.Type.READ)) {
                ConceptMap answer = tx.execute(inferenceQuery).get(0);
                keyStatements = TypeQLHelpers.generateKeyStatements(answer.map());
            }
        }

        Set<Statement> expectedStatements = getStatements(TypeQL.parsePatternList(
                "$currency \"GBP\" isa currency;\n" +
                "$transaction has currency \"GBP\";\n"

        ));

        assertEquals(expectedStatements, keyStatements);
    }
}