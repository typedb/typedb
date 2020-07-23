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

package grakn.core.test.behaviour.resolution.framework.test;

import grakn.core.concept.answer.ConceptMap;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.test.behaviour.resolution.framework.complete.Completer;
import grakn.core.test.behaviour.resolution.framework.complete.SchemaManager;
import grakn.core.test.rule.GraknTestServer;
import graql.lang.Graql;
import graql.lang.query.GraqlGet;
import graql.lang.statement.Statement;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.List;
import java.util.Set;

import static grakn.core.test.behaviour.resolution.framework.common.Utils.getStatements;
import static grakn.core.test.behaviour.resolution.framework.test.LoadTest.loadTestStub;
import static org.junit.Assert.assertEquals;

public class TestCompleter {

    @ClassRule
    public static final GraknTestServer graknTestServer = new GraknTestServer();

    @Test
    public void testValidResolutionHasExactlyOneAnswer() {
        Set<Statement> expectedResolutionStatements = getStatements(Graql.parsePatternList("" +
                "$r0-com isa company;" +
                "$r0-com has is-liable $r0-lia;" +
                "$r0-com has company-id 0;" +
                "$r0-lia true;" +
                "$r1-c2 isa company, has name $r1-n2;" +
                "$r1-n2 \"the-company\";" +
                "$r1-l2 true;" +
                "$r1-c2 has is-liable $r1-l2;" +
                "$x0 (instance: $r1-c2) isa isa-property, has type-label \"company\";" +
                "$x1 (owned: $r1-n2, owner: $r1-c2) isa has-attribute-property;" +
                "$x2 (owned: $r1-l2, owner: $r1-c2) isa has-attribute-property;" +
                "$_ (body: $x0, body: $x1, head: $x2) isa resolution, has rule-label \"company-is-liable\";" +
                "$r1-n2 == \"the-company\";" +
                "$r1-c2 has name $r1-n2;" +
                "$r1-c2 isa company;" +
                "$r1-c2 has company-id 0;" +
                "$r2-c1 isa company;" +
                "$r2-n1 \"the-company\";" +
                "$r2-c1 has name $r2-n1;" +
                "$x3 (instance: $r2-c1) isa isa-property, has type-label \"company\";" +
                "$x4 (owned: $r2-n1, owner: $r2-c1) isa has-attribute-property;" +
                "$_ (body: $x3, head: $x4) isa resolution, has rule-label \"company-has-name\";" +
                "$r2-c1 has company-id 0;"
        ));

        try (Session session = graknTestServer.sessionWithNewKeyspace()) {

            loadTestStub(session, "basic_recursion");

            Completer completer = new Completer(session);
            try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
                completer.loadRules(SchemaManager.getAllRules(tx));
            }

            SchemaManager.undefineAllRules(session);
            SchemaManager.addResolutionSchema(session);
            SchemaManager.connectResolutionSchema(session);
            completer.complete();

            try (Transaction tx = session.transaction(Transaction.Type.READ)) {
                List<ConceptMap> answers = tx.execute(Graql.match(expectedResolutionStatements).get());

                assertEquals(answers.size(), 1);
            }
        }
    }

    @Test
    public void testDeduplicationOfInferredConcepts() {
        try (Session session = graknTestServer.sessionWithNewKeyspace()) {

            loadTestStub(session, "transitivity");

            Completer completer = new Completer(session);
            try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
                completer.loadRules(SchemaManager.getAllRules(tx));
            }

            SchemaManager.undefineAllRules(session);
            SchemaManager.addResolutionSchema(session);
            SchemaManager.connectResolutionSchema(session);
            completer.complete();

            try (Transaction tx = session.transaction(Transaction.Type.READ)) {
                GraqlGet inferredAnswersQuery = Graql.match(Graql.var("lh").isa("location-hierarchy")).get();
                List<ConceptMap> inferredAnswers = tx.execute(inferredAnswersQuery);
                assertEquals(6, inferredAnswers.size());

                GraqlGet resolutionsQuery = Graql.match(Graql.var("res").isa("resolution")).get();
                List<ConceptMap> resolutionAnswers = tx.execute(resolutionsQuery);
                assertEquals(4, resolutionAnswers.size());
            }
        }
    }
}