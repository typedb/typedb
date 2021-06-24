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

import com.vaticle.typedb.core.TypeDB.Transaction;
import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.common.parameters.Options;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.rocks.RocksSession;
import com.vaticle.typedb.core.rocks.RocksTypeDB;
import com.vaticle.typedb.core.test.behaviour.resolution.framework.reference.Reasoner;
import com.vaticle.typedb.core.test.integration.util.Util;
import com.vaticle.typeql.lang.TypeQL;
import com.vaticle.typeql.lang.pattern.Pattern;
import com.vaticle.typeql.lang.query.TypeQLMatch;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static com.vaticle.typedb.core.test.behaviour.resolution.framework.test.LoadTest.loadBasicRecursionExample;
import static com.vaticle.typedb.core.test.behaviour.resolution.framework.test.LoadTest.loadTransitivityExample;
import static org.junit.Assert.assertEquals;

public class TestReasoner {

    private static final String database = "TestReasoner";
    private static final Path dataDir = Paths.get(System.getProperty("user.dir")).resolve(database);
    private static final Path logDir = dataDir.resolve("logs");
    private static final Options.Database options = new Options.Database().dataDir(dataDir).logsDir(logDir);
    private RocksTypeDB typeDB;

    @Before
    public void setUp() throws IOException {
        Util.resetDirectory(dataDir);
        this.typeDB = RocksTypeDB.open(options);
        this.typeDB.databases().create(database);
    }

    @After
    public void tearDown() {
        this.typeDB.close();
    }

    @Test
    public void testValidResolutionHasExactlyOneAnswer() {
        Pattern expectedResolutionVariables = TypeQL.parsePattern("" +
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
        );

        loadBasicRecursionExample(typeDB, database);

        try (RocksSession session = typeDB.session(database, Arguments.Session.Type.DATA)) {
            Reasoner completer = Reasoner.runRules(session);
            try (Transaction tx = session.transaction(Arguments.Transaction.Type.READ)) {
                List<ConceptMap> answers = tx.query().match(TypeQL.match(expectedResolutionVariables)).toList();
                assertEquals(answers.size(), 1);
            }
        }
    }

    @Test
    public void testDeduplicationOfInferredConcepts() {
        loadTransitivityExample(typeDB, database);
        try (RocksSession session = typeDB.session(database, Arguments.Session.Type.DATA)) {
            Reasoner completer = Reasoner.runRules(session);
            try (Transaction tx = session.transaction(Arguments.Transaction.Type.READ)) {
                TypeQLMatch inferredAnswersQuery = TypeQL.match(TypeQL.var("lh").isa("location-hierarchy"));
                List<ConceptMap> inferredAnswers = tx.query().match(inferredAnswersQuery).toList();
                assertEquals(6, inferredAnswers.size());

                TypeQLMatch resolutionsQuery = TypeQL.match(TypeQL.var("res").isa("resolution"));
                List<ConceptMap> resolutionAnswers = tx.query().match(resolutionsQuery).toList();
                assertEquals(4, resolutionAnswers.size());
            }
        }
    }
}