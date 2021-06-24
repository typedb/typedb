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
import com.vaticle.typedb.core.rocks.RocksSession;
import com.vaticle.typedb.core.rocks.RocksTypeDB;
import com.vaticle.typedb.core.test.behaviour.resolution.framework.CorrectnessChecker;
import com.vaticle.typedb.core.test.integration.util.Util;
import com.vaticle.typeql.lang.TypeQL;
import com.vaticle.typeql.lang.query.TypeQLMatch;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;

import static com.vaticle.typedb.core.common.test.Util.assertThrows;
import static com.vaticle.typedb.core.test.behaviour.resolution.framework.CompletenessChecker.CompletenessException;
import static com.vaticle.typedb.core.test.behaviour.resolution.framework.SoundnessChecker.SoundnessException;
import static com.vaticle.typedb.core.test.behaviour.resolution.framework.test.LoadTest.loadBasicRecursionExample;
import static com.vaticle.typedb.core.test.behaviour.resolution.framework.test.LoadTest.loadComplexRecursionExample;
import static com.vaticle.typedb.core.test.behaviour.resolution.framework.test.LoadTest.loadEmployableExample;
import static com.vaticle.typedb.core.test.behaviour.resolution.framework.test.LoadTest.loadTransitivityExample;

public class TestCorrectnessChecker {

    private static final String database = "TestCorrectnessChecker";
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
    public void testResolutionPassesForTransitivity() {
        TypeQLMatch inferenceQuery = TypeQL.parseQuery("" +
                "match $lh (superior: $continent, subordinate: $area) isa location-hierarchy; " +
                "$continent isa continent; " +
                "$area isa area;").asMatch();
        loadTransitivityExample(typeDB, database);
        testCorrectness(inferenceQuery);
    }

    @Test
    public void testResolutionThrowsWhenRuleIsNotTriggeredTransitivityExample() {
        TypeQLMatch inferenceQuery = TypeQL.parseQuery("" +
                "match $lh (superior: $continent, location-hierarchy_subordinate: $area) isa location-hierarchy; " +
                "$continent isa continent; " +
                "$area isa area;").asMatch();
        loadTransitivityExample(typeDB, database);
        // Undefine a rule in the keyspace under test such that the expected facts will not be inferred
        try (RocksSession session = typeDB.session(database, Arguments.Session.Type.SCHEMA)) {
            try (Transaction tx = session.transaction(Arguments.Transaction.Type.WRITE)) {
                tx.query().undefine(TypeQL.undefine(TypeQL.type("location-hierarchy-transitivity").sub("rule")));
                tx.commit();
            }
        }
        try (RocksSession session = typeDB.session(database, Arguments.Session.Type.DATA)) {
            CorrectnessChecker resolution = CorrectnessChecker.initialise(session);
            assertThrows(CompletenessException.class, () -> resolution.checkCompleteness(inferenceQuery));
        }
    }

    @Test
    public void testSoundnessThrowsWhenRuleTriggersTooOftenEmployableExample() {
        TypeQLMatch inferenceQuery = TypeQL.parseQuery("match $x has employable true;").asMatch();
        loadEmployableExample(typeDB, database);

        CorrectnessChecker resolution;
        try (RocksSession session = typeDB.session(database, Arguments.Session.Type.DATA)) {
            resolution = CorrectnessChecker.initialise(session);
        }
        // Undefine a rule in the database under test such that the expected facts will not be inferred
        try (RocksSession schemaSession = typeDB.session(database, Arguments.Session.Type.SCHEMA)) {
            try (Transaction tx = schemaSession.transaction(Arguments.Transaction.Type.WRITE)) {
                tx.query().undefine(TypeQL.undefine(Collections.singletonList(
                        TypeQL.rule("people-are-employable"))));
                tx.query().define(TypeQL.parseQuery("define\n" +
                                                            "rule people-are-employable:\n" +
                                                            "when {\n" +
                                                            "    $p isa animal;\n" +
                                                            "} then {\n" +
                                                            "    $p has employable true;\n" +
                                                            "};").asDefine());
                tx.commit();
            }
        }
        try (RocksSession session2 = typeDB.session(database, Arguments.Session.Type.DATA)) {
            assertThrows(SoundnessException.class, () -> resolution.checkSoundness(session2, inferenceQuery));
        }
    }

    @Test
    public void testSoundnessThrowsWhenRuleTriggersTooOftenTransitivityExample() {
        TypeQLMatch inferenceQuery = TypeQL.parseQuery("" +
                "match $lh ($continent, $area) isa location-hierarchy; " +
                "$continent isa continent; " +
                "$area isa area;").asMatch();
        loadTransitivityExample(typeDB, database);
        CorrectnessChecker resolution;
        try (RocksSession session = typeDB.session(database, Arguments.Session.Type.DATA)) {
            resolution = CorrectnessChecker.initialise(session);
        }
        // Undefine a rule in the database under test such that the expected facts will not be inferred
        try (RocksSession schemaSession = typeDB.session(database, Arguments.Session.Type.SCHEMA)) {
            try (Transaction tx = schemaSession.transaction(Arguments.Transaction.Type.WRITE)) {
                tx.query().undefine(TypeQL.undefine(Collections.singletonList(
                        TypeQL.rule("location-hierarchy-transitivity"))));
                tx.query().define(TypeQL.parseQuery("define\n" +
                                                            "rule location-hierarchy-transitivity:\n" +
                                                            "when {\n" +
                                                            "  ($a, $b) isa location-hierarchy;\n" +
                                                            "  ($b, $c) isa location-hierarchy;\n" +
                                                            "  not { $a is $c; };\n" +
                                                            "} then {\n" +
                                                            "  (superior: $a, subordinate: $c) isa location-hierarchy;\n" +
                                                            "};").asDefine());
                tx.commit();
            }
        }
        try (RocksSession session2 = typeDB.session(database, Arguments.Session.Type.DATA)) {
            assertThrows(SoundnessException.class, () -> resolution.checkSoundness(session2, inferenceQuery));
        }
    }

    @Test
    public void testResolutionThrowsWhenRuleTriggersTooOftenAndResultCountIsIncorrectTransitivityExample() {
        TypeQLMatch inferenceQuery = TypeQL.parseQuery("" +
                "match $lh ($continent, " +
                "$area) isa location-hierarchy; " +
                "$continent isa continent; " +
                "$area isa area;").asMatch();

        loadTransitivityExample(typeDB, database);

        // Undefine a rule in the keyspace under test such that the expected facts will not be inferred
        try (RocksSession session = typeDB.session(database, Arguments.Session.Type.DATA)) {
            Transaction tx = session.transaction(Arguments.Transaction.Type.WRITE);
            tx.query().undefine(TypeQL.undefine(Collections.singletonList(
                    TypeQL.rule("location-hierarchy-transitivity"))));
            tx.query().define(TypeQL.parseQuery("define" +
                                                "rule location-hierarchy-transitivity:\n" +
                                                "when {\n" +
                                                "  ($a, $b) isa location-hierarchy;\n" +
                                                "  ($b, $c) isa location-hierarchy;\n" +
                                                "  $a != $c;\n" +
                                                "} then {\n" +
                                                "  (superior: $a, subordinate: $c) isa location-hierarchy;\n" +
                                                "};").asDefine());
            tx.commit();
        }
        try (RocksSession session = typeDB.session(database, Arguments.Session.Type.DATA)) {
            CorrectnessChecker resolution = CorrectnessChecker.initialise(session);
            assertThrows(CompletenessException.class, () -> resolution.checkCompleteness(inferenceQuery));
            assertThrows(SoundnessException.class, () -> resolution.checkSoundness(session,
                                                                                   inferenceQuery));
        }
    }

    @Test
    public void testResolutionPassesForTwoRecursiveRules() {
        loadComplexRecursionExample(typeDB, database);
        TypeQLMatch inferenceQuery = TypeQL.parseQuery("match $transaction has currency $currency;").asMatch();
        testCorrectness(inferenceQuery);
    }

    @Test
    public void testBasicRecursion() {
        loadBasicRecursionExample(typeDB, database);
        TypeQLMatch inferenceQuery = TypeQL.parseQuery("match $com isa company, has is-liable $lia;").asMatch();
        testCorrectness(inferenceQuery);
    }

    private void testCorrectness(TypeQLMatch inferenceQuery) {
        try (RocksSession session = typeDB.session(database, Arguments.Session.Type.DATA)) {
            CorrectnessChecker resolutionTest = CorrectnessChecker.initialise(session);
            resolutionTest.checkSoundness(session, inferenceQuery);
            resolutionTest.checkCompleteness(inferenceQuery);
        }
    }
}
