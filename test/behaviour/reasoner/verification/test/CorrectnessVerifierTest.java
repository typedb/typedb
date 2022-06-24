/*
 * Copyright (C) 2022 Vaticle
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

package com.vaticle.typedb.core.test.behaviour.reasoner.verification.test;

import com.vaticle.typedb.core.TypeDB;
import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.common.parameters.Options;
import com.vaticle.typedb.core.database.CoreDatabaseManager;
import com.vaticle.typedb.core.database.CoreSession;
import com.vaticle.typedb.core.database.CoreTransaction;
import com.vaticle.typedb.core.test.behaviour.reasoner.verification.CorrectnessVerifier;
import com.vaticle.typedb.core.test.behaviour.reasoner.verification.CorrectnessVerifier.CompletenessException;
import com.vaticle.typedb.core.test.behaviour.reasoner.verification.CorrectnessVerifier.SoundnessException;
import com.vaticle.typedb.core.test.integration.util.Util;
import com.vaticle.typeql.lang.query.TypeQLMatch;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import static com.vaticle.typedb.common.collection.Collections.list;
import static com.vaticle.typedb.core.common.test.Util.assertNotThrows;
import static com.vaticle.typedb.core.common.test.Util.assertThrows;
import static com.vaticle.typeql.lang.TypeQL.and;
import static com.vaticle.typeql.lang.TypeQL.define;
import static com.vaticle.typeql.lang.TypeQL.parseQuery;
import static com.vaticle.typeql.lang.TypeQL.rule;
import static com.vaticle.typeql.lang.TypeQL.type;
import static com.vaticle.typeql.lang.TypeQL.var;
import static com.vaticle.typeql.lang.common.TypeQLArg.ValueType.BOOLEAN;
import static com.vaticle.typeql.lang.common.TypeQLToken.Type.ATTRIBUTE;
import static com.vaticle.typeql.lang.common.TypeQLToken.Type.ENTITY;

public class CorrectnessVerifierTest {

    private static final String database = "CorrectnessVerifierTest";
    private static final Path dataDir = Paths.get(System.getProperty("user.dir")).resolve(database);
    private static final Path logDir = dataDir.resolve("logs");
    private static final Options.Database options = new Options.Database().dataDir(dataDir).reasonerDebuggerDir(logDir);
    private CoreDatabaseManager databaseMgr;

    @Before
    public void setUp() throws IOException {
        Util.resetDirectory(dataDir);
        this.databaseMgr = CoreDatabaseManager.open(options);
        this.databaseMgr.create(database);
        try (TypeDB.Session session = databaseMgr.session(CorrectnessVerifierTest.database, Arguments.Session.Type.SCHEMA)) {
            try (TypeDB.Transaction tx = session.transaction(Arguments.Transaction.Type.WRITE)) {
                tx.query().define(define(list(
                        type("employable").sub(ATTRIBUTE).value(BOOLEAN),
                        type("person").sub(ENTITY).owns("employable"),
                        rule("people-are-employable")
                                .when(and(var("p").isa("person")))
                                .then(var("p").has("employable", true))
                )));
                tx.commit();
            }
        }
        try (TypeDB.Session session = databaseMgr.session(CorrectnessVerifierTest.database, Arguments.Session.Type.DATA)) {
            try (TypeDB.Transaction tx = session.transaction(Arguments.Transaction.Type.WRITE)) {
                tx.query().insert(parseQuery("insert $p isa person;").asInsert());
                tx.commit();
            }
        }
    }

    @After
    public void tearDown() {
        this.databaseMgr.close();
    }

    @Test
    public void testCorrectnessPassesForEmployableExample() {
        TypeQLMatch inferenceQuery = parseQuery("match $x has employable true;").asMatch();
        try (CoreSession session = databaseMgr.session(database, Arguments.Session.Type.DATA)) {
            CorrectnessVerifier correctnessVerifier = CorrectnessVerifier.initialise(session);
            correctnessVerifier.verifyCorrectness(inferenceQuery);
            correctnessVerifier.close();
        }
    }

    @Test
    public void testSoundnessThrowsWhenRuleTriggersTooOftenEmployableExample() {
        TypeQLMatch inferenceQuery = parseQuery("match $x has employable true;").asMatch();
        CorrectnessVerifier correctnessVerifier;
        try (CoreSession session = databaseMgr.session(database, Arguments.Session.Type.DATA)) {
            correctnessVerifier = CorrectnessVerifier.initialise(session);
            try (CoreTransaction tx = session.transaction(Arguments.Transaction.Type.WRITE)) {
                tx.query().insert(parseQuery("insert $p isa person;"));
                tx.commit();
            }
            assertThrows(() -> correctnessVerifier.verifySoundness(inferenceQuery), SoundnessException.class);
            assertNotThrows(() -> correctnessVerifier.verifyCompleteness(inferenceQuery));
        }
    }

    @Test
    public void testCompletenessThrowsWhenRuleIsNotTriggeredEmployableExample() {
        TypeQLMatch inferenceQuery = parseQuery("match $x has employable true;").asMatch();
        CorrectnessVerifier correctnessVerifier;
        try (CoreSession session = databaseMgr.session(database, Arguments.Session.Type.DATA)) {
            correctnessVerifier = CorrectnessVerifier.initialise(session);
            try (CoreTransaction tx = session.transaction(Arguments.Transaction.Type.WRITE)) {
                tx.query().delete(parseQuery("match $p isa person; delete $p isa person;"));
                tx.commit();
            }
            assertThrows(() -> correctnessVerifier.verifyCompleteness(inferenceQuery), CompletenessException.class);
            assertNotThrows(() -> correctnessVerifier.verifySoundness(inferenceQuery));
        }
    }

}
