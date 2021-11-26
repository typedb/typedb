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

package com.vaticle.typedb.core.test.behaviour.reasoner;

import com.vaticle.typedb.core.TypeDB;
import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.common.parameters.Options;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.rocks.RocksDatabase;
import com.vaticle.typedb.core.rocks.RocksSession;
import com.vaticle.typedb.core.rocks.RocksTransaction;
import com.vaticle.typedb.core.rocks.RocksTypeDB;
import com.vaticle.typedb.core.test.behaviour.reasoner.verification.CorrectnessVerifier;
import com.vaticle.typeql.lang.TypeQL;
import com.vaticle.typeql.lang.common.exception.TypeQLException;
import com.vaticle.typeql.lang.query.TypeQLMatch;
import io.cucumber.java.After;
import io.cucumber.java.Before;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.core.test.behaviour.reasoner.verification.CorrectnessVerifier.initialise;
import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

public class ReasonerSteps {

    public static RocksTypeDB typedb;
    public static Path dataDir = Paths.get(System.getProperty("user.dir")).resolve("typedb");
    public static Path logsDir = dataDir.resolve("logs");
    public static Options.Database options = new Options.Database().dataDir(dataDir).reasonerDebuggerDir(logsDir);
    public static RocksSession session;
    public static RocksTransaction reasoningTx;
    public static String DATABASE = "typedb-reasoner-test";
    private static CorrectnessVerifier correctnessVerifier;
    private static TypeQLMatch typeQLQuery;
    private static List<ConceptMap> answers;

    @Before
    public synchronized void before() throws IOException {
        assertNull(typedb);
        resetDirectory();
        System.out.println("Connecting to TypeDB ...");
        typedb = RocksTypeDB.open(options);
        typedb.databases().create(DATABASE);
    }

    @After
    public synchronized void after() {
        if (reasoningTx != null) reasoningTx.close();
        reasoningTx = null;
        if (session != null) session.close();
        session = null;
        if (correctnessVerifier != null) correctnessVerifier.close();
        correctnessVerifier = null;
        typedb.databases().all().forEach(RocksDatabase::delete);
        typedb.close();
        assertFalse(typedb.isOpen());
        typedb = null;
    }

    static RocksSession dataSession() {
        if (session == null || !session.isOpen()) session = typedb.session(DATABASE, Arguments.Session.Type.DATA);
        return session;
    }

    static RocksTransaction reasoningTx() {
        if (reasoningTx == null || reasoningTx.isOpen()) {
            reasoningTx = dataSession().transaction(Arguments.Transaction.Type.READ,
                                                    new Options.Transaction().infer(true));
        }
        return reasoningTx;
    }

    private void clearReasoningTx() {
        if (reasoningTx != null) {
            if (reasoningTx.isOpen()) reasoningTx.close();
            reasoningTx = null;
        }
    }

    @Given("reasoning schema")
    public void schema(String defineQueryStatements) {
        if (correctnessVerifier != null) correctnessVerifier.close();
        if (session != null) session.close();
        try (RocksSession session = typedb.session(DATABASE, Arguments.Session.Type.SCHEMA)) {
            try (RocksTransaction tx = session.transaction(Arguments.Transaction.Type.WRITE)) {
                tx.query().define(TypeQL.parseQuery(String.join("\n", defineQueryStatements)).asDefine());
                tx.commit();
            }
        }
    }

    @Given("reasoning data")
    public void data(String insertQueryStatements) {
        try (RocksSession session = typedb.session(DATABASE, Arguments.Session.Type.DATA)) {
            try (RocksTransaction tx = session.transaction(Arguments.Transaction.Type.WRITE)) {
                tx.query().insert(TypeQL.parseQuery(String.join("\n", insertQueryStatements)).asInsert());
                tx.commit();
            }
        }
    }

    @Given("verifier is initialised")
    public void verifier_is_initialised() {
        correctnessVerifier = initialise(dataSession());
    }

    @Given("reasoning query")
    public void query(String typeQLQueryStatements) {
        try {
            clearReasoningTx();
            typeQLQuery = TypeQL.parseQuery(String.join("\n", typeQLQueryStatements)).asMatch();
            answers = reasoningTx().query().match(typeQLQuery).toList();
        } catch (TypeQLException e) {
            // NOTE: We manually close transaction here, because we want to align with all non-java clients,
            // where parsing happens at server-side which closes transaction if they fail
            clearReasoningTx();
            throw e;
        }
    }

    @Given("verify answer set is equivalent for query")
    public void verify_answer_set_is_equivalent_for_query(String equivalentQuery) {
        try {
            assertNotNull("A typeql query must have been previously loaded in order to test answer equivalence.", typeQLQuery);
            assertNotNull("There are no previous answers to test against; was the reference query ever executed?", answers);
            Set<ConceptMap> newAnswers = reasoningTx().query().match(TypeQL.parseQuery(equivalentQuery).asMatch()).toSet();
            assertEquals(set(answers), newAnswers);
        } catch (TypeQLException e) {
            // NOTE: We manually close transaction here, because we want to align with all non-java clients,
            // where parsing happens at server-side which closes transaction if they fail
            clearReasoningTx();
            throw e;
        }
    }

    @Then("verify answer size is: {number}")
    public void verify_answer_size(int expectedAnswers) {
        assertEquals(String.format("Expected [%d] answers, but got [%d]", expectedAnswers, answers.size()),
                     expectedAnswers, answers.size());
    }

    @Then("verify answers are consistent across {int} executions")
    public static void verify_answers_are_consistent_across_n_executions(int executionCount) {
        Set<ConceptMap> oldAnswers = reasoningTx().query().match(typeQLQuery).toSet();
        for (int i = 0; i < executionCount - 1; i++) {
            try (TypeDB.Transaction transaction = dataSession().transaction(Arguments.Transaction.Type.READ,
                                                                            new Options.Transaction().infer(true))) {
                Set<ConceptMap> answers = transaction.query().match(typeQLQuery).toSet();
                assertEquals(oldAnswers, answers);
            }
        }
    }

    @Then("verify answers are correct")
    public void verify_answers_are_correct() {
        correctnessVerifier.verifySoundness(typeQLQuery);
        correctnessVerifier.verifyCompleteness(typeQLQuery);
    }

    @Then("verify answers are sound")
    public void verify_answers_are_sound() {
        correctnessVerifier.verifySoundness(typeQLQuery);
    }

    @Then("verify answers are complete")
    public void verify_answers_are_complete() {
        correctnessVerifier.verifyCompleteness(typeQLQuery);
    }

    private static void resetDirectory() throws IOException {
        if (Files.exists(dataDir)) {
            System.out.println("Database directory exists!");
            Files.walk(dataDir).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
            System.out.println("Database directory deleted!");
        }

        Files.createDirectory(dataDir);
        System.out.println("Database Directory created: " + dataDir.toString());
    }

}
