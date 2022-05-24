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
 */

package com.vaticle.typedb.core.reasoner.controller;

import com.vaticle.typedb.common.concurrent.NamedThreadFactory;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.common.parameters.Options.Database;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concurrent.actor.Actor;
import com.vaticle.typedb.core.concurrent.actor.ActorExecutorGroup;
import com.vaticle.typedb.core.database.CoreDatabaseManager;
import com.vaticle.typedb.core.database.CoreSession;
import com.vaticle.typedb.core.database.CoreTransaction;
import com.vaticle.typedb.core.logic.LogicManager;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.pattern.Disjunction;
import com.vaticle.typedb.core.pattern.variable.Variable;
import com.vaticle.typedb.core.reasoner.ReasonerConsumer;
import com.vaticle.typedb.core.reasoner.processor.AbstractProcessor;
import com.vaticle.typedb.core.test.integration.util.Util;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typeql.lang.TypeQL;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.core.common.collection.Bytes.MB;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Reasoner.REASONING_TERMINATED_WITH_CAUSE;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static java.lang.Thread.sleep;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNull;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

public class ControllerTest {

    private static final boolean PREVENT_HANGING = true;
    private static final Path dataDir = Paths.get(System.getProperty("user.dir")).resolve("computation-graph-test");
    private static final Path logDir = dataDir.resolve("logs");
    private static final Database options = new Database().dataDir(dataDir).reasonerDebuggerDir(logDir)
            .storageDataCacheSize(MB).storageIndexCacheSize(MB).traceInference(false).explain(true);
    private static final String database = "computation-graph-test";
    private static CoreDatabaseManager databaseMgr;

    @Before
    public void setUp() throws IOException {
        Util.resetDirectory(dataDir);
        databaseMgr = CoreDatabaseManager.open(options);
        databaseMgr.create(database);
    }

    @After
    public void tearDown() {
        databaseMgr.close();
    }

    @Test
    public void test_conjunction_no_rules() throws InterruptedException {
        try (CoreSession session = schemaSession()) {
            try (CoreTransaction transaction = singleThreadElgTransaction(session)) {
                transaction.query().define(TypeQL.parseQuery(
                        "define person sub entity, owns age, plays twins:twin1, plays twins:twin2;" +
                                "age sub attribute, value long;" +
                                "twins sub relation, relates twin1, relates twin2;"));
                transaction.commit();
            }
        }
        try (CoreSession session = dataSession()) {
            try (CoreTransaction transaction = singleThreadElgTransaction(session)) {
                transaction.query().insert(TypeQL.parseQuery("insert $p1 isa person, has age 24; $t(twin1: $p1, twin2: $p2) isa twins; $p2 isa person;"));
                transaction.query().insert(TypeQL.parseQuery("insert $p1 isa person, has age 24; $t(twin1: $p1, twin2: $p2) isa twins; $p2 isa person;"));
                transaction.query().insert(TypeQL.parseQuery("insert $p1 isa person, has age 24; $t(twin1: $p1, twin2: $p2) isa twins; $p2 isa person;"));
                transaction.commit();
            }
        }
        try (CoreSession session = dataSession()) {
            try (CoreTransaction transaction = singleThreadElgTransaction(session)) {
                Conjunction conjunctionPattern = resolvedConjunction("{ $t(twin1: $p1, twin2: $p2) isa twins; $p1 has age $a; }", transaction.logic());
                createRootAndAssertResponses(transaction, conjunctionPattern, 3L, 0L);
            }
        }
    }

    @Test
    public void test_exceptions_are_propagated() throws InterruptedException {
        try (CoreSession session = schemaSession()) {
            try (CoreTransaction transaction = singleThreadElgTransaction(session)) {
                transaction.query().define(TypeQL.parseQuery(
                        "define person sub entity, owns age, plays twins:twin1, plays twins:twin2;" +
                                "age sub attribute, value long;" +
                                "twins sub relation, relates twin1, relates twin2;"));
                transaction.commit();
            }
        }
        try (CoreSession session = dataSession()) {
            try (CoreTransaction transaction = singleThreadElgTransaction(session)) {
                transaction.query().insert(TypeQL.parseQuery("insert $p1 isa person, has age 24; $t(twin1: $p1, twin2: $p2) isa twins; $p2 isa person;"));
                transaction.query().insert(TypeQL.parseQuery("insert $p1 isa person, has age 24; $t(twin1: $p1, twin2: $p2) isa twins; $p2 isa person;"));
                transaction.query().insert(TypeQL.parseQuery("insert $p1 isa person, has age 24; $t(twin1: $p1, twin2: $p2) isa twins; $p2 isa person;"));
                transaction.commit();
            }
        }
        try (CoreSession session = dataSession()) {
            try (CoreTransaction transaction = singleThreadElgTransaction(session)) {
                Conjunction conjunctionPattern = resolvedConjunction("{ $t(twin1: $p1, twin2: $p2) isa twins; $p1 has age $a; }", transaction.logic());
                ControllerRegistry registry = transaction.reasoner().controllerRegistry();
                AnswerProducer answerProducer = new AnswerProducer();
                try {
                    registry.createRootConjunction(conjunctionPattern, new HashSet<>(), options.explain(), answerProducer);
                } catch (TypeDBException e) {
                    fail();
                }
                Exception e = new RuntimeException();
                registry.terminate(e);
                Throwable receivedException = answerProducer.exceptions().poll(100, TimeUnit.MILLISECONDS);
                assertEquals(TypeDBException.of(REASONING_TERMINATED_WITH_CAUSE, e), receivedException);
            }
        }
    }

    @Test
    public void test_disjunction_no_rules() throws InterruptedException {
        try (CoreSession session = schemaSession()) {
            try (CoreTransaction transaction = singleThreadElgTransaction(session)) {
                transaction.query().define(TypeQL.parseQuery(
                        "define person sub entity, owns age, plays twins:twin1, plays twins:twin2;" +
                                "age sub attribute, value long;" +
                                "twins sub relation, relates twin1, relates twin2;"));
                transaction.commit();
            }
        }
        try (CoreSession session = dataSession()) {
            try (CoreTransaction transaction = singleThreadElgTransaction(session)) {
                transaction.query().insert(TypeQL.parseQuery("insert $p1 isa person, has age 24; $t(twin1: $p1, twin2: $p2) isa twins; $p2 isa person;"));
                transaction.query().insert(TypeQL.parseQuery("insert $p1 isa person, has age 25; $t(twin1: $p1, twin2: $p2) isa twins; $p2 isa person;"));
                transaction.query().insert(TypeQL.parseQuery("insert $p1 isa person, has age 26; $t(twin1: $p1, twin2: $p2) isa twins; $p2 isa person;"));
                transaction.commit();
            }
        }
        try (CoreSession session = dataSession()) {
            try (CoreTransaction transaction = singleThreadElgTransaction(session)) {
                Set<Identifier.Variable.Retrievable> filter = set(Identifier.Variable.name("t"),
                                                           Identifier.Variable.name("p1"),
                                                           Identifier.Variable.name("p2"));
                Disjunction disjunction = resolvedDisjunction("{ $t(twin1: $p1, twin2: $p2) isa twins; { $p1 has age 24; } or { $p1 has age 26; }; }", transaction.logic());
                createRootAndAssertResponses(transaction, disjunction, filter, 2L, 0L);
            }
        }
    }

    @Test
    public void test_no_rules_with_no_answers() throws InterruptedException {
        try (CoreSession session = schemaSession()) {
            try (CoreTransaction transaction = singleThreadElgTransaction(session)) {
                transaction.query().define(TypeQL.parseQuery(
                        "define person sub entity, owns age, plays twins:twin1, plays twins:twin2;" +
                                "age sub attribute, value long;" +
                                "twins sub relation, relates twin1, relates twin2;"));
                transaction.commit();
            }
        }
        try (CoreSession session = dataSession()) {
            try (CoreTransaction transaction = singleThreadElgTransaction(session)) {
                transaction.query().insert(TypeQL.parseQuery("insert $p1 isa person, has age 24;"));
                transaction.query().insert(TypeQL.parseQuery("insert $p1 isa person, has age 24;"));
                transaction.query().insert(TypeQL.parseQuery("insert $p1 isa person, has age 24;"));
                transaction.commit();
            }
        }
        try (CoreSession session = dataSession()) {
            try (CoreTransaction transaction = singleThreadElgTransaction(session)) {
                Conjunction conjunctionPattern = resolvedConjunction("{ $t(twin1: $p1, twin2: $p2) isa twins; " +
                                                                             "$p1 has age $a; }", transaction.logic());
                createRootAndAssertResponses(transaction, conjunctionPattern, 0L, 0L);
            }
        }
    }

    @Test
    public void test_simple_rule() throws InterruptedException {
        try (CoreSession session = schemaSession()) {
            try (CoreTransaction transaction = singleThreadElgTransaction(session)) {
                transaction.query().define(TypeQL.parseQuery(
                        "define person sub entity, owns name, owns age, plays twins:twin1, plays twins:twin2;" +
                                "age sub attribute, value long;" +
                                "name sub attribute, value string;" +
                                "twins sub relation, relates twin1, relates twin2;" +
                                "rule bobs-are-42: when { $p1 isa person, has name \"Bob\"; } then { $p1 has age 42; };"));
                transaction.commit();
            }
        }
        try (CoreSession session = dataSession()) {
            try (CoreTransaction transaction = singleThreadElgTransaction(session)) {
                transaction.query().insert(TypeQL.parseQuery("insert $p1 isa person, has name \"Bob\";"));
                transaction.query().insert(TypeQL.parseQuery("insert $p1 isa person, has name \"Bob\";"));
                transaction.query().insert(TypeQL.parseQuery("insert $p1 isa person, has name \"Bob\";"));
                transaction.query().insert(TypeQL.parseQuery("insert $p1 isa person, has age 42;"));
                transaction.query().insert(TypeQL.parseQuery("insert $p1 isa person, has age 42;"));
                transaction.query().insert(TypeQL.parseQuery("insert $p1 isa person, has age 42;"));
                transaction.commit();
            }
        }
        try (CoreSession session = dataSession()) {
            try (CoreTransaction transaction = singleThreadElgTransaction(session)) {
                Conjunction conjunctionPattern = resolvedConjunction("{ $p1 isa person, has age 42; }", transaction.logic());
                createRootAndAssertResponses(transaction, conjunctionPattern, 6L, 3L);
            }
        }
    }

    @Test
    public void test_nested_disjunction() throws InterruptedException {
        try (CoreSession session = schemaSession()) {
            try (CoreTransaction transaction = singleThreadElgTransaction(session)) {
                transaction.query().define(TypeQL.parseQuery(
                        "define person sub entity, owns age, owns name;" +
                                "age sub attribute, value long;" +
                                "name sub attribute, value string;" +
                                "rule bobs-are-42: when { $p1 has name \"Bob\"; } then { $p1 has age 42; };" +
                                "rule susans-are-24: when { $p1 has name \"Susan\"; } then { $p1 has age 24; };"
                ));
                transaction.commit();
            }
        }

        try (CoreSession session = dataSession()) {
            try (CoreTransaction transaction = singleThreadElgTransaction(session)) {
                transaction.query().insert(TypeQL.parseQuery("insert $p1 isa person, has name \"Bob\";"));
                transaction.query().insert(TypeQL.parseQuery("insert $p1 isa person, has name \"Bob\";"));
                transaction.query().insert(TypeQL.parseQuery("insert $p1 isa person, has name \"Susan\";"));
                transaction.query().insert(TypeQL.parseQuery("insert $p1 isa person, has age 30;"));
                transaction.commit();
            }
        }
        try (CoreSession session = dataSession()) {
            try (CoreTransaction transaction = singleThreadElgTransaction(session)) {
                Conjunction conjunctionPattern = resolvedConjunction("{ $p isa person; not { { $p has age 24; } or { $p has age 42; }; }; }", transaction.logic());
                createRootAndAssertResponses(transaction, conjunctionPattern, 1L, 0L);
            }
        }
    }

    @Test
    public void test_negation() throws InterruptedException {
        try (CoreSession session = schemaSession()) {
            try (CoreTransaction transaction = singleThreadElgTransaction(session)) {
                transaction.query().define(TypeQL.parseQuery(
                        "define person sub entity, owns age, owns name;" +
                                "age sub attribute, value long;" +
                                "name sub attribute, value string;" +
                                "rule susans-are-24: when { $p1 has name \"Susan\"; } then { $p1 has age 24; };"
                ));
                transaction.commit();
            }
        }

        try (CoreSession session = dataSession()) {
            try (CoreTransaction transaction = singleThreadElgTransaction(session)) {
                transaction.query().insert(TypeQL.parseQuery("insert $p1 isa person, has name \"Bob\";"));
                transaction.commit();
            }
        }

        try (CoreSession session = dataSession()) {
            try (CoreTransaction transaction = singleThreadElgTransaction(session)) {
                Conjunction conjunctionPattern = resolvedConjunction("{ $p isa person; not { $p has age 24; }; }", transaction.logic());
                createRootAndAssertResponses(transaction, conjunctionPattern, 1L, 0L);
            }
        }
    }

    @Test
    public void test_chained_rules() throws InterruptedException {
        try (CoreSession session = schemaSession()) {
            try (CoreTransaction transaction = singleThreadElgTransaction(session)) {
                transaction.query().define(TypeQL.parseQuery(
                        "define person sub entity, owns name, owns age, plays employment:employee;" +
                                "age sub attribute, value long;" +
                                "name sub attribute, value string;" +
                                "employment sub relation, relates employee;" +
                                "rule bobs-are-42: when { $p1 isa person, has name \"Bob\"; } then { $p1 has age 42; };" +
                                "rule those-aged-42-are-employed: when { $x has age 42; } then { (employee: $x) isa employment; };"));
                transaction.commit();
            }
        }
        try (CoreSession session = dataSession()) {
            try (CoreTransaction transaction = singleThreadElgTransaction(session)) {
                transaction.query().insert(TypeQL.parseQuery("insert $p isa person, has name \"Bob\";"));
                transaction.query().insert(TypeQL.parseQuery("insert $p isa person, has name \"Bob\";"));
                transaction.query().insert(TypeQL.parseQuery("insert $p isa person, has name \"Bob\";"));
                transaction.query().insert(TypeQL.parseQuery("insert $p isa person, has age 42;"));
                transaction.query().insert(TypeQL.parseQuery("insert $p isa person, has age 42;"));
                transaction.query().insert(TypeQL.parseQuery("insert $p isa person, has age 42;"));
                transaction.query().insert(TypeQL.parseQuery("insert $p isa person; $e(employee: $p) isa employment;"));
                transaction.query().insert(TypeQL.parseQuery("insert $p isa person; $e(employee: $p) isa employment;"));
                transaction.query().insert(TypeQL.parseQuery("insert $p isa person; $e(employee: $p) isa employment;"));
                transaction.commit();
            }
        }
        try (CoreSession session = dataSession()) {
            try (CoreTransaction transaction = singleThreadElgTransaction(session)) {

                String rootConjunction = "{ $e(employee: $x) isa employment; }";
                Conjunction conjunctionPattern = resolvedConjunction(rootConjunction, transaction.logic());
                createRootAndAssertResponses(transaction, conjunctionPattern, 9L, 6L);
            }
        }
    }

    @Test
    public void test_shallow_rerequest_chain() throws InterruptedException {
        try (CoreSession session = schemaSession()) {
            try (CoreTransaction transaction = singleThreadElgTransaction(session)) {
                transaction.query().define(TypeQL.parseQuery(
                        "define woman sub entity, plays marriage:wife, plays friendship:friend;" +
                                "man sub entity, plays marriage:husband, plays friendship:friend;" +
                                "friendship sub relation, relates friend;" +
                                "marriage sub relation, relates husband, relates wife;" +
                                "rule marriage-is-friendship: when {$x isa man; $y isa woman; " +
                                "(husband: $x, wife: $y) isa marriage; } then { (friend: $x, friend: $y) isa friendship; }; "));
                transaction.commit();
            }
        }
        try (CoreSession session = dataSession()) {
            try (CoreTransaction transaction = singleThreadElgTransaction(session)) {
                String insert = "insert $y isa woman; $x isa man; (husband: $x, wife: $y) isa marriage;";
                transaction.query().insert(TypeQL.parseQuery(insert));
                transaction.query().insert(TypeQL.parseQuery(insert));

                String insert2 = "insert $y isa woman; $x isa woman; (wife: $x, wife: $y) isa marriage;";
                transaction.query().insert(TypeQL.parseQuery(insert2));
                transaction.query().insert(TypeQL.parseQuery(insert2));

                String insert3 = "insert $y isa man;";
                transaction.query().insert(TypeQL.parseQuery(insert3));
                transaction.query().insert(TypeQL.parseQuery(insert3));
                transaction.commit();
            }
        }
        try (CoreSession session = dataSession()) {
            try (CoreTransaction transaction = singleThreadElgTransaction(session)) {
                String rootConjunction = "{ $a isa woman; $b isa man; $f(friend: $a, friend: $b) isa friendship; }";
                Conjunction conjunctionPattern = resolvedConjunction(rootConjunction, transaction.logic());
                createRootAndAssertResponses(transaction, conjunctionPattern, 2L, 2L);
            }
        }
    }

    @Test
    public void test_deep_rerequest_chain() throws InterruptedException {
        try (CoreSession session = schemaSession()) {
            try (CoreTransaction transaction = singleThreadElgTransaction(session)) {
                transaction.query().define(TypeQL.parseQuery(
                        "define woman sub entity, plays marriage:wife, plays friendship:friend, plays employment:employee, plays association:associated;" +
                                "man sub entity, plays marriage:husband, plays friendship:friend;" +
                                "friendship sub relation, relates friend;" +
                                "company sub entity, plays employment:employer, plays association:associated;" +
                                "marriage sub relation, relates husband, relates wife;" +
                                "employment sub relation, relates employee, relates employer;" +
                                "association sub relation, relates associated;" +
                                "rule marriage-is-friendship: when {$x isa man; $y isa woman; " +
                                "(husband: $x, wife: $y) isa marriage; } then { (friend: $x, friend: $y) isa friendship; }; " +
                                "rule employment-is-association: when {$x isa woman; $y isa company; " +
                                "(employee: $x, employer: $y) isa employment; } then { (associated: $x, associated: $y) isa association; }; "));
                transaction.commit();
            }
        }
        try (CoreSession session = dataSession()) {
            try (CoreTransaction transaction = singleThreadElgTransaction(session)) {
                Stream.of(
                        "insert $y isa woman; $x isa man; (husband: $x, wife: $y) isa marriage; (employee: $y, employer: $z) isa employment; $z isa company;",
                        "insert $y isa woman; $x isa man; (husband: $x, wife: $y) isa marriage;",
                        "insert $y isa woman; $x isa woman; (wife: $x, wife: $y) isa marriage;",
                        "insert $y isa man;",
                        "insert $y isa woman;",
                        "insert $y isa woman; (employee: $y, employer: $z) isa employment; $z isa company;",
                        "insert $z isa company;"
                ).forEach(q -> transaction.query().insert(TypeQL.parseQuery(q)));
                transaction.commit();
            }
        }
        try (CoreSession session = dataSession()) {
            try (CoreTransaction transaction = singleThreadElgTransaction(session)) {
                Conjunction conjunctionPattern = resolvedConjunction(
                        "{ $x isa man; (friend: $x, friend: $y) isa friendship; $y isa woman; " +
                                "(associated: $y, associated: $z) isa association; $z isa company; }", transaction.logic());
                createRootAndAssertResponses(transaction, conjunctionPattern, 1L, 1L);
            }
        }
    }

    @Test
    public void test_recursive_termination_and_deduplication_in_transitivity() throws InterruptedException {
        try (CoreSession session = schemaSession()) {
            try (CoreTransaction transaction = singleThreadElgTransaction(session)) {
                transaction.query().define(TypeQL.parseQuery(
                        "define location sub entity, plays containment:container, plays containment:contained;" +
                                "containment sub relation, relates contained, relates container;" +
                                "rule transitive-containment: when {" +
                                "(container:$x, contained:$y) isa containment;" +
                                "(container:$y, contained:$z) isa containment;" +
                                "} then {" +
                                "(container:$x, contained:$z) isa containment;" +
                                "};"));
                transaction.commit();
            }
        }
        try (CoreSession session = dataSession()) {
            try (CoreTransaction transaction = singleThreadElgTransaction(session)) {
                transaction.query().insert(
                        TypeQL.parseQuery(
                                "insert " +
                                        "$l1 isa location; $l2 isa location; $l3 isa location; $l4 isa location; " +
                                        "(container:$l1, contained:$l2) isa containment;" +
                                        "(container:$l2, contained:$l3) isa containment;" +
                                        "(container:$l3, contained:$l4) isa containment;"
                        ));
                transaction.commit();
            }
        }
        try (CoreSession session = dataSession()) {
            try (CoreTransaction transaction = singleThreadElgTransaction(session)) {
                Conjunction conjunctionPattern = resolvedConjunction("{ (container:$l3, contained:$l4) isa containment; }", transaction.logic());
                createRootAndAssertResponses(transaction, conjunctionPattern, 6L, 3L);
            }
        }
    }

    private static Disjunction resolvedDisjunction(String query, LogicManager logicMgr) {
        Disjunction disjunction = Disjunction.create(TypeQL.parsePattern(query).asConjunction().normalise());
        logicMgr.typeInference().applyCombination(disjunction);
        return disjunction;
    }

    private static Conjunction resolvedConjunction(String query, LogicManager logicMgr) {
        Disjunction disjunction = resolvedDisjunction(query, logicMgr);
        assert disjunction.conjunctions().size() == 1;
        return disjunction.conjunctions().get(0);
    }

    private CoreSession schemaSession() {
        return databaseMgr.session(database, Arguments.Session.Type.SCHEMA);
    }

    private CoreSession dataSession() {
        return databaseMgr.session(database, Arguments.Session.Type.DATA);
    }

    private CoreTransaction singleThreadElgTransaction(CoreSession session) {
        CoreTransaction transaction = session.transaction(Arguments.Transaction.Type.WRITE);
        ActorExecutorGroup service = new ActorExecutorGroup(1, new NamedThreadFactory("typedb-actor"));
        transaction.reasoner().controllerRegistry().setExecutorService(service);
        return transaction;
    }

    private static class AnswerProducer implements ReasonerConsumer<ConceptMap> {

        private final LinkedBlockingQueue<ConceptMap> responses;
        private final LinkedBlockingQueue<Throwable> exceptions;
        private final AtomicBoolean doneReceived;
        private Actor.Driver<? extends AbstractProcessor<?, ?, ?, ?>> rootProcessor;
        private boolean pullOnSet;

        private AnswerProducer() {
            responses = new LinkedBlockingQueue<>();
            exceptions = new LinkedBlockingQueue<>();
            doneReceived = new AtomicBoolean(false);
            pullOnSet = false;
        }

        private void getNextAnswer() {
            if (rootProcessor != null) pull();
            else pullOnSet = true;
        }

        private void pull() {
            rootProcessor.execute(actor -> actor.rootPull());
        }

        @Override
        public void setRootProcessor(Actor.Driver<? extends AbstractProcessor<?, ConceptMap, ?, ?>> rootProcessor) {
            this.rootProcessor = rootProcessor;
            if (pullOnSet) pull();
        }

        @Override
        public void receiveAnswer(ConceptMap answer) {
            assertFalse(doneReceived.get());
            responses.add(answer);
            getNextAnswer();
        }

        @Override
        public void finish() {
            doneReceived.set(true);
        }

        @Override
        public void exception(Throwable e) {
            exceptions.add(e);
        }

        private LinkedBlockingQueue<Throwable> exceptions() {
            return exceptions;
        }
    }

    private void createRootAndAssertResponses(CoreTransaction transaction, Disjunction disjunction,
                                              Set<Identifier.Variable.Retrievable> filter, long answerCount,
                                              long explainableAnswers) throws InterruptedException {
        ControllerRegistry registry = transaction.reasoner().controllerRegistry();
        AnswerProducer answerProducer = new AnswerProducer();
        answerProducer.getNextAnswer();
        try {
             registry.createRootDisjunction(disjunction, filter, options.explain(), answerProducer);
        } catch (TypeDBException e) {
            fail();
            return;
        }
        assertResponses(answerProducer.responses, answerCount, explainableAnswers, answerProducer.doneReceived);
    }

    private void createRootAndAssertResponses(CoreTransaction transaction, Conjunction conjunction, long answerCount,
                                              long explainableAnswers) throws InterruptedException {
        ControllerRegistry registry = transaction.reasoner().controllerRegistry();
        Set<Identifier.Variable.Retrievable> filter = new HashSet<>();
        iterate(conjunction.variables()).map(Variable::id).filter(Identifier::isName).map(Identifier.Variable::asName)
                .forEachRemaining(filter::add);
        AnswerProducer answerProducer = new AnswerProducer();
        answerProducer.getNextAnswer();
        try {
            registry.createRootConjunction(conjunction, filter, options.explain(), answerProducer);
        } catch (TypeDBException e) {
            fail();
            return;
        }
        assertResponses(answerProducer.responses, answerCount, explainableAnswers, answerProducer.doneReceived);
    }

    private void assertResponses(
            LinkedBlockingQueue<ConceptMap> responses, long answerCount, long explainableAnswers,
            AtomicBoolean doneReceived
    ) throws InterruptedException {
        long startTime = System.currentTimeMillis();
        long n = answerCount + 1; //total number of traversal answers, plus one expected Exhausted (-1 answer)
        int answersFound = 0;
        int explainableAnswersFound = 0;
        for (int i = 0; i < n - 1; i++) {
            ConceptMap answer;
            if (PREVENT_HANGING) answer = responses.poll(500, TimeUnit.MILLISECONDS);  // polling prevents the test hanging
            else answer = responses.take();

            if (answer != null) {
                answersFound += 1;
                if (answer.explainables().iterator().count() > 0) {
                    explainableAnswersFound++;
                }
            }
        }

        assertEquals(answerCount, answersFound);
        assertEquals(explainableAnswers, explainableAnswersFound);
        ConceptMap answer = responses.poll(500, TimeUnit.MILLISECONDS);  // Poll for one more answer, expecting failure
        assertNull(answer);
        assertTrue(doneReceived.get());
        assertTrue(responses.isEmpty());
        System.out.println("Time : " + (System.currentTimeMillis() - startTime));
    }
}
