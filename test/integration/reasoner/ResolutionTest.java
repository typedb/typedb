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

package grakn.core.reasoner;

import grakn.core.common.parameters.Arguments;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concurrent.actor.Actor;
import grakn.core.concurrent.actor.EventLoopGroup;
import grakn.core.pattern.Conjunction;
import grakn.core.pattern.Disjunction;
import grakn.core.pattern.variable.Variable;
import grakn.core.reasoner.resolution.ResolverRegistry;
import grakn.core.reasoner.resolution.answer.AnswerState;
import grakn.core.reasoner.resolution.answer.AnswerState.UpstreamVars.Initial;
import grakn.core.reasoner.resolution.framework.Request;
import grakn.core.reasoner.resolution.framework.ResolutionAnswer;
import grakn.core.reasoner.resolution.framework.Resolver;
import grakn.core.reasoner.resolution.resolver.Root;
import grakn.core.rocks.RocksGrakn;
import grakn.core.rocks.RocksSession;
import grakn.core.rocks.RocksTransaction;
import grakn.core.test.integration.util.Util;
import graql.lang.Graql;
import graql.lang.pattern.variable.Reference;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static grakn.common.collection.Collections.set;
import static grakn.core.common.iterator.Iterators.iterate;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

public class ResolutionTest {

    private static Path directory = Paths.get(System.getProperty("user.dir")).resolve("resolution-test");
    private static String database = "resolution-test";
    private static RocksGrakn grakn;

    @Before
    public void setUp() throws IOException {
        Util.resetDirectory(directory);
        grakn = RocksGrakn.open(directory);
        grakn.databases().create(database);
    }

    @After
    public void tearDown() {
        grakn.close();
    }

    @Test
    public void test_conjunction_no_rules() throws InterruptedException {
        try (RocksSession session = schemaSession()) {
            try (RocksTransaction transaction = singleThreadElgTransaction(session)) {
                transaction.query().define(Graql.parseQuery(
                        "define person sub entity, owns age, plays twins:twin1, plays twins:twin2;" +
                                "age sub attribute, value long;" +
                                "twins sub relation, relates twin1, relates twin2;"));
                transaction.commit();
            }
        }
        try (RocksSession session = dataSession()) {
            try (RocksTransaction transaction = singleThreadElgTransaction(session)) {
                transaction.query().insert(Graql.parseQuery("insert $p1 isa person, has age 24; $t(twin1: $p1, twin2: $p2) isa twins; $p2 isa person;"));
                transaction.query().insert(Graql.parseQuery("insert $p1 isa person, has age 24; $t(twin1: $p1, twin2: $p2) isa twins; $p2 isa person;"));
                transaction.query().insert(Graql.parseQuery("insert $p1 isa person, has age 24; $t(twin1: $p1, twin2: $p2) isa twins; $p2 isa person;"));
                transaction.commit();
            }
        }
        try (RocksSession session = dataSession()) {
            try (RocksTransaction transaction = singleThreadElgTransaction(session)) {
                Conjunction conjunctionPattern = parseConjunction(transaction, "{ $t(twin1: $p1, twin2: $p2) isa twins; $p1 has age $a; }");
                createRootAndAssertResponses(transaction, conjunctionPattern, null, null, 3L);
            }
        }
    }

    @Test
    public void test_conjunction_no_rules_limited_offset() throws InterruptedException {
        try (RocksSession session = schemaSession()) {
            try (RocksTransaction transaction = singleThreadElgTransaction(session)) {
                transaction.query().define(Graql.parseQuery(
                        "define person sub entity, owns age, plays twins:twin1, plays twins:twin2;" +
                                "age sub attribute, value long;" +
                                "twins sub relation, relates twin1, relates twin2;"));
                transaction.commit();
            }
        }
        try (RocksSession session = dataSession()) {
            try (RocksTransaction transaction = singleThreadElgTransaction(session)) {
                transaction.query().insert(Graql.parseQuery("insert $p1 isa person, has age 24; $t(twin1: $p1, twin2: $p2) isa twins; $p2 isa person;"));
                transaction.query().insert(Graql.parseQuery("insert $p1 isa person, has age 24; $t(twin1: $p1, twin2: $p2) isa twins; $p2 isa person;"));
                transaction.query().insert(Graql.parseQuery("insert $p1 isa person, has age 24; $t(twin1: $p1, twin2: $p2) isa twins; $p2 isa person;"));
                transaction.commit();
            }
        }
        try (RocksSession session = dataSession()) {
            try (RocksTransaction transaction = singleThreadElgTransaction(session)) {
                Conjunction conjunctionPattern = parseConjunction(transaction, "{ $t(twin1: $p1, twin2: $p2) isa twins; $p1 has age $a; }");
                createRootAndAssertResponses(transaction, conjunctionPattern, 1L, 1L, 1L);
            }
        }
    }

    @Test
    public void test_disjunction_no_rules() throws InterruptedException {
        try (RocksSession session = schemaSession()) {
            try (RocksTransaction transaction = singleThreadElgTransaction(session)) {
                transaction.query().define(Graql.parseQuery(
                        "define person sub entity, owns age, plays twins:twin1, plays twins:twin2;" +
                                "age sub attribute, value long;" +
                                "twins sub relation, relates twin1, relates twin2;"));
                transaction.commit();
            }
        }
        try (RocksSession session = dataSession()) {
            try (RocksTransaction transaction = singleThreadElgTransaction(session)) {
                transaction.query().insert(Graql.parseQuery("insert $p1 isa person, has age 24; $t(twin1: $p1, twin2: $p2) isa twins; $p2 isa person;"));
                transaction.query().insert(Graql.parseQuery("insert $p1 isa person, has age 25; $t(twin1: $p1, twin2: $p2) isa twins; $p2 isa person;"));
                transaction.query().insert(Graql.parseQuery("insert $p1 isa person, has age 26; $t(twin1: $p1, twin2: $p2) isa twins; $p2 isa person;"));
                transaction.commit();
            }
        }
        try (RocksSession session = dataSession()) {
            try (RocksTransaction transaction = singleThreadElgTransaction(session)) {
                Set<Reference.Name> filter = set(Reference.name("t"),
                                                 Reference.name("p1"),
                                                 Reference.name("p2"));
                Disjunction disjunction = parseDisjunction(transaction, "{ $t(twin1: $p1, twin2: $p2) isa twins; { $p1 has age 24; } or { $p1 has age 26; }; }");
                createRootAndAssertResponses(transaction, disjunction, filter, null, null, 2L);
            }
        }
    }

    @Test
    public void test_disjunction_no_rules_limit_offset() throws InterruptedException {
        try (RocksSession session = schemaSession()) {
            try (RocksTransaction transaction = singleThreadElgTransaction(session)) {
                transaction.query().define(Graql.parseQuery(
                        "define person sub entity, owns age, plays twins:twin1, plays twins:twin2;" +
                                "age sub attribute, value long;" +
                                "twins sub relation, relates twin1, relates twin2;"));
                transaction.commit();
            }
        }
        try (RocksSession session = dataSession()) {
            try (RocksTransaction transaction = singleThreadElgTransaction(session)) {
                transaction.query().insert(Graql.parseQuery("insert $p1 isa person, has age 24; $t(twin1: $p1, twin2: $p2) isa twins; $p2 isa person;"));
                transaction.query().insert(Graql.parseQuery("insert $p1 isa person, has age 25; $t(twin1: $p1, twin2: $p2) isa twins; $p2 isa person;"));
                transaction.query().insert(Graql.parseQuery("insert $p1 isa person, has age 26; $t(twin1: $p1, twin2: $p2) isa twins; $p2 isa person;"));
                transaction.query().insert(Graql.parseQuery("insert $p1 isa person, has age 27; $t(twin1: $p1, twin2: $p2) isa twins; $p2 isa person;"));
                transaction.commit();
            }
        }
        try (RocksSession session = dataSession()) {
            try (RocksTransaction transaction = singleThreadElgTransaction(session)) {
                Set<Reference.Name> filter = set(Reference.name("t"),
                                                 Reference.name("p1"),
                                                 Reference.name("p2"));
                Disjunction disjunction = parseDisjunction(transaction, "{ $t(twin1: $p1, twin2: $p2) isa twins; " +
                        "{ $p1 has age 24; } or { $p1 has age 26; } or { $p1 has age 27;} ; }");
                createRootAndAssertResponses(transaction, disjunction, filter, 1L, 1L, 1L);
            }
        }
    }

    @Test
    public void test_no_rules_with_no_answers() throws InterruptedException {
        try (RocksSession session = schemaSession()) {
            try (RocksTransaction transaction = singleThreadElgTransaction(session)) {
                transaction.query().define(Graql.parseQuery(
                        "define person sub entity, owns age, plays twins:twin1, plays twins:twin2;" +
                                "age sub attribute, value long;" +
                                "twins sub relation, relates twin1, relates twin2;"));
                transaction.commit();
            }
        }
        try (RocksSession session = dataSession()) {
            try (RocksTransaction transaction = singleThreadElgTransaction(session)) {
                transaction.query().insert(Graql.parseQuery("insert $p1 isa person, has age 24;"));
                transaction.query().insert(Graql.parseQuery("insert $p1 isa person, has age 24;"));
                transaction.query().insert(Graql.parseQuery("insert $p1 isa person, has age 24;"));
                transaction.commit();
            }
        }
        try (RocksSession session = dataSession()) {
            try (RocksTransaction transaction = singleThreadElgTransaction(session)) {
                Conjunction conjunctionPattern = parseConjunction(transaction, "{ $t(twin1: $p1, twin2: $p2) isa twins; " +
                        "$p1 has age $a; }");
                createRootAndAssertResponses(transaction, conjunctionPattern, null, null, 0L);
            }
        }
    }

    @Test
    public void test_simple_rule() throws InterruptedException {
        // TODO We would like to reach into the reasoner to check that:
        //  - 3 answers come from the direct traversal at the root,
        //  - 3 answers come from the concludable via the rule and its retrievable, checking their sent/received messages
        //  are consistent with our expectation.

        try (RocksSession session = schemaSession()) {
            try (RocksTransaction transaction = singleThreadElgTransaction(session)) {
                transaction.query().define(Graql.parseQuery(
                        "define person sub entity, owns name, owns age, plays twins:twin1, plays twins:twin2;" +
                                "age sub attribute, value long;" +
                                "name sub attribute, value string;" +
                                "twins sub relation, relates twin1, relates twin2;" +
                                "rule bobs-are-42: when { $p1 isa person, has name \"Bob\"; } then { $p1 has age 42; };"));
                transaction.commit();
            }
        }
        try (RocksSession session = dataSession()) {
            try (RocksTransaction transaction = singleThreadElgTransaction(session)) {
                transaction.query().insert(Graql.parseQuery("insert $p1 isa person, has name \"Bob\";"));
                transaction.query().insert(Graql.parseQuery("insert $p1 isa person, has name \"Bob\";"));
                transaction.query().insert(Graql.parseQuery("insert $p1 isa person, has name \"Bob\";"));
                transaction.query().insert(Graql.parseQuery("insert $p1 isa person, has age 42;"));
                transaction.query().insert(Graql.parseQuery("insert $p1 isa person, has age 42;"));
                transaction.query().insert(Graql.parseQuery("insert $p1 isa person, has age 42;"));
                transaction.commit();
            }
        }
        try (RocksSession session = dataSession()) {
            try (RocksTransaction transaction = singleThreadElgTransaction(session)) {
                Conjunction conjunctionPattern = parseConjunction(transaction, "{ $p1 isa person, has age 42; }");
                createRootAndAssertResponses(transaction, conjunctionPattern, null, null, 6L);
            }
        }
    }

    @Test
    public void test_chained_rules() throws InterruptedException {
        try (RocksSession session = schemaSession()) {
            try (RocksTransaction transaction = singleThreadElgTransaction(session)) {
                transaction.query().define(Graql.parseQuery(
                        "define person sub entity, owns name, owns age, plays employment:employee;" +
                                "age sub attribute, value long;" +
                                "name sub attribute, value string;" +
                                "employment sub relation, relates employee;" +
                                "rule bobs-are-42: when { $p1 isa person, has name \"Bob\"; } then { $p1 has age 42; };" +
                                "rule those-aged-42-are-employed: when { $x has age 42; } then { (employee: $x) isa employment; };"));
                transaction.commit();
            }
        }
        try (RocksSession session = dataSession()) {
            try (RocksTransaction transaction = singleThreadElgTransaction(session)) {
                transaction.query().insert(Graql.parseQuery("insert $p isa person, has name \"Bob\";"));
                transaction.query().insert(Graql.parseQuery("insert $p isa person, has name \"Bob\";"));
                transaction.query().insert(Graql.parseQuery("insert $p isa person, has name \"Bob\";"));
                transaction.query().insert(Graql.parseQuery("insert $p isa person, has age 42;"));
                transaction.query().insert(Graql.parseQuery("insert $p isa person, has age 42;"));
                transaction.query().insert(Graql.parseQuery("insert $p isa person, has age 42;"));
                transaction.query().insert(Graql.parseQuery("insert $p isa person; $e(employee: $p) isa employment;"));
                transaction.query().insert(Graql.parseQuery("insert $p isa person; $e(employee: $p) isa employment;"));
                transaction.query().insert(Graql.parseQuery("insert $p isa person; $e(employee: $p) isa employment;"));
                transaction.commit();
            }
        }
        try (RocksSession session = dataSession()) {
            try (RocksTransaction transaction = singleThreadElgTransaction(session)) {

                String rootConjunction = "{ $e(employee: $x) isa employment; }";
                Conjunction conjunctionPattern = parseConjunction(transaction, rootConjunction);
                createRootAndAssertResponses(transaction, conjunctionPattern, null, null, 9L);
            }
        }
    }

    @Test
    public void test_shallow_rerequest_chain() throws InterruptedException {
        try (RocksSession session = schemaSession()) {
            try (RocksTransaction transaction = singleThreadElgTransaction(session)) {
                transaction.query().define(Graql.parseQuery(
                        "define woman sub entity, plays marriage:wife, plays friendship:friend;" +
                                "man sub entity, plays marriage:husband, plays friendship:friend;" +
                                "friendship sub relation, relates friend;" +
                                "marriage sub relation, relates husband, relates wife;" +
                                "rule marriage-is-friendship: when {$x isa man; $y isa woman; " +
                                "(husband: $x, wife: $y) isa marriage; } then { (friend: $x, friend: $y) isa friendship; }; "));
                transaction.commit();
            }
        }
        try (RocksSession session = dataSession()) {
            try (RocksTransaction transaction = singleThreadElgTransaction(session)) {
                String insert = "insert $y isa woman; $x isa man; (husband: $x, wife: $y) isa marriage;";
                transaction.query().insert(Graql.parseQuery(insert));
                transaction.query().insert(Graql.parseQuery(insert));

                String insert2 = "insert $y isa woman; $x isa woman; (wife: $x, wife: $y) isa marriage;";
                transaction.query().insert(Graql.parseQuery(insert2));
                transaction.query().insert(Graql.parseQuery(insert2));

                String insert3 = "insert $y isa man;";
                transaction.query().insert(Graql.parseQuery(insert3));
                transaction.query().insert(Graql.parseQuery(insert3));
                transaction.commit();
            }
        }
        try (RocksSession session = dataSession()) {
            try (RocksTransaction transaction = singleThreadElgTransaction(session)) {
                String rootConjunction = "{ $a isa woman; $b isa man; $f(friend: $a, friend: $b) isa friendship; }";
                Conjunction conjunctionPattern = parseConjunction(transaction, rootConjunction);
                createRootAndAssertResponses(transaction, conjunctionPattern, null, null, 2L);
            }
        }
    }

    @Test
    public void test_deep_rerequest_chain() throws InterruptedException {
        try (RocksSession session = schemaSession()) {
            try (RocksTransaction transaction = singleThreadElgTransaction(session)) {
                transaction.query().define(Graql.parseQuery(
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
        try (RocksSession session = dataSession()) {
            try (RocksTransaction transaction = singleThreadElgTransaction(session)) {
                Stream.of(
                        "insert $y isa woman; $x isa man; (husband: $x, wife: $y) isa marriage; (employee: $y, employer: $z) isa employment; $z isa company;",
                        "insert $y isa woman; $x isa man; (husband: $x, wife: $y) isa marriage;",
                        "insert $y isa woman; $x isa woman; (wife: $x, wife: $y) isa marriage;",
                        "insert $y isa man;",
                        "insert $y isa woman;",
                        "insert $y isa woman; (employee: $y, employer: $z) isa employment; $z isa company;",
                        "insert $z isa company;"
                ).forEach(q -> transaction.query().insert(Graql.parseQuery(q)));
                transaction.commit();
            }
        }
        try (RocksSession session = dataSession()) {
            try (RocksTransaction transaction = singleThreadElgTransaction(session)) {
                Conjunction conjunctionPattern = parseConjunction(transaction, "{ $x isa man; " +
                        "(friend: $x, friend: $y) isa friendship; $y isa woman; (associated: $y, associated: $z) isa association; $z isa company; }");
                createRootAndAssertResponses(transaction, conjunctionPattern, null, null, 1L);
            }
        }
    }

    @Test
    public void test_recursive_termination_and_deduplication_in_transitivity() throws InterruptedException {
        try (RocksSession session = schemaSession()) {
            try (RocksTransaction transaction = singleThreadElgTransaction(session)) {
                transaction.query().define(Graql.parseQuery(
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
        try (RocksSession session = dataSession()) {
            try (RocksTransaction transaction = singleThreadElgTransaction(session)) {
                transaction.query().insert(
                        Graql.parseQuery(
                                "insert " +
                                        "$l1 isa location; $l2 isa location; $l3 isa location; $l4 isa location; " +
                                        "(container:$l1, contained:$l2) isa containment;" +
                                        "(container:$l2, contained:$l3) isa containment;" +
                                        "(container:$l3, contained:$l4) isa containment;"
                        ));
                transaction.commit();
            }
        }
        try (RocksSession session = dataSession()) {
            try (RocksTransaction transaction = singleThreadElgTransaction(session)) {
                Conjunction conjunctionPattern = parseConjunction(transaction, "{ (container:$l3, contained:$l4) isa containment; }");
                createRootAndAssertResponses(transaction, conjunctionPattern, null, null, 6L);
            }
        }
    }

    @Ignore // TODO Un-ignore, ignored until explanations are ready to use
    @Test
    public void test_answer_recorder() throws InterruptedException {
        String atomic1 = "$p1 isa person, has name \"Bob\";";
        String atomic2 = "$p1 isa person, has age 42;";
        String rule = "rule bobs-are-42: when { $p1 has name \"Bob\"; } then { $p1 has age 42; };";
        try (RocksSession session = schemaSession()) {
            try (RocksTransaction transaction = singleThreadElgTransaction(session)) {
                transaction.query().define(Graql.parseQuery(
                        "define person sub entity, owns age, owns name;" +
                                "age sub attribute, value long;" +
                                "name sub attribute, value string;" +
                                rule));
                transaction.commit();
            }
        }
        try (RocksSession session = dataSession()) {
            try (RocksTransaction transaction = singleThreadElgTransaction(session)) {
                transaction.query().insert(Graql.parseQuery("insert " + atomic1));
                transaction.query().insert(Graql.parseQuery("insert " + atomic1));
                transaction.query().insert(Graql.parseQuery("insert " + atomic1));
                transaction.commit();
            }
        }

        long atomic1TraversalAnswerCount = 3L;
        long ruleTraversalAnswerCount = 0L;
        long atomic2TraversalAnswerCount = 0L;
        long conjunctionTraversalAnswerCount = 3L;
        long answerCount = conjunctionTraversalAnswerCount + atomic2TraversalAnswerCount + ruleTraversalAnswerCount
                + atomic1TraversalAnswerCount;

        try (RocksSession session = dataSession()) {
            try (RocksTransaction transaction = singleThreadElgTransaction(session)) {
                Conjunction conjunctionPattern = parseConjunction(transaction, "{ " + atomic2 + " }");
                ResolverRegistry registry = transaction.reasoner().resolverRegistry();
                LinkedBlockingQueue<ResolutionAnswer> responses = new LinkedBlockingQueue<>();
                AtomicLong doneReceived = new AtomicLong(0L);
                Set<Reference.Name> filter = iterate(conjunctionPattern.variables()).map(Variable::reference)
                        .filter(Reference::isName).map(Reference::asName).toSet();
                Actor<Root.Conjunction> root = registry.rootConjunction(conjunctionPattern, filter, null, null, responses::add,
                                                                        iterDone -> doneReceived.incrementAndGet());

                for (int i = 0; i < answerCount; i++) {
                    AnswerState.DownstreamVars.Identity downstream = Initial.of(new ConceptMap()).toDownstreamVars();
                    root.tell(actor ->
                                      actor.receiveRequest(
                                              Request.create(new Request.Path(root, downstream), downstream, null),
                                              0)
                    );
                    ResolutionAnswer answer = responses.take();

                    // TODO write more meaningful explanation tests
                    System.out.println(answer);
                }
            }
        }
    }

    private Disjunction parseDisjunction(RocksTransaction transaction, String query) {
        Disjunction disjunction = Disjunction.create(Graql.parsePattern(query).asConjunction().normalise());
        disjunction.conjunctions().forEach(conj -> transaction.logic().typeResolver().resolve(conj));
        return disjunction;
    }

    private Conjunction parseConjunction(RocksTransaction transaction, String query) {
        Conjunction conjunction = Disjunction.create(Graql.parsePattern(query).asConjunction().normalise()).conjunctions().iterator().next();
        transaction.logic().typeResolver().resolve(conjunction);
        return conjunction;
    }

    private RocksSession schemaSession() {
        return grakn.session(database, Arguments.Session.Type.SCHEMA);
    }

    private RocksSession dataSession() {
        return grakn.session(database, Arguments.Session.Type.DATA);
    }

    private RocksTransaction singleThreadElgTransaction(RocksSession session) {
        RocksTransaction transaction = session.transaction(Arguments.Transaction.Type.WRITE);
        transaction.reasoner().resolverRegistry().setEventLoopGroup(new EventLoopGroup(1));
        return transaction;
    }

    private void createRootAndAssertResponses(RocksTransaction transaction, Disjunction disjunction,
                                              Set<Reference.Name> filter, @Nullable Long offset, @Nullable Long limit,
                                              long answerCount) throws InterruptedException {
        ResolverRegistry registry = transaction.reasoner().resolverRegistry();
        LinkedBlockingQueue<ResolutionAnswer> responses = new LinkedBlockingQueue<>();
        AtomicLong doneReceived = new AtomicLong(0L);
        Actor<Root.Disjunction> root =
                registry.rootDisjunction(disjunction, filter, offset, limit, responses::add, iterDone -> doneReceived.incrementAndGet());
        assertResponses(root, filter, responses, doneReceived, answerCount);
    }

    private void createRootAndAssertResponses(RocksTransaction transaction, Conjunction conjunction, @Nullable Long offset,
                                              @Nullable Long limit, long answerCount) throws InterruptedException {
        ResolverRegistry registry = transaction.reasoner().resolverRegistry();
        LinkedBlockingQueue<ResolutionAnswer> responses = new LinkedBlockingQueue<>();
        AtomicLong doneReceived = new AtomicLong(0L);
        Set<Reference.Name> filter = iterate(conjunction.variables()).map(Variable::reference).filter(Reference::isName)
                .map(Reference::asName).toSet();
        Actor<Root.Conjunction> root =
                registry.rootConjunction(conjunction, filter, offset, limit, responses::add, iterDone -> doneReceived.incrementAndGet());
        assertResponses(root, filter, responses, doneReceived, answerCount);
    }

    private void assertResponses(Actor<? extends Resolver<?>> root, Set<Reference.Name> filter, LinkedBlockingQueue<ResolutionAnswer> responses,
                                 AtomicLong doneReceived, long answerCount)
            throws InterruptedException {
        long startTime = System.currentTimeMillis();
        long n = answerCount + 1; //total number of traversal answers, plus one expected Exhausted (-1 answer)
        for (int i = 0; i < n; i++) {
            AnswerState.DownstreamVars.Identity downstream = Initial.of(new ConceptMap()).toDownstreamVars();
            root.tell(actor -> actor.receiveRequest(Request.create(
                    new Request.Path(root, downstream), downstream, ResolutionAnswer.Derivation.EMPTY
            ), 0));
        }
        int answersFound = 0;
        for (int i = 0; i < n - 1; i++) {
            ResolutionAnswer answer = responses.poll(1000, TimeUnit.MILLISECONDS); // polling prevents the test hanging
            if (answer != null) answersFound += 1;
        }
        Thread.sleep(1000);
        assertEquals(answerCount, answersFound);
        assertEquals(1, doneReceived.get());
        assertTrue(responses.isEmpty());
        System.out.println("Time : " + (System.currentTimeMillis() - startTime));
    }
}
