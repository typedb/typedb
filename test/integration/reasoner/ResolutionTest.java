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
 */

package grakn.core.reasoner;

import grakn.common.concurrent.actor.Actor;
import grakn.common.concurrent.actor.EventLoopGroup;
import grakn.core.Grakn;
import grakn.core.common.parameters.Arguments;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.pattern.Conjunction;
import grakn.core.pattern.Disjunction;
import grakn.core.reasoner.resolution.ResolverRegistry;
import grakn.core.reasoner.resolution.answer.NoOpAggregator;
import grakn.core.reasoner.resolution.framework.Request;
import grakn.core.reasoner.resolution.framework.ResolutionAnswer;
import grakn.core.reasoner.resolution.resolver.RootResolver;
import grakn.core.rocks.RocksGrakn;
import grakn.core.test.integration.util.Util;
import graql.lang.Graql;
import graql.lang.query.GraqlDefine;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static grakn.common.collection.Collections.list;
import static java.nio.charset.StandardCharsets.UTF_8;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

public class ResolutionTest {

    private static Path directory = Paths.get(System.getProperty("user.dir")).resolve("resolution-test");
    private static String database = "resolution-test";

    @Test
    public void singleConcludable() throws InterruptedException, IOException {
        Util.resetDirectory(directory);

        try (Grakn grakn = RocksGrakn.open(directory)) {
            grakn.databases().create(database);

            try (Grakn.Session session = grakn.session(database, Arguments.Session.Type.SCHEMA)) {

                try (Grakn.Transaction transaction = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    transaction.query().define(Graql.parseQuery(
                            "define person sub entity, owns name, owns age;" +
                                    "name sub attribute, value string;" +
                                    "age sub attribute, value long;" +
                                    "rule alices-are-42: when { $p isa person, has name \"Alice\"; } then { $p has age 42; };"));
                    transaction.commit();
                }
            }
            try (Grakn.Session session = grakn.session(database, Arguments.Session.Type.DATA)) {
                try (Grakn.Transaction transaction = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    transaction.query().insert(Graql.parseQuery(
                            "insert $p1 isa person, has name \"Alice\";" +
                                    "$p2 isa person, has name \"Alice\";" +
                                    "$p3 isa person, has name \"Alice\";" +
                                    "$p4 isa person, has name \"Alice\";" +
                                    "$p5 isa person, has name \"Alice\";" +
                                    "$p6 isa person, has name \"Brian\", has age 24;" +
                                    "$p7 isa person, has name \"Brian\", has age 24;" +
                                    "$p8 isa person, has name \"Brian\", has age 24;" +
                                    "$p9 isa person, has name \"Brian\", has age 24;" +
                                    "$p10 isa person, has name \"Brian\", has age 24;"
                    ));

                    transaction.commit();
                }
            }
            Conjunction conjunctionPattern = parseConjunction("{ $x has age $a; }");

            LinkedBlockingQueue<ResolutionAnswer> responses = new LinkedBlockingQueue<>();
            AtomicLong doneReceived = new AtomicLong(0L);
            EventLoopGroup elg = new EventLoopGroup(1, "reasoning-elg");
            ResolverRegistry registry = new ResolverRegistry(elg);
            Actor<RootResolver> root = registerRoot(conjunctionPattern, responses::add, doneReceived::incrementAndGet, registry);
            assertResponses(root, responses, doneReceived, 10L, registry);
        }
    }


//    @Test
//    public void twoConcludables() throws InterruptedException {
//        LinkedBlockingQueue<ResolutionAnswer> responses = new LinkedBlockingQueue<>();
//        AtomicLong doneReceived = new AtomicLong(0L);
//        EventLoopGroup elg = new EventLoopGroup(1, "reasoning-elg");
//        ResolverRegistry registry = new ResolverRegistry(elg);
//
//        long atomic1Pattern = 2L;
//        long atomic1TraversalAnswerCount = 2L;
//        registerConcludable(atomic1Pattern, list(), atomic1TraversalAnswerCount, registry);
//
//        long atomic2Pattern = 20L;
//        long atomic2TraversalAnswerCount = 2L;
//        registerConcludable(atomic2Pattern, list(), atomic2TraversalAnswerCount, registry);
//
//        List<Long> conjunctionPattern = list(atomic2Pattern, atomic1Pattern);
//        long conjunctionTraversalAnswerCount = 0L;
//
//        Actor<RootResolver> root = registerRoot(conjunctionPattern, responses::add, doneReceived::incrementAndGet, registry);
//        assertResponses(root, responses, doneReceived, conjunctionTraversalAnswerCount + (atomic2TraversalAnswerCount * atomic1TraversalAnswerCount), registry);
//    }
//
//    @Test
//    public void filteringConcludable() throws InterruptedException {
//        LinkedBlockingQueue<ResolutionAnswer> responses = new LinkedBlockingQueue<>();
//        AtomicLong doneReceived = new AtomicLong(0L);
//        EventLoopGroup elg = new EventLoopGroup(1, "reasoning-elg");
//        ResolverRegistry registry = new ResolverRegistry(elg);
//
//        long atomic1Pattern = 2L;
//        long atomic1TraversalAnswerCount = 2L;
//        registerConcludable(atomic1Pattern, list(), atomic1TraversalAnswerCount, registry);
//
//        long atomic2Pattern = 20L;
//        long atomic2TraversalAnswerCount = 0L;
//        registerConcludable(atomic2Pattern, list(), atomic2TraversalAnswerCount, registry);
//
//        List<Long> conjunctionPattern = list(atomic2Pattern, atomic1Pattern);
//        long conjunctionTraversalAnswerCount = 0L;
//
//        Actor<RootResolver> root = registerRoot(conjunctionPattern, responses::add, doneReceived::incrementAndGet, registry);
//        assertResponses(root, responses, doneReceived, conjunctionTraversalAnswerCount + (atomic1TraversalAnswerCount * atomic2TraversalAnswerCount), registry);
//    }
//
//    @Test
//    public void simpleRule() throws InterruptedException {
//        LinkedBlockingQueue<ResolutionAnswer> responses = new LinkedBlockingQueue<>();
//        AtomicLong doneReceived = new AtomicLong(0L);
//        EventLoopGroup elg = new EventLoopGroup(1, "reasoning-elg");
//        ResolverRegistry registry = new ResolverRegistry(elg);
//
//        long atomic1Pattern = -2L;
//        long atomic1TraversalAnswerCount = 1L;
//        registerConcludable(atomic1Pattern, list(), atomic1TraversalAnswerCount, registry);
//
//        List<Long> rulePattern = list(-2L);
//        long ruleTraversalAnswerCount = 0L;
//        registerRule(rulePattern, ruleTraversalAnswerCount, registry);
//
//        long atomic2Pattern = 2L;
//        long atomic2TraversalAnswerCount = 1L;
//        registerConcludable(atomic2Pattern, Arrays.asList(rulePattern), atomic2TraversalAnswerCount, registry);
//
//        List<Long> conjunctionPattern = list(atomic2Pattern);
//        long conjunctionTraversalAnswerCount = 0L;
//
//        Actor<RootResolver> root = registerRoot(conjunctionPattern, responses::add, doneReceived::incrementAndGet, registry);
//        long answerCount = conjunctionTraversalAnswerCount + atomic2TraversalAnswerCount + ruleTraversalAnswerCount + atomic1TraversalAnswerCount;
//        assertResponses(root, responses, doneReceived, answerCount, registry);
//    }
//
//    @Test
//    public void concludableChainWithRule() throws InterruptedException {
//        LinkedBlockingQueue<ResolutionAnswer> responses = new LinkedBlockingQueue<>();
//        AtomicLong doneReceived = new AtomicLong(0L);
//        EventLoopGroup elg = new EventLoopGroup(1, "reasoning-elg");
//        ResolverRegistry registry = new ResolverRegistry(elg);
//
//        long atomic1Pattern = -2L;
//        long atomic1TraversalAnswerCount = 1L;
//        registerConcludable(atomic1Pattern, list(), atomic1TraversalAnswerCount, registry);
//
//        List<Long> rulePattern = list(atomic1Pattern);
//        long ruleTraversalAnswerCount = 1L;
//        registerRule(rulePattern, ruleTraversalAnswerCount, registry);
//
//        long atomic2Pattern = 2L;
//        long atomic2TraversalAnswerCount = 1L;
//        registerConcludable(atomic2Pattern, Arrays.asList(rulePattern), atomic2TraversalAnswerCount, registry);
//
//        long atomic3Pattern = 20L;
//        long atomic3TraversalAnswerCount = 1L;
//        registerConcludable(atomic3Pattern, list(), atomic3TraversalAnswerCount, registry);
//
//        List<Long> conjunctionPattern = list(atomic3Pattern, atomic2Pattern);
//        long conjunctionTraversalAnswerCount = 0L;
//
//        Actor<RootResolver> root = registerRoot(conjunctionPattern, responses::add, doneReceived::incrementAndGet, registry);
//        long answerCount = conjunctionTraversalAnswerCount + (atomic3TraversalAnswerCount * (atomic2TraversalAnswerCount + ruleTraversalAnswerCount + atomic1TraversalAnswerCount));
//        assertResponses(root, responses, doneReceived, answerCount, registry);
//    }
//
//    @Test
//    public void shallowRerequestChain() throws InterruptedException {
//        LinkedBlockingQueue<ResolutionAnswer> responses = new LinkedBlockingQueue<>();
//        AtomicLong doneReceived = new AtomicLong(0L);
//        EventLoopGroup elg = new EventLoopGroup(1, "reasoning-elg");
//        ResolverRegistry registry = new ResolverRegistry(elg);
//
//        long atomic1Pattern = 2L;
//        long atomic1TraversalAnswerCount = 2L;
//        registerConcludable(atomic1Pattern, list(), atomic1TraversalAnswerCount, registry);
//
//        long atomic2Pattern = 20L;
//        long atomic2TraversalAnswerCount = 2L;
//        registerConcludable(atomic2Pattern, list(), atomic2TraversalAnswerCount, registry);
//
//        long atomic3Pattern = 200L;
//        long atomic3TraversalAnswerCount = 2L;
//        registerConcludable(atomic3Pattern, list(), atomic3TraversalAnswerCount, registry);
//
//        List<Long> conjunctionPattern = list(atomic3Pattern, atomic2Pattern, atomic1Pattern);
//        long conjunctionTraversalAnswerCount = 0L;
//        Actor<RootResolver> root = registerRoot(conjunctionPattern, responses::add, doneReceived::incrementAndGet, registry);
//
//        long answerCount = conjunctionTraversalAnswerCount + (atomic3TraversalAnswerCount * atomic2TraversalAnswerCount * atomic1TraversalAnswerCount);
//        assertResponses(root, responses, doneReceived, answerCount, registry);
//    }
//
//    @Test
//    public void deepRerequestChain() throws InterruptedException {
//        LinkedBlockingQueue<ResolutionAnswer> responses = new LinkedBlockingQueue<>();
//        AtomicLong doneReceived = new AtomicLong(0L);
//        EventLoopGroup elg = new EventLoopGroup(1, "reasoning-elg");
//        ResolverRegistry registry = new ResolverRegistry(elg);
//
//        long atomic1Pattern = 2L;
//        long atomic1TraversalAnswerCount = 10L;
//        registerConcludable(atomic1Pattern, list(), atomic1TraversalAnswerCount, registry);
//
//        long atomic2Pattern = 20L;
//        long atomic2TraversalAnswerCount = 10L;
//        registerConcludable(atomic2Pattern, list(), atomic2TraversalAnswerCount, registry);
//
//        long atomic3Pattern = 200L;
//        long atomic3TraversalAnswerCount = 10L;
//        registerConcludable(atomic3Pattern, list(), atomic3TraversalAnswerCount, registry);
//
//        long atomic4Pattern = 2000L;
//        long atomic4TraversalAnswerCount = 10L;
//        registerConcludable(atomic4Pattern, list(), atomic4TraversalAnswerCount, registry);
//
//        long atomic5Pattern = 20000L;
//        long atomic5TraversalAnswerCount = 10L;
//        registerConcludable(atomic5Pattern, list(), atomic5TraversalAnswerCount, registry);
//
//        List<Long> conjunctionPattern = list(atomic5Pattern, atomic4Pattern, atomic3Pattern, atomic2Pattern, atomic1Pattern);
//        long conjunctionTraversalAnswerCount = 0L;
//        Actor<RootResolver> root = registerRoot(conjunctionPattern, responses::add, doneReceived::incrementAndGet, registry);
//
//        long answerCount = conjunctionTraversalAnswerCount + (atomic5TraversalAnswerCount * atomic4TraversalAnswerCount * atomic3TraversalAnswerCount * atomic2TraversalAnswerCount * atomic1TraversalAnswerCount);
//        assertResponses(root, responses, doneReceived, answerCount, registry);
//    }
//
//    @Test
//    public void bulkActorCreation() throws InterruptedException {
//        LinkedBlockingQueue<ResolutionAnswer> responses = new LinkedBlockingQueue<>();
//        AtomicLong doneReceived = new AtomicLong(0L);
//        EventLoopGroup elg = new EventLoopGroup(1, "reasoning-elg");
//        ResolverRegistry registry = new ResolverRegistry(elg);
//
//        long start = System.currentTimeMillis();
//
//        long atomicPattern = 1L;
//        List<List<Long>> atomicRulePatterns = new ArrayList<>();
//        for (long i = 2L; i < 1000_000L; i++) {
//            List<Long> pattern = list(i);
//            atomicRulePatterns.add(pattern);
//        }
//        long atomicTraversalAnswerCount = 1L;
//        registerConcludable(atomicPattern, atomicRulePatterns, atomicTraversalAnswerCount, registry);
//
//        List<Long> conjunctionPattern = list(atomicPattern);
//        long conjunctionTraversalAnswerCount = 0L;
//        Actor<RootResolver> root = registerRoot(conjunctionPattern, responses::add, doneReceived::incrementAndGet, registry);
//
//        root.tell(actor ->
//                          actor.executeReceiveRequest(
//                                  new Request(new Request.Path(root), new ConceptMap(), null),
//                                  registry
//                          )
//        );
//        responses.take();
//
//        long elapsed = System.currentTimeMillis() - start;
//
//        System.out.println("elapsed = " + elapsed);
//    }
//
//    @Test
//    public void recursiveTerminationAndDeduplication() throws InterruptedException {
//        LinkedBlockingQueue<ResolutionAnswer> responses = new LinkedBlockingQueue<>();
//        AtomicLong doneReceived = new AtomicLong(0L);
//        EventLoopGroup elg = new EventLoopGroup(1, "reasoning-elg");
//        ResolverRegistry registry = new ResolverRegistry(elg);
//
//        long atomicPattern = 1L;
//
//        List<Long> rulePattern = list(atomicPattern);
//        long ruleTraversalAnswerCount = 1L;
//        registerRule(rulePattern, ruleTraversalAnswerCount, registry);
//
//        long atomic1TraversalAnswerCount = 1L;
//        registerConcludable(atomicPattern, Arrays.asList(rulePattern), atomic1TraversalAnswerCount, registry);
//
//        List<Long> conjunctionPattern = list(atomicPattern);
//        long conjunctionTraversalAnswerCount = 0L;
//        Actor<RootResolver> root = registerRoot(conjunctionPattern, responses::add, doneReceived::incrementAndGet, registry);
//
//        // the recursively produced answers will be identical, so will be deduplicated
//        long answerCount = conjunctionTraversalAnswerCount + atomic1TraversalAnswerCount + ruleTraversalAnswerCount + atomic1TraversalAnswerCount - atomic1TraversalAnswerCount;
//        assertResponses(root, responses, doneReceived, answerCount, registry);
//    }
//
//    @Test
//    public void answerRecorderTest() throws InterruptedException {
//        LinkedBlockingQueue<ResolutionAnswer> responses = new LinkedBlockingQueue<>();
//        AtomicLong doneReceived = new AtomicLong(0L);
//        EventLoopGroup elg = new EventLoopGroup(1, "reasoning-elg");
//        ResolverRegistry registry = new ResolverRegistry(elg);
//
//        long atomic1Pattern = 10L;
//        long atomic1TraversalAnswerCount = 1L;
//        registerConcludable(atomic1Pattern, list(), atomic1TraversalAnswerCount, registry);
//
//        List<Long> rulePattern = list(10L);
//        long ruleTraversalAnswerCount = 0L;
//        registerRule(rulePattern, ruleTraversalAnswerCount, registry);
//
//        long atomic2Pattern = 2010L;
//        long atomic2TraversalAnswerCount = 0L;
//        registerConcludable(atomic2Pattern, Arrays.asList(rulePattern), atomic2TraversalAnswerCount, registry);
//
//        List<Long> conjunctionPattern = list(atomic2Pattern);
//        long conjunctionTraversalAnswerCount = 1L;
//        Actor<RootResolver> root = registerRoot(conjunctionPattern, responses::add, doneReceived::incrementAndGet, registry);
//
//        long answerCount = conjunctionTraversalAnswerCount + atomic2TraversalAnswerCount + ruleTraversalAnswerCount + atomic1TraversalAnswerCount;
//
//        for (int i = 0; i < answerCount; i++) {
//            root.tell(actor ->
//                              actor.executeReceiveRequest(
//                                      new Request(new Request.Path(root), new ConceptMap(), null),
//                                      registry
//                              )
//            );
//            ResolutionAnswer answer = responses.take();
//
//            // TODO write more meaningful explanation tests
//            System.out.println(answer);
//        }
//    }

    private Conjunction parseConjunction(String query) {
        return Disjunction.create(Graql.parsePattern(query).asConjunction().normalise()).conjunctions().iterator().next();
    }

    private Actor<RootResolver> registerRoot(Conjunction pattern, Consumer<ResolutionAnswer> onAnswer, Runnable onExhausted, ResolverRegistry resolverRegistry) {
        return resolverRegistry.createRoot(pattern, onAnswer, onExhausted);
    }

//    private void registerConcludable(long pattern, List<List<Long>> rules, long traversalAnswerCount, ResolverRegistry registry) {
//        registry.registerConcludable(pattern, rules, traversalAnswerCount);
//    }
//
//    private void registerRule(List<Long> pattern, long traversalAnswerCount, ResolverRegistry registry) {
//        registry.registerRule(pattern, traversalAnswerCount);
//    }

    private void assertResponses(final Actor<RootResolver> root, final LinkedBlockingQueue<ResolutionAnswer> responses,
                                 final AtomicLong doneReceived, final long answerCount, ResolverRegistry registry)
            throws InterruptedException {
        long startTime = System.currentTimeMillis();
        long n = answerCount + 1; //total number of traversal answers, plus one expected Exhausted (-1 answer)
        for (int i = 0; i < n; i++) {
            root.tell(actor ->
                              actor.executeReceiveRequest(
                                      new Request(new Request.Path(root), NoOpAggregator.create(), ResolutionAnswer.Derivation.EMPTY),
                                      registry
                              )
            );
        }

        for (int i = 0; i < n - 1; i++) {
            ResolutionAnswer answer = responses.take();
        }
        Thread.sleep(1000);
        assertEquals(1, doneReceived.get());
        assertTrue(responses.isEmpty());
        System.out.println("Time : " + (System.currentTimeMillis() - startTime));
    }
}
