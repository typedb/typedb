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

package grakn.core.reasoner.resolution;

import grakn.common.concurrent.actor.Actor;
import grakn.common.concurrent.actor.EventLoopGroup;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.reasoner.resolution.framework.Request;
import grakn.core.reasoner.resolution.framework.ResolutionAnswer;
import grakn.core.reasoner.resolution.resolver.RootResolver;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static grakn.common.collection.Collections.list;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

public class ResolutionTest {

    @Test
    public void singleConcludable() throws InterruptedException {
        LinkedBlockingQueue<ResolutionAnswer> responses = new LinkedBlockingQueue<>();
        AtomicLong doneReceived = new AtomicLong(0L);
        EventLoopGroup elg = new EventLoopGroup(1, "reasoning-elg");
        ResolverRegistry registry = new ResolverRegistry(elg);

        long atomicPattern = 0L;
        long atomicTraversalSize = 5L;
        registerConcludable(atomicPattern, list(), atomicTraversalSize, registry);

        List<Long> conjunctionPattern = list(atomicPattern);
        long conjunctionTraversalSize = 5L; // hard coded internally

        Actor<RootResolver> root = registerRoot(conjunctionPattern, conjunctionTraversalSize, responses::add, doneReceived::incrementAndGet, registry);
        assertResponses(root, responses, doneReceived, atomicTraversalSize + conjunctionTraversalSize, registry);
    }


    @Test
    public void twoConcludables() throws InterruptedException {
        LinkedBlockingQueue<ResolutionAnswer> responses = new LinkedBlockingQueue<>();
        AtomicLong doneReceived = new AtomicLong(0L);
        EventLoopGroup elg = new EventLoopGroup(1, "reasoning-elg");
        ResolverRegistry registry = new ResolverRegistry(elg);

        long atomic1Pattern = 2L;
        long atomic1TraversalSize = 2L;
        registerConcludable(atomic1Pattern, list(), atomic1TraversalSize, registry);

        long atomic2Pattern = 20L;
        long atomic2TraversalSize = 2L;
        registerConcludable(atomic2Pattern, list(), atomic2TraversalSize, registry);

        List<Long> conjunctionPattern = list(atomic2Pattern, atomic1Pattern);
        long conjunctionTraversalSize = 0L;

        Actor<RootResolver> root = registerRoot(conjunctionPattern, conjunctionTraversalSize, responses::add, doneReceived::incrementAndGet, registry);
        assertResponses(root, responses, doneReceived, conjunctionTraversalSize + (atomic2TraversalSize * atomic1TraversalSize), registry);
    }

    @Test
    public void filteringConcludable() throws InterruptedException {
        LinkedBlockingQueue<ResolutionAnswer> responses = new LinkedBlockingQueue<>();
        AtomicLong doneReceived = new AtomicLong(0L);
        EventLoopGroup elg = new EventLoopGroup(1, "reasoning-elg");
        ResolverRegistry registry = new ResolverRegistry(elg);

        long atomic1Pattern = 2L;
        long atomic1TraversalSize = 2L;
        registerConcludable(atomic1Pattern, list(), atomic1TraversalSize, registry);

        long atomic2Pattern = 20L;
        long atomic2TraversalSize = 0L;
        registerConcludable(atomic2Pattern, list(), atomic2TraversalSize, registry);

        List<Long> conjunctionPattern = list(atomic2Pattern, atomic1Pattern);
        long conjunctionTraversalSize = 0L;

        Actor<RootResolver> root = registerRoot(conjunctionPattern, conjunctionTraversalSize, responses::add, doneReceived::incrementAndGet, registry);
        assertResponses(root, responses, doneReceived, conjunctionTraversalSize + (atomic1TraversalSize * atomic2TraversalSize), registry);
    }

    @Test
    public void simpleRule() throws InterruptedException {
        LinkedBlockingQueue<ResolutionAnswer> responses = new LinkedBlockingQueue<>();
        AtomicLong doneReceived = new AtomicLong(0L);
        EventLoopGroup elg = new EventLoopGroup(1, "reasoning-elg");
        ResolverRegistry registry = new ResolverRegistry(elg);

        long atomic1Pattern = -2L;
        long atomic1TraversalSize = 1L;
        registerConcludable(atomic1Pattern, list(), atomic1TraversalSize, registry);

        List<Long> rulePattern = list(-2L);
        long ruleTraversalSize = 0L;
        registerRule(rulePattern, ruleTraversalSize, registry);

        long atomic2Pattern = 2L;
        long atomic2TraversalSize = 1L;
        registerConcludable(atomic2Pattern, Arrays.asList(rulePattern), atomic2TraversalSize, registry);

        List<Long> conjunctionPattern = list(atomic2Pattern);
        long conjunctionTraversalSize = 0L;

        Actor<RootResolver> root = registerRoot(conjunctionPattern, conjunctionTraversalSize, responses::add, doneReceived::incrementAndGet, registry);
        long answerCount = conjunctionTraversalSize + atomic2TraversalSize + ruleTraversalSize + atomic1TraversalSize;
        assertResponses(root, responses, doneReceived, answerCount, registry);
    }

    @Test
    public void concludableChainWithRule() throws InterruptedException {
        LinkedBlockingQueue<ResolutionAnswer> responses = new LinkedBlockingQueue<>();
        AtomicLong doneReceived = new AtomicLong(0L);
        EventLoopGroup elg = new EventLoopGroup(1, "reasoning-elg");
        ResolverRegistry registry = new ResolverRegistry(elg);

        long atomic1Pattern = -2L;
        long atomic1TraversalSize = 1L;
        registerConcludable(atomic1Pattern, list(), atomic1TraversalSize, registry);

        List<Long> rulePattern = list(atomic1Pattern);
        long ruleTraversalSize = 1L;
        registerRule(rulePattern, ruleTraversalSize, registry);

        long atomic2Pattern = 2L;
        long atomic2TraversalSize = 1L;
        registerConcludable(atomic2Pattern, Arrays.asList(rulePattern), atomic2TraversalSize, registry);

        long atomic3Pattern = 20L;
        long atomic3TraversalSize = 1L;
        registerConcludable(atomic3Pattern, list(), atomic3TraversalSize, registry);

        List<Long> conjunctionPattern = list(atomic3Pattern, atomic2Pattern);
        long conjunctionTraversalSize = 0L;

        Actor<RootResolver> root = registerRoot(conjunctionPattern, conjunctionTraversalSize, responses::add, doneReceived::incrementAndGet, registry);
        long answerCount = conjunctionTraversalSize + (atomic3TraversalSize * (atomic2TraversalSize + ruleTraversalSize + atomic1TraversalSize));
        assertResponses(root, responses, doneReceived, answerCount, registry);
    }

    @Test
    public void shallowRerequestChain() throws InterruptedException {
        LinkedBlockingQueue<ResolutionAnswer> responses = new LinkedBlockingQueue<>();
        AtomicLong doneReceived = new AtomicLong(0L);
        EventLoopGroup elg = new EventLoopGroup(1, "reasoning-elg");
        ResolverRegistry registry = new ResolverRegistry(elg);

        long atomic1Pattern = 2L;
        long atomic1TraversalSize = 2L;
        registerConcludable(atomic1Pattern, list(), atomic1TraversalSize, registry);

        long atomic2Pattern = 20L;
        long atomic2TraversalSize = 2L;
        registerConcludable(atomic2Pattern, list(), atomic2TraversalSize, registry);

        long atomic3Pattern = 200L;
        long atomic3TraversalSize = 2L;
        registerConcludable(atomic3Pattern, list(), atomic3TraversalSize, registry);

        List<Long> conjunctionPattern = list(atomic3Pattern, atomic2Pattern, atomic1Pattern);
        long conjunctionTraversalSize = 0L;
        Actor<RootResolver> root = registerRoot(conjunctionPattern, conjunctionTraversalSize, responses::add, doneReceived::incrementAndGet, registry);

        long answerCount = conjunctionTraversalSize + (atomic3TraversalSize * atomic2TraversalSize * atomic1TraversalSize);
        assertResponses(root, responses, doneReceived, answerCount, registry);
    }

    @Test
    public void deepRerequestChain() throws InterruptedException {
        LinkedBlockingQueue<ResolutionAnswer> responses = new LinkedBlockingQueue<>();
        AtomicLong doneReceived = new AtomicLong(0L);
        EventLoopGroup elg = new EventLoopGroup(1, "reasoning-elg");
        ResolverRegistry registry = new ResolverRegistry(elg);

        long atomic1Pattern = 2L;
        long atomic1TraversalSize = 10L;
        registerConcludable(atomic1Pattern, list(), atomic1TraversalSize, registry);

        long atomic2Pattern = 20L;
        long atomic2TraversalSize = 10L;
        registerConcludable(atomic2Pattern, list(), atomic2TraversalSize, registry);

        long atomic3Pattern = 200L;
        long atomic3TraversalSize = 10L;
        registerConcludable(atomic3Pattern, list(), atomic3TraversalSize, registry);

        long atomic4Pattern = 2000L;
        long atomic4TraversalSize = 10L;
        registerConcludable(atomic4Pattern, list(), atomic4TraversalSize, registry);

        long atomic5Pattern = 20000L;
        long atomic5TraversalSize = 10L;
        registerConcludable(atomic5Pattern, list(), atomic5TraversalSize, registry);

        List<Long> conjunctionPattern = list(atomic5Pattern, atomic4Pattern, atomic3Pattern, atomic2Pattern, atomic1Pattern);
        long conjunctionTraversalSize = 0L;
        Actor<RootResolver> root = registerRoot(conjunctionPattern, conjunctionTraversalSize, responses::add, doneReceived::incrementAndGet, registry);

        long answerCount = conjunctionTraversalSize + (atomic5TraversalSize * atomic4TraversalSize * atomic3TraversalSize * atomic2TraversalSize * atomic1TraversalSize);
        assertResponses(root, responses, doneReceived, answerCount, registry);
    }

    @Test
    public void bulkActorCreation() throws InterruptedException {
        LinkedBlockingQueue<ResolutionAnswer> responses = new LinkedBlockingQueue<>();
        AtomicLong doneReceived = new AtomicLong(0L);
        EventLoopGroup elg = new EventLoopGroup(1, "reasoning-elg");
        ResolverRegistry registry = new ResolverRegistry(elg);

        long start = System.currentTimeMillis();

        long atomicPattern = 1L;
        List<List<Long>> atomicRulePatterns = new ArrayList<>();
        for (long i = 2L; i < 1000_000L; i++) {
            List<Long> pattern = list(i);
            atomicRulePatterns.add(pattern);
        }
        long atomicTraversalSize = 1L;
        registerConcludable(atomicPattern, atomicRulePatterns, atomicTraversalSize, registry);

        List<Long> conjunctionPattern = list(atomicPattern);
        long conjunctionTraversalSize = 0L;
        Actor<RootResolver> root = registerRoot(conjunctionPattern, conjunctionTraversalSize, responses::add, doneReceived::incrementAndGet, registry);

        root.tell(actor ->
                          actor.executeReceiveRequest(
                                  new Request(new Request.Path(root), new ConceptMap(), list(), null),
                                  registry
                          )
        );
        responses.take();

        long elapsed = System.currentTimeMillis() - start;

        System.out.println("elapsed = " + elapsed);
    }

    @Test
    public void recursiveTerminationAndDeduplication() throws InterruptedException {
        LinkedBlockingQueue<ResolutionAnswer> responses = new LinkedBlockingQueue<>();
        AtomicLong doneReceived = new AtomicLong(0L);
        EventLoopGroup elg = new EventLoopGroup(1, "reasoning-elg");
        ResolverRegistry registry = new ResolverRegistry(elg);

        long atomicPattern = 1L;

        List<Long> rulePattern = list(atomicPattern);
        long ruleTraversalSize = 1L;
        registerRule(rulePattern, ruleTraversalSize, registry);

        long atomic1TraversalSize = 1L;
        registerConcludable(atomicPattern, Arrays.asList(rulePattern), atomic1TraversalSize, registry);

        List<Long> conjunctionPattern = list(atomicPattern);
        long conjunctionTraversalSize = 0L;
        Actor<RootResolver> root = registerRoot(conjunctionPattern, conjunctionTraversalSize, responses::add, doneReceived::incrementAndGet, registry);

        // the recursively produced answers will be identical, so will be deduplicated
        long answerCount = conjunctionTraversalSize + atomic1TraversalSize + ruleTraversalSize + atomic1TraversalSize - atomic1TraversalSize;
        assertResponses(root, responses, doneReceived, answerCount, registry);
    }

    @Test
    public void answerRecorderTest() throws InterruptedException {
        LinkedBlockingQueue<ResolutionAnswer> responses = new LinkedBlockingQueue<>();
        AtomicLong doneReceived = new AtomicLong(0L);
        EventLoopGroup elg = new EventLoopGroup(1, "reasoning-elg");
        ResolverRegistry registry = new ResolverRegistry(elg);

        long atomic1Pattern = 10L;
        long atomic1TraversalSize = 1L;
        registerConcludable(atomic1Pattern, list(), atomic1TraversalSize, registry);

        List<Long> rulePattern = list(10L);
        long ruleTraversalSize = 0L;
        registerRule(rulePattern, ruleTraversalSize, registry);

        long atomic2Pattern = 2010L;
        long atomic2TraversalSize = 0L;
        registerConcludable(atomic2Pattern, Arrays.asList(rulePattern), atomic2TraversalSize, registry);

        List<Long> conjunctionPattern = list(atomic2Pattern);
        long conjunctionTraversalSize = 1L;
        Actor<RootResolver> root = registerRoot(conjunctionPattern, conjunctionTraversalSize, responses::add, doneReceived::incrementAndGet, registry);

        long answerCount = conjunctionTraversalSize + atomic2TraversalSize + ruleTraversalSize + atomic1TraversalSize;

        for (int i = 0; i < answerCount; i++) {
            root.tell(actor ->
                              actor.executeReceiveRequest(
                                      new Request(new Request.Path(root), new ConceptMap(), list(), null),
                                      registry
                              )
            );
            ResolutionAnswer answer = responses.take();

            // TODO write more meaningful explanation tests
            System.out.println(answer);
        }
    }


    private Actor<RootResolver> registerRoot(List<Long> pattern, long traversalSize, Consumer<ResolutionAnswer> onAnswer, Runnable onExhausted, ResolverRegistry resolverRegistry) {
        return resolverRegistry.createRoot(pattern, traversalSize, onAnswer, onExhausted);
    }

    private void registerConcludable(long pattern, List<List<Long>> rules, long traversalSize, ResolverRegistry registry) {
        registry.registerConcludable(pattern, rules, traversalSize);
    }

    private void registerRule(List<Long> pattern, long traversalSize, ResolverRegistry registry) {
        registry.registerRule(pattern, traversalSize);
    }

    private void assertResponses(final Actor<RootResolver> root, final LinkedBlockingQueue<ResolutionAnswer> responses,
                                 final AtomicLong doneReceived, final long answerCount, ResolverRegistry registry)
            throws InterruptedException {
        long startTime = System.currentTimeMillis();
        long n = answerCount + 1; //total number of traversal answers, plus one expected Exhausted (-1 answer)
        for (int i = 0; i < n; i++) {
            root.tell(actor ->
                              actor.executeReceiveRequest(
                                      new Request(new Request.Path(root), new ConceptMap(), list(), ResolutionAnswer.Derivation.EMPTY),
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
