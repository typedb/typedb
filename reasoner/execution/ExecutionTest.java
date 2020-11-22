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

package grakn.core.reasoner.execution;

import grakn.common.concurrent.actor.Actor;
import grakn.common.concurrent.actor.EventLoopGroup;
import grakn.core.reasoner.execution.actor.Concludable;
import grakn.core.reasoner.execution.actor.Root;
import grakn.core.reasoner.execution.actor.Rule;
import grakn.core.reasoner.execution.framework.Request;
import grakn.core.reasoner.execution.framework.Response;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import static grakn.common.collection.Collections.list;
import static junit.framework.TestCase.assertTrue;

public class ExecutionTest {

    @Test
    public void singleConcludable() throws InterruptedException {
        LinkedBlockingQueue<Response> responses = new LinkedBlockingQueue<>();
        EventLoopGroup elg = new EventLoopGroup(1, "reasoning-elg");
        Registry registry = new Registry(elg);

        long atomicPattern = 0L;
        long atomicTraversalSize = 5L;
        registerConcludable(atomicPattern, list(), atomicTraversalSize, registry, elg);

        List<Long> conjunctionPattern = list(atomicPattern);
        long conjunctionTraversalSize = 5L;
        Actor<Root> conjunction = registerConjunction(conjunctionPattern, conjunctionTraversalSize, -10L, responses, elg);

        assertResponsesSync(conjunction, atomicTraversalSize + conjunctionTraversalSize, responses, registry);
    }

    @Test
    public void twoConcludables() throws InterruptedException {
        LinkedBlockingQueue<Response> responses = new LinkedBlockingQueue<>();
        EventLoopGroup elg = new EventLoopGroup(1, "reasoning-elg");
        Registry registry = new Registry(elg);

        long atomic1Pattern = 2L;
        long atomic1TraversalSize = 2L;
        registerConcludable(atomic1Pattern, list(), atomic1TraversalSize, registry, elg);

        long atomic2Pattern = 20L;
        long atomic2TraversalSize = 2L;
        registerConcludable(atomic2Pattern, list(), atomic2TraversalSize, registry, elg);

        List<Long> conjunctionPattern = list(atomic2Pattern, atomic1Pattern);
        long conjunctionTraversalSize = 0L;
        Actor<Root> conjunction = registerConjunction(conjunctionPattern, conjunctionTraversalSize,0L,  responses, elg);

        assertResponses(conjunction, conjunctionTraversalSize + (atomic2TraversalSize * atomic1TraversalSize), responses, registry);
    }

    @Test
    public void filteringConcludable() throws InterruptedException {
        LinkedBlockingQueue<Response> responses = new LinkedBlockingQueue<>();
        EventLoopGroup elg = new EventLoopGroup(1, "reasoning-elg");
        Registry registry = new Registry(elg);

        long atomic1Pattern = 2L;
        long atomic1TraversalSize = 2L;
        registerConcludable(atomic1Pattern, list(), atomic1TraversalSize, registry, elg);

        long atomic2Pattern = 20L;
        long atomic2TraversalSize = 0L;
        registerConcludable(atomic2Pattern, list(), atomic2TraversalSize, registry, elg);

        List<Long> conjunctionPattern = list(atomic2Pattern, atomic1Pattern);
        long conjunctionTraversalSize = 0L;
        Actor<Root> conjunction = registerConjunction(conjunctionPattern, conjunctionTraversalSize,0L,  responses, elg);

        assertResponses(conjunction, conjunctionTraversalSize + (atomic1TraversalSize * atomic2TraversalSize), responses, registry);
    }

    @Test
    public void simpleRule() throws InterruptedException {
        LinkedBlockingQueue<Response> responses = new LinkedBlockingQueue<>();
        EventLoopGroup elg = new EventLoopGroup(1, "reasoning-elg");
        Registry registry = new Registry(elg);

        long atomic1Pattern = -2L;
        long atomic1TraversalSize = 1L;
        registerConcludable(atomic1Pattern, list(), atomic1TraversalSize, registry, elg);

        List<Long> rulePattern = list(-2L);
        long ruleTraversalSize = 0L;
        long ruleTraversalOffset = 0L;
        registerRule(rulePattern, ruleTraversalSize, ruleTraversalOffset, registry, elg);

        long atomic2Pattern = 2L;
        long atomic2TraversalSize = 1L;
        registerConcludable(atomic2Pattern, Arrays.asList(rulePattern), atomic2TraversalSize, registry, elg);

        List<Long> conjunctionPattern = list(atomic2Pattern);
        long conjunctionTraversalSize = 0L;
        long conjunctionTraversalOffset = 0L;
        Actor<Root> conjunction = registerConjunction(conjunctionPattern, conjunctionTraversalSize, conjunctionTraversalOffset, responses, elg);

        long answerCount = conjunctionTraversalSize + atomic2TraversalSize + ruleTraversalSize + atomic1TraversalSize;
        assertResponses(conjunction, answerCount, responses, registry);
    }

    @Test
    public void concludableChainWithRule() throws InterruptedException {
        LinkedBlockingQueue<Response> responses = new LinkedBlockingQueue<>();
        EventLoopGroup elg = new EventLoopGroup(1, "reasoning-elg");
        Registry registry = new Registry(elg);

        long atomic1Pattern = -2L;
        long atomic1TraversalSize = 1L;
        registerConcludable(atomic1Pattern, list(), atomic1TraversalSize, registry, elg);

        List<Long> rulePattern = list(atomic1Pattern);
        long ruleTraversalSize = 1L;
        long ruleTraversalOffset = -10L;
        registerRule(rulePattern, ruleTraversalSize, ruleTraversalOffset, registry, elg);

        long atomic2Pattern = 2L;
        long atomic2TraversalSize = 1L;
        registerConcludable(atomic2Pattern, Arrays.asList(rulePattern), atomic2TraversalSize, registry, elg);

        long atomic3Pattern = 20L;
        long atomic3TraversalSize = 1L;
        registerConcludable(atomic3Pattern, list(), atomic3TraversalSize, registry, elg);

        List<Long> conjunctionPattern = list(atomic3Pattern, atomic2Pattern);
        long conjunctionTraversalSize = 0L;
        long conjunctionTraversalOffset = 0L;
        Actor<Root> conjunction = registerConjunction(conjunctionPattern, conjunctionTraversalSize, conjunctionTraversalOffset, responses, elg);

        long answerCount = conjunctionTraversalSize + (atomic3TraversalSize * (atomic2TraversalSize + ruleTraversalSize + atomic1TraversalSize));
        assertResponses(conjunction, answerCount, responses, registry);
    }

    @Test
    public void shallowRerequestChain() throws InterruptedException {
        LinkedBlockingQueue<Response> responses = new LinkedBlockingQueue<>();
        EventLoopGroup elg = new EventLoopGroup(1, "reasoning-elg");
        Registry registry = new Registry(elg);

        long atomic1Pattern = 2L;
        long atomic1TraversalSize = 2L;
        registerConcludable(atomic1Pattern, list(), atomic1TraversalSize, registry, elg);

        long atomic2Pattern = 20L;
        long atomic2TraversalSize = 2L;
        registerConcludable(atomic2Pattern, list(), atomic2TraversalSize, registry, elg);

        long atomic3Pattern = 200L;
        long atomic3TraversalSize = 2L;
        registerConcludable(atomic3Pattern, list(), atomic3TraversalSize, registry, elg);

        List<Long> conjunctionPattern = list(atomic3Pattern, atomic2Pattern, atomic1Pattern);
        long conjunctionTraversalSize = 0L;
        long conjunctionTraversalOffset = 0L;
        Actor<Root> conjunction = registerConjunction(conjunctionPattern, conjunctionTraversalSize, conjunctionTraversalOffset, responses, elg);

        long answerCount = conjunctionTraversalSize + (atomic3TraversalSize * atomic2TraversalSize * atomic1TraversalSize);
        assertResponses(conjunction, answerCount, responses, registry);
    }

    @Test
    public void deepRerequestChain() throws InterruptedException {
        LinkedBlockingQueue<Response> responses = new LinkedBlockingQueue<>();
        EventLoopGroup elg = new EventLoopGroup(1, "reasoning-elg");
        Registry registry = new Registry(elg);

        long atomic1Pattern = 2L;
        long atomic1TraversalSize = 10L;
        registerConcludable(atomic1Pattern, list(), atomic1TraversalSize, registry, elg);

        long atomic2Pattern = 20L;
        long atomic2TraversalSize = 10L;
        registerConcludable(atomic2Pattern, list(), atomic2TraversalSize, registry, elg);

        long atomic3Pattern = 200L;
        long atomic3TraversalSize = 10L;
        registerConcludable(atomic3Pattern, list(), atomic3TraversalSize, registry, elg);

        long atomic4Pattern = 2000L;
        long atomic4TraversalSize = 10L;
        registerConcludable(atomic4Pattern, list(), atomic4TraversalSize, registry, elg);

        long atomic5Pattern = 20000L;
        long atomic5TraversalSize = 10L;
        registerConcludable(atomic5Pattern, list(), atomic5TraversalSize, registry, elg);

        List<Long> conjunctionPattern = list(atomic5Pattern, atomic4Pattern, atomic3Pattern, atomic2Pattern, atomic1Pattern);
        long conjunctionTraversalSize = 0L;
        long conjunctionTraversalOffset = 0L;
        Actor<Root> conjunction = registerConjunction(conjunctionPattern, conjunctionTraversalSize, conjunctionTraversalOffset, responses, elg);

        long answerCount = conjunctionTraversalSize + (atomic5TraversalSize * atomic4TraversalSize * atomic3TraversalSize * atomic2TraversalSize * atomic1TraversalSize);
        assertResponses(conjunction, answerCount, responses, registry);
    }

    @Test
    public void bulkActorCreation() throws InterruptedException {
        LinkedBlockingQueue<Response> responses = new LinkedBlockingQueue<>();
        EventLoopGroup elg = new EventLoopGroup(1, "reasoning-elg");
        Registry registry = new Registry(elg);

        long start = System.currentTimeMillis();

        long atomicPattern = 1L;
        List<List<Long>> atomicRulePatterns = new ArrayList<>();
        for (long i = 2L; i < 1000_000L; i++) {
            List<Long> pattern = list(i);
            atomicRulePatterns.add(pattern);
        }
        long atomicTraversalSize = 1L;
        registerConcludable(atomicPattern, atomicRulePatterns, atomicTraversalSize, registry, elg);

        List<Long> conjunctionPattern = list(atomicPattern);
        long conjunctionTraversalSize = 0L;
        long conjunctionTraversalOffset = 0L;
        Actor<Root> conjunction = registerConjunction(conjunctionPattern, conjunctionTraversalSize, conjunctionTraversalOffset, responses, elg);

        conjunction.tell(actor ->
                actor.executeReceiveRequest(
                        new Request(new Request.Path(conjunction), list(), list(), null),
                        registry
                )
        );
        responses.take();

        long elapsed = System.currentTimeMillis() - start;

        System.out.println("elapsed = " + elapsed);
    }

    @Test
    public void recursiveRequestsTermination() throws InterruptedException {
        LinkedBlockingQueue<Response> responses = new LinkedBlockingQueue<>();
        EventLoopGroup elg = new EventLoopGroup(1, "reasoning-elg");
        Registry registry = new Registry(elg);

        long atomicPattern = 1L;

        List<Long> rulePattern = list(atomicPattern);
        long ruleTraversalSize = 1L;
        long ruleTraversalOffset = 0L;
        registerRule(rulePattern, ruleTraversalSize, ruleTraversalOffset, registry, elg);

        long atomic1TraversalSize = 1L;
        registerConcludable(atomicPattern, Arrays.asList(rulePattern), atomic1TraversalSize, registry, elg);

        List<Long> conjunctionPattern = list(atomicPattern);
        long conjunctionTraversalSize = 0L;
        long conjunctionTraversalOffset = 0L;
        Actor<Root> conjunction = registerConjunction(conjunctionPattern, conjunctionTraversalSize, conjunctionTraversalOffset, responses, elg);

        long answerCount = conjunctionTraversalSize + atomic1TraversalSize + ruleTraversalSize + atomic1TraversalSize;
        assertResponses(conjunction, answerCount, responses, registry);
    }

    @Test
    public void answerDeduplication() throws InterruptedException {
        LinkedBlockingQueue<Response> responses = new LinkedBlockingQueue<>();
        EventLoopGroup elg = new EventLoopGroup(1, "reasoning-elg");
        Registry registry = new Registry(elg);

        long traversalSize = 100L;

        long atomicPattern = 1L;
        long atomicTraversalSize = traversalSize;
        registerConcludable(atomicPattern, list(), atomicTraversalSize, registry, elg);

        List<Long> conjunctionPattern = list(atomicPattern);
        long conjunctionTraversalSize = traversalSize;
        long conjunctionTraversalOffset = 1L;
        Actor<Root> conjunction = registerConjunction(conjunctionPattern, conjunctionTraversalSize, conjunctionTraversalOffset, responses, elg);

        long answerCount = traversalSize;
        assertResponses(conjunction, answerCount, responses, registry);
    }

    @Test
    public void answerRecorderTest() throws InterruptedException {
        LinkedBlockingQueue<Response> responses = new LinkedBlockingQueue<>();
        EventLoopGroup elg = new EventLoopGroup(1, "reasoning-elg");
        Registry registry = new Registry(elg);

        long atomic1Pattern = 10L;
        long atomic1TraversalSize = 1L;
        registerConcludable(atomic1Pattern, list(), atomic1TraversalSize, registry, elg);

        List<Long> rulePattern = list(10L);
        long ruleTraversalSize = 0L;
        long ruleTraversalOffset = 0L;
        registerRule(rulePattern, ruleTraversalSize, ruleTraversalOffset, registry, elg);

        long atomic2Pattern = 2010L;
        long atomic2TraversalSize = 0L;
        registerConcludable(atomic2Pattern, Arrays.asList(rulePattern), atomic2TraversalSize, registry, elg);

        List<Long> conjunctionPattern = list(atomic2Pattern);
        long conjunctionTraversalSize = 1L;
        long conjunctionTraversalOffset = -10L;
        Actor<Root> conjunction = registerConjunction(conjunctionPattern, conjunctionTraversalSize, conjunctionTraversalOffset, responses, elg);

        long answerCount = conjunctionTraversalSize + atomic2TraversalSize + ruleTraversalSize + atomic1TraversalSize;

        for (int i = 0; i < answerCount; i++) {
            conjunction.tell(actor ->
                    actor.executeReceiveRequest(
                            new Request(new Request.Path(conjunction), list(), list(), null),
                            registry
                    )
            );
            Response answer = responses.take();

            // TODO write more meaningful explanation tests
            System.out.println(answer);
        }
    }


    private void registerConcludable(long pattern, List<List<Long>> rules, long traversalSize, Registry registry, EventLoopGroup elg) {
        registry.registerConcludable(pattern, p -> Actor.create(elg, self -> new Concludable(self, p, rules, traversalSize)));
    }

    private Actor<Root> registerConjunction(List<Long> pattern, long traversalSize, long traversalOffset, LinkedBlockingQueue<Response> responses, EventLoopGroup elg) {
        return Actor.create(elg, self -> new Root(self, pattern, traversalSize, traversalOffset, responses));
    }

    private void registerRule(List<Long> pattern, long traversalSize, long traversalOffset, Registry registry, EventLoopGroup elg) {
        registry.registerRule(pattern, p -> Actor.create(elg, self -> new Rule(self, p, traversalSize, traversalOffset)));
    }

    private void assertResponses(Actor<Root> conjunction, long answerCount, LinkedBlockingQueue<Response> responses, Registry registry) throws InterruptedException {
        long startTime = System.currentTimeMillis();
        long n = answerCount + 1; //total number of traversal answers, plus one expected Exhausted (-1 answer)
        for (int i = 0; i < n; i++) {
            conjunction.tell(actor ->
                    actor.executeReceiveRequest(
                            new Request(new Request.Path(conjunction), list(), list(), null),
                            registry
                    )
            );
        }

        for (int i = 0; i < n - 1; i++) {
            Response answer = responses.take();
            assertTrue(answer.isAnswer());
        }
        assertTrue(responses.take().isExhausted());
        System.out.println("Time : " + (System.currentTimeMillis() - startTime));
        assertTrue(responses.isEmpty());
    }

    private void assertResponsesSync(Actor<Root> conjunction, long answerCount, LinkedBlockingQueue<Response> responses, Registry registry) throws InterruptedException {
        long startTime = System.currentTimeMillis();
        long n = answerCount + 1; //total number answers, plus one expected DONE (-1 answer)
        for (int i = 0; i < n; i++) {
            conjunction.tell(actor ->
                    actor.executeReceiveRequest(new Request(new Request.Path(conjunction), list(), list(), null), registry));
            Response answer = responses.take();
            if (i < n - 1) {
                assertTrue(answer.isAnswer());
            } else {
                assertTrue(answer.isExhausted());
            }
        }
        System.out.println("Time : " + (System.currentTimeMillis() - startTime));
    }
}
