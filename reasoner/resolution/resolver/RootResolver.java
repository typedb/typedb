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

package grakn.core.reasoner.resolution.resolver;

import grakn.core.common.iterator.Iterators;
import grakn.core.concept.ConceptManager;
import grakn.core.logic.LogicManager;
import grakn.core.logic.resolvable.Concludable;
import grakn.core.reasoner.resolution.Planner;
import grakn.core.reasoner.resolution.ResolutionRecorder;
import grakn.core.reasoner.resolution.ResolverRegistry;
import grakn.core.reasoner.resolution.answer.AnswerState;
import grakn.core.reasoner.resolution.answer.AnswerState.Partial;
import grakn.core.reasoner.resolution.answer.AnswerState.Top;
import grakn.core.reasoner.resolution.framework.Request;
import grakn.core.traversal.TraversalEngine;
import grakn.core.traversal.common.Identifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.function.Consumer;

public interface RootResolver {

    void submitAnswer(Top answer);

    void submitFail(int iteration);

    class Conjunction extends ConjunctionResolver<Conjunction> implements RootResolver {

        private static final Logger LOG = LoggerFactory.getLogger(Conjunction.class);

        private final grakn.core.pattern.Conjunction conjunction;
        private final Set<Identifier.Variable.Retrievable> missingBounds;
        private final Consumer<Top> onAnswer;
        private final Consumer<Integer> onFail;
        private final Consumer<Throwable> onException;

        public Conjunction(Driver<Conjunction> driver, grakn.core.pattern.Conjunction conjunction,
                           Consumer<Top> onAnswer, Consumer<Integer> onFail, Consumer<Throwable> onException,
                           Driver<ResolutionRecorder> resolutionRecorder, ResolverRegistry registry,
                           TraversalEngine traversalEngine, ConceptManager conceptMgr, LogicManager logicMgr,
                           Planner planner, boolean resolutionTracing) {
            super(driver, Conjunction.class.getSimpleName() + "(pattern:" + conjunction + ")",
                  resolutionRecorder, registry, traversalEngine, conceptMgr, logicMgr, planner, resolutionTracing);
            this.conjunction = conjunction;
            this.missingBounds = missingBounds(this.conjunction);
            this.onAnswer = onAnswer;
            this.onFail = onFail;
            this.onException = onException;
        }

        @Override
        public grakn.core.pattern.Conjunction conjunction() {
            return conjunction;
        }

        @Override
        Set<Identifier.Variable.Retrievable> missingBounds() {
            return missingBounds;
        }

        @Override
        Set<Concludable> concludablesTriggeringRules() {
            return Iterators.iterate(Concludable.create(conjunction))
                    .filter(c -> c.getApplicableRules(conceptMgr, logicMgr).hasNext())
                    .toSet();
        }

        @Override
        public void terminate(Throwable cause) {
            super.terminate(cause);
            onException.accept(cause);
        }

        @Override
        public void submitAnswer(Top answer) {
            LOG.debug("Submitting answer: {}", answer);
            if (answer.recordExplanations()) {
                LOG.trace("Recording root answer: {}", answer);
                resolutionRecorder.execute(state -> state.record(answer));
            }
            onAnswer.accept(answer);
        }

        @Override
        public void submitFail(int iteration) {
            onFail.accept(iteration);
        }

        @Override
        protected void answerToUpstream(AnswerState answer, Request fromUpstream, int iteration) {
            assert answer.isTop();
            submitAnswer(answer.asTop());
        }

        @Override
        protected void failToUpstream(Request fromUpstream, int iteration) {
            submitFail(iteration);
        }

        @Override
        protected void nextAnswer(Request fromUpstream, RequestState requestState, int iteration) {
            if (requestState.hasDownstreamProducer()) {
                requestFromDownstream(requestState.nextDownstreamProducer(), fromUpstream, iteration);
            } else {
                submitFail(iteration);
            }
        }

        @Override
        protected Optional<AnswerState> toUpstreamAnswer(Partial<?> fromDownstream) {
            assert fromDownstream.isIdentity();
            return Optional.of(fromDownstream.asIdentity().toTop());
        }

        @Override
        RequestState requestStateNew(int iteration, boolean singleAnswerRequired) {
            return new RequestState(iteration, singleAnswerRequired);
        }

        @Override
        RequestState requestStateForIteration(RequestState requestStatePrior, int iteration, boolean singleAnswerRequired) {
            return new RequestState(iteration, singleAnswerRequired, requestStatePrior.produced());
        }


    }

    class Disjunction extends DisjunctionResolver<Disjunction> implements RootResolver {

        private static final Logger LOG = LoggerFactory.getLogger(Disjunction.class);
        private final Consumer<Top> onAnswer;
        private final Consumer<Integer> onFail;
        private final Consumer<Throwable> onException;

        public Disjunction(Driver<Disjunction> driver, grakn.core.pattern.Disjunction disjunction,
                           Consumer<Top> onAnswer, Consumer<Integer> onFail, Consumer<Throwable> onException,
                           Driver<ResolutionRecorder> resolutionRecorder, ResolverRegistry registry,
                           TraversalEngine traversalEngine, ConceptManager conceptMgr, boolean resolutionTracing) {
            super(driver, Disjunction.class.getSimpleName() + "(pattern:" + disjunction + ")", disjunction,
                  resolutionRecorder, registry, traversalEngine, conceptMgr, resolutionTracing);
            this.onAnswer = onAnswer;
            this.onFail = onFail;
            this.onException = onException;
            this.isInitialised = false;
        }

        @Override
        public void terminate(Throwable cause) {
            super.terminate(cause);
            onException.accept(cause);
        }

        @Override
        protected void nextAnswer(Request fromUpstream, RequestState requestState, int iteration) {
            if (requestState.hasDownstreamProducer()) {
                requestFromDownstream(requestState.nextDownstreamProducer(), fromUpstream, iteration);
            } else {
                submitFail(iteration);
            }
        }

        @Override
        protected void answerToUpstream(AnswerState answer, Request fromUpstream, int iteration) {
            assert answer.isTop();
            submitAnswer(answer.asTop());
        }

        @Override
        public void submitAnswer(Top answer) {
            LOG.debug("Submitting answer: {}", answer);
            if (answer.recordExplanations()) {
                LOG.trace("Recording root answer: {}", answer);
                resolutionRecorder.execute(state -> state.record(answer));
            }
            answered++;
            onAnswer.accept(answer);
        }

        @Override
        public void submitFail(int iteration) {
            onFail.accept(iteration);
        }

        @Override
        protected boolean tryAcceptUpstreamAnswer(AnswerState upstreamAnswer, Request fromUpstream, int iteration) {
            RequestState requestState = requestStates.get(fromUpstream);
            if (!requestState.hasProduced(upstreamAnswer.conceptMap())) {
                requestState.recordProduced(upstreamAnswer.conceptMap());
                answerToUpstream(upstreamAnswer, fromUpstream, iteration);
                return true;
            } else {
                return false;
            }
        }

        @Override
        protected AnswerState toUpstreamAnswer(Partial<?> answer) {
            assert answer.isIdentity();
            return answer.asIdentity().toTop();
        }

        @Override
        protected RequestState requestStateForIteration(RequestState requestStatePrior, int newIteration) {
            return new RequestState(newIteration, requestStatePrior.produced());
        }
    }
}
