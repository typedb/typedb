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

import grakn.core.concept.ConceptManager;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concurrent.actor.Actor;
import grakn.core.logic.LogicManager;
import grakn.core.reasoner.resolution.Planner;
import grakn.core.reasoner.resolution.ResolutionRecorder;
import grakn.core.reasoner.resolution.ResolverRegistry;
import grakn.core.reasoner.resolution.answer.AnswerState;
import grakn.core.reasoner.resolution.answer.AnswerState.Partial;
import grakn.core.reasoner.resolution.answer.AnswerState.Top;
import grakn.core.reasoner.resolution.framework.Request;
import grakn.core.traversal.TraversalEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

public interface Root {

    void submitAnswer(Top answer);

    void submitFail(int iteration);

    class Conjunction extends ConjunctionResolver<Conjunction, Conjunction.Responses> implements Root {

        private static final Logger LOG = LoggerFactory.getLogger(Conjunction.class);

        private final Consumer<Top> onAnswer;
        private final Consumer<Integer> onFail;
        private Consumer<Throwable> onException;
        private final Long offset;
        private final Long limit;
        private long skipped;
        private long answered;

        public Conjunction(Actor<Conjunction> self, grakn.core.pattern.Conjunction conjunction,
                           @Nullable Long offset, @Nullable Long limit, Consumer<Top> onAnswer,
                           Consumer<Integer> onFail, Consumer<Throwable> onException, Actor<ResolutionRecorder> resolutionRecorder, ResolverRegistry registry,
                           TraversalEngine traversalEngine, ConceptManager conceptMgr, LogicManager logicMgr, Planner planner, boolean resolutionTracing) {
            super(self, Conjunction.class.getSimpleName() + "(pattern:" + conjunction + ")", conjunction, resolutionRecorder,
                  registry, traversalEngine, conceptMgr, logicMgr, planner, resolutionTracing);
            this.offset = offset;
            this.limit = limit;
            this.onAnswer = onAnswer;
            this.onFail = onFail;
            this.onException = onException;
            this.skipped = 0;
            this.answered = 0;
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
                resolutionRecorder.tell(state -> state.record(answer));
            }
            onAnswer.accept(answer);
        }

        @Override
        public void submitFail(int iteration) {
            onFail.accept(iteration);
        }

        @Override
        protected void answerToUpstream(AnswerState answer, Request fromUpstream, int iteration) {
            if ((limit == null) || (answered < limit)) {
                submitAnswer(answer.asTop());
                this.answered++;
            } else {
                submitFail(iteration);
            }
        }

        @Override
        protected void failToUpstream(Request fromUpstream, int iteration) {
            submitFail(iteration);
        }

        @Override
        protected void nextAnswer(Request fromUpstream, Responses responses, int iteration) {
            if (responses.hasDownstreamProducer()) {
                requestFromDownstream(responses.nextDownstreamProducer(), fromUpstream, iteration);
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
        Responses responsesNew(int iteration) {
            return new Responses(iteration);
        }

        @Override
        Responses responsesForIteration(Responses responsesPrior, int iteration) {
            return new Responses(iteration, responsesPrior.produced());
        }

        @Override
        boolean tryAcceptUpstreamAnswer(AnswerState upstreamAnswer, Request fromUpstream, int iteration) {
            Responses responses = this.responses.get(fromUpstream);
            if (!responses.hasProduced(upstreamAnswer.conceptMap())) {
                responses.recordProduced(upstreamAnswer.conceptMap());
                if (mustOffset()) {
                    this.skipped++;
                    return false;
                } else {
                    answerToUpstream(upstreamAnswer, fromUpstream, iteration);
                    return true;
                }
            } else {
                return false;
            }
        }

        private boolean mustOffset() {
            return offset != null && skipped < offset;
        }

        static class Responses extends CompoundResolver.Responses {

            private final Set<ConceptMap> produced;

            public Responses(int iteration) {
                this(iteration, new HashSet<>());
            }

            public Responses(int iteration, Set<ConceptMap> produced) {
                super(iteration);
                this.produced = produced;
            }

            public void recordProduced(ConceptMap conceptMap) {
                produced.add(conceptMap);
            }

            public boolean hasProduced(ConceptMap conceptMap) {
                return produced.contains(conceptMap);
            }

            public Set<ConceptMap> produced() {
                return produced;
            }
        }
    }

    class Disjunction extends DisjunctionResolver<Disjunction> implements Root {

        private static final Logger LOG = LoggerFactory.getLogger(Disjunction.class);
        private final Long offset;
        private final Long limit;
        private final Consumer<Top> onAnswer;
        private final Consumer<Integer> onFail;
        private final Consumer<Throwable> onException;

        public Disjunction(Actor<Disjunction> self, grakn.core.pattern.Disjunction disjunction,
                           @Nullable Long offset, @Nullable Long limit, Consumer<Top> onAnswer,
                           Consumer<Integer> onFail, Consumer<Throwable> onException, Actor<ResolutionRecorder> resolutionRecorder, ResolverRegistry registry,
                           TraversalEngine traversalEngine, ConceptManager conceptMgr, boolean resolutionTracing) {
            super(self, Disjunction.class.getSimpleName() + "(pattern:" + disjunction + ")", disjunction,
                  resolutionRecorder, registry, traversalEngine, conceptMgr, resolutionTracing);
            this.offset = offset;
            this.limit = limit;
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
        protected void nextAnswer(Request fromUpstream, Responses responseProducer, int iteration) {
            if (responseProducer.hasDownstreamProducer()) {
                requestFromDownstream(responseProducer.nextDownstreamProducer(), fromUpstream, iteration);
            } else {
                submitFail(iteration);
            }
        }

        @Override
        protected void answerToUpstream(AnswerState answer, Request fromUpstream, int iteration) {
            assert answer.isTop();
            if ((limit == null) || (answered < limit)) {
                submitAnswer(answer.asTop());
                this.answered++;
            } else {
                submitFail(iteration);
            }
        }

        @Override
        public void submitAnswer(Top answer) {
            LOG.debug("Submitting answer: {}", answer);
            if (answer.recordExplanations()) {
                LOG.trace("Recording root answer: {}", answer);
                resolutionRecorder.tell(state -> state.record(answer));
            }
            answered++;
            onAnswer.accept(answer);
        }

        @Override
        public void submitFail(int iteration) {
            onFail.accept(iteration);
        }

        public boolean mustOffset() {
            return offset != null && skipped < offset;
        }

        @Override
        protected boolean tryAcceptUpstreamAnswer(AnswerState upstreamAnswer, Request fromUpstream, int iteration) {
            Responses responses = this.responses.get(fromUpstream);
            if (!responses.hasProduced(upstreamAnswer.conceptMap())) {
                responses.recordProduced(upstreamAnswer.conceptMap());
                if (mustOffset()) {
                    this.skipped++;
                    return false;
                } else {
                    answerToUpstream(upstreamAnswer, fromUpstream, iteration);
                    return true;
                }
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
        protected Responses responsesForIteration(Responses responsesPrior, int newIteration) {
            return new Responses(newIteration, responsesPrior.produced());
        }
    }
}
