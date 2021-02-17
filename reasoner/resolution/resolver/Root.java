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
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concurrent.actor.Actor;
import grakn.core.logic.LogicManager;
import grakn.core.reasoner.resolution.Planner;
import grakn.core.reasoner.resolution.ResolutionRecorder;
import grakn.core.reasoner.resolution.ResolverRegistry;
import grakn.core.reasoner.resolution.answer.AnswerState;
import grakn.core.reasoner.resolution.answer.AnswerState.Partial;
import grakn.core.reasoner.resolution.answer.AnswerState.Partial.Filtered;
import grakn.core.reasoner.resolution.answer.AnswerState.Top;
import grakn.core.reasoner.resolution.answer.Mapping;
import grakn.core.reasoner.resolution.framework.Request;
import grakn.core.reasoner.resolution.framework.Resolver;
import grakn.core.reasoner.resolution.framework.Response;
import grakn.core.reasoner.resolution.framework.ResponseProducer;
import grakn.core.traversal.TraversalEngine;
import grakn.core.traversal.common.Identifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

import static grakn.core.common.iterator.Iterators.iterate;

public interface Root {

    void submitAnswer(Top answer);

    void submitFail(int iteration);

    class Conjunction extends ConjunctionResolver<Conjunction> implements Root {

        private static final Logger LOG = LoggerFactory.getLogger(Conjunction.class);

        private final Consumer<Top> onAnswer;
        private final Consumer<Integer> onFail;
        private final Long offset;
        private final Long limit;
        private long skipped;
        private long answered;

        public Conjunction(Actor<Conjunction> self, grakn.core.pattern.Conjunction conjunction,
                           @Nullable Long offset, @Nullable Long limit, Consumer<Top> onAnswer,
                           Consumer<Integer> onFail, Actor<ResolutionRecorder> resolutionRecorder, ResolverRegistry registry,
                           TraversalEngine traversalEngine, ConceptManager conceptMgr, LogicManager logicMgr, Planner planner, boolean explanations) {
            super(self, Conjunction.class.getSimpleName() + "(pattern:" + conjunction + ")", conjunction, resolutionRecorder,
                  registry, traversalEngine, conceptMgr, logicMgr, planner, explanations);
            this.offset = offset;
            this.limit = limit;
            this.onAnswer = onAnswer;
            this.onFail = onFail;
            this.skipped = 0;
            this.answered = 0;
        }

        @Override
        public void submitAnswer(Top answer) {
            LOG.debug("Submitting answer: {}", answer);
            if (explanations()) {
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

        /*
        NOTE special behaviour: don't clear the deduplication set, in the root
         */
        @Override
        protected ResponseProducer responseProducerReiterate(Request fromUpstream, ResponseProducer responseProducerPrevious,
                                                             int newIteration) {
            assert newIteration > responseProducerPrevious.iteration();
            LOG.debug("{}: Updating ResponseProducer for iteration '{}'", name(), newIteration);

            Plans.Plan plan = plans.getOrCreate(fromUpstream.partialAnswer().conceptMap().concepts().keySet(), resolvables, negateds);

            assert !plan.isEmpty();
            ResponseProducer responseProducerNewIter = responseProducerPrevious.newIterationRetainDedup(Iterators.empty(), newIteration);
            Partial.Mapped downstream = fromUpstream.partialAnswer()
                    .mapToDownstream(Mapping.of(downstreamResolvers.get(plan.get(0)).mapping()));
            Request toDownstream = Request.create(self(),downstreamResolvers.get(plan.get(0)).resolver(), downstream, 0);
            responseProducerNewIter.addDownstreamProducer(toDownstream);
            return responseProducerNewIter;
        }

        protected void failToUpstream(Request fromUpstream, int iteration) {
            submitFail(iteration);
        }

        @Override
        protected void nextAnswer(Request fromUpstream, ResponseProducer responseProducer, int iteration) {
            if (responseProducer.hasUpstreamAnswer()) {
                Top upstreamAnswer = responseProducer.upstreamAnswers().next().asTop();
                responseProducer.recordProduced(upstreamAnswer.conceptMap());
                submitAnswer(upstreamAnswer);
            } else {
                if (responseProducer.hasDownstreamProducer()) {
                    requestFromDownstream(responseProducer.nextDownstreamProducer(), fromUpstream, iteration);
                } else {
                    submitFail(iteration);
                }
            }
        }

        @Override
        protected Optional<AnswerState> toUpstreamAnswer(Partial<?> fromDownstream) {
            return Optional.of(fromDownstream.asIdentity().toTop());
        }

        @Override
        public boolean mustOffset() {
            return offset != null && skipped < offset;
        }

        @Override
        public void offsetOccurred() {
            this.skipped++;
        }

        @Override
        protected void exception(Throwable e) {
            LOG.error("Actor exception", e);
            // TODO, once integrated into the larger flow of executing queries, kill the resolvers and report and exception to root
        }

    }

    class Disjunction extends Resolver<Disjunction> implements Root {

        private static final Logger LOG = LoggerFactory.getLogger(Disjunction.class);

        private final Actor<ResolutionRecorder> resolutionRecorder;
        private final Consumer<Top> onAnswer;
        private final Consumer<Integer> onFail;
        private final List<Actor<ConjunctionResolver.Nested>> downstreamResolvers;
        private final grakn.core.pattern.Disjunction disjunction;
        private final Long offset;
        private final Long limit;
        private long skipped;
        private long answered;
        private boolean isInitialised;
        private ResponseProducer responseProducer;

        public Disjunction(Actor<Disjunction> self, grakn.core.pattern.Disjunction disjunction,
                           @Nullable Long offset, @Nullable Long limit, Consumer<Top> onAnswer,
                           Consumer<Integer> onFail, Actor<ResolutionRecorder> resolutionRecorder, ResolverRegistry registry,
                           TraversalEngine traversalEngine, ConceptManager conceptMgr, boolean explanations) {
            super(self, Disjunction.class.getSimpleName() + "(pattern:" + disjunction + ")", registry, traversalEngine, conceptMgr, explanations);
            this.disjunction = disjunction;
            this.offset = offset;
            this.limit = limit;
            this.onAnswer = onAnswer;
            this.onFail = onFail;
            this.resolutionRecorder = resolutionRecorder;
            this.isInitialised = false;
            this.downstreamResolvers = new ArrayList<>();
        }

        @Override
        public void submitAnswer(Top answer) {
            LOG.debug("Submitting answer: {}", answer);
            if (explanations()) {
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

        @Override
        public void receiveRequest(Request fromUpstream, int iteration) {
            LOG.trace("{}: received Request: {}", name(), fromUpstream);
            if (!isInitialised) {
                initialiseDownstreamResolvers();
                responseProducer = responseProducerCreate(fromUpstream, iteration);
            }
            mayReiterateResponseProducer(fromUpstream, iteration);
            if (iteration < responseProducer.iteration()) {
                // short circuit if the request came from a prior iteration
                onFail.accept(iteration);
            } else {
                assert iteration == responseProducer.iteration();
                nextAnswer(fromUpstream, iteration);
            }
        }

        private void nextAnswer(Request fromUpstream, int iteration) {
            if (responseProducer.hasDownstreamProducer()) {
                requestFromDownstream(responseProducer.nextDownstreamProducer(), fromUpstream, iteration);
            } else {
                submitFail(iteration);
            }
        }

        private void mayReiterateResponseProducer(Request fromUpstream, int iteration) {
            if (responseProducer.iteration() + 1 == iteration) {
                responseProducer = responseProducerReiterate(fromUpstream, responseProducer, iteration);
            }
        }

        @Override
        protected void receiveAnswer(Response.Answer fromDownstream, int iteration) {
            LOG.trace("{}: received answer: {}", name(), fromDownstream);

            Request toDownstream = fromDownstream.sourceRequest();
            Request fromUpstream = fromUpstream(toDownstream);

            Top answer = fromDownstream.answer().asIdentity().toTop();
            ConceptMap filteredMap = answer.conceptMap();
            if (!responseProducer.hasProduced(filteredMap)) {
                responseProducer.recordProduced(filteredMap);
                if (offset != null && skipped < offset) {
                    skipped++;
                    nextAnswer(fromUpstream, iteration);
                    return;
                }
                if (limit != null && answered >= limit) submitFail(iteration);
                else submitAnswer(answer);
            } else {
                nextAnswer(fromUpstream, iteration);
            }
        }

        @Override
        protected void receiveFail(Response.Fail fromDownstream, int iteration) {
            LOG.trace("{}: received Exhausted, with iter {}: {}", name(), iteration, fromDownstream);
            Request toDownstream = fromDownstream.sourceRequest();
            Request fromUpstream = fromUpstream(toDownstream);

            if (iteration < responseProducer.iteration()) {
                // short circuit old iteration failed messages back out of the actor model
                submitFail(iteration);
                return;
            }
            responseProducer.removeDownstreamProducer(fromDownstream.sourceRequest());
            nextAnswer(fromUpstream, iteration);
        }

        @Override
        protected void initialiseDownstreamResolvers() {
            LOG.debug("{}: initialising downstream resolvers", name());
            for (grakn.core.pattern.Conjunction conjunction : disjunction.conjunctions()) {
                downstreamResolvers.add(registry.conjunction(conjunction));
            }
            isInitialised = true;
        }

        @Override
        protected ResponseProducer responseProducerCreate(Request fromUpstream, int iteration) {
            LOG.debug("{}: Creating a new ResponseProducer for request: {}", name(), fromUpstream);
            assert fromUpstream.partialAnswer().isIdentity();
            ResponseProducer responseProducer = new ResponseProducer(Iterators.empty(), iteration);
            assert !downstreamResolvers.isEmpty();
            for (Actor<ConjunctionResolver.Nested> conjunctionResolver : downstreamResolvers) {
                Filtered downstream = fromUpstream.partialAnswer().asIdentity()
                        .filterToDownstream(conjunctionRetrievedIds(conjunctionResolver));
                Request request = Request.create(self(), conjunctionResolver, downstream);
                responseProducer.addDownstreamProducer(request);
            }
            return responseProducer;
        }

        private Set<Identifier.Variable.Retrievable> conjunctionRetrievedIds(Actor<ConjunctionResolver.Nested> conjunctionResolver) {
            // TODO use a map from resolvable to resolvers, then we don't have to reach into the state and use the conjunction
            return iterate(conjunctionResolver.state.conjunction.variables()).filter(v -> v.id().isRetrievable())
                    .map(v -> v.id().asRetrievable()).toSet();
        }

        @Override
        protected ResponseProducer responseProducerReiterate(Request fromUpstream, ResponseProducer responseProducerPrevious,
                                                             int newIteration) {
            assert newIteration > responseProducerPrevious.iteration();
            LOG.debug("{}: Updating ResponseProducer for iteration '{}'", name(), newIteration);

            assert newIteration > responseProducerPrevious.iteration();
            ResponseProducer responseProducerNewIter = responseProducerPrevious.newIterationRetainDedup(Iterators.empty(), newIteration);
            for (Actor<ConjunctionResolver.Nested> conjunctionResolver : downstreamResolvers) {
                Filtered downstream = fromUpstream.partialAnswer().asIdentity()
                        .filterToDownstream(conjunctionRetrievedIds(conjunctionResolver));
                Request request = Request.create(self(), conjunctionResolver, downstream);
                responseProducer.addDownstreamProducer(request);
            }
            return responseProducerNewIter;
        }

        @Override
        protected void exception(Throwable e) {
            LOG.error("Actor exception", e);
            // TODO, once integrated into the larger flow of executing queries, kill the resolvers and report and exception to root
        }

    }
}
