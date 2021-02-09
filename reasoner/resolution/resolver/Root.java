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

import grakn.core.common.exception.GraknException;
import grakn.core.common.iterator.Iterators;
import grakn.core.common.iterator.ResourceIterator;
import grakn.core.concept.ConceptManager;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concurrent.actor.Actor;
import grakn.core.logic.LogicManager;
import grakn.core.reasoner.resolution.Planner;
import grakn.core.reasoner.resolution.ResolutionRecorder;
import grakn.core.reasoner.resolution.ResolverRegistry;
import grakn.core.reasoner.resolution.answer.AnswerState;
import grakn.core.reasoner.resolution.framework.Request;
import grakn.core.reasoner.resolution.framework.ResolutionAnswer;
import grakn.core.reasoner.resolution.framework.Resolver;
import grakn.core.reasoner.resolution.framework.Response;
import grakn.core.reasoner.resolution.framework.ResponseProducer;
import grakn.core.traversal.TraversalEngine;
import graql.lang.pattern.variable.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;

public interface Root {

    void submitAnswer(ResolutionAnswer answer);

    void submitFail(int iteration);

    class Conjunction extends ConjunctionResolver<Conjunction> implements Root {

        private static final Logger LOG = LoggerFactory.getLogger(Conjunction.class);

        private final Consumer<ResolutionAnswer> onAnswer;
        private final Consumer<Integer> onFail;
        private final Set<Reference.Name> filter;
        private final Long offset;
        private final Long limit;
        private long skipped;
        private long answered;

        public Conjunction(Actor<Conjunction> self, grakn.core.pattern.Conjunction conjunction, Set<Reference.Name> filter,
                           @Nullable Long offset, @Nullable Long limit, Consumer<ResolutionAnswer> onAnswer,
                           Consumer<Integer> onFail, Actor<ResolutionRecorder> resolutionRecorder, ResolverRegistry registry,
                           TraversalEngine traversalEngine, ConceptManager conceptMgr, LogicManager logicMgr, Planner planner, boolean explanations) {
            super(self, Conjunction.class.getSimpleName() + "(pattern:" + conjunction + ")", conjunction, resolutionRecorder,
                  registry, traversalEngine, conceptMgr, logicMgr, planner, explanations);
            this.filter = filter;
            this.offset = offset;
            this.limit = limit;
            this.onAnswer = onAnswer;
            this.onFail = onFail;
            this.skipped = 0;
            this.answered = 0;
        }

        @Override
        public void submitAnswer(ResolutionAnswer answer) {
            LOG.debug("Submitting answer: {}", answer.derived());
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
        protected void respondToUpstream(Response response, int iteration) {
            if ((response.isAnswer() && limit == null) || (limit != null && answered < limit)) {
                submitAnswer(response.asAnswer().answer());
                this.answered++;
            } else if (response.isFail() || (limit != null && answered >= limit)) {
                submitFail(iteration);
            } else {
                throw GraknException.of(ILLEGAL_STATE);
            }
        }

        @Override
        protected void nextAnswer(Request fromUpstream, ResponseProducer responseProducer, int iteration) {
            if (responseProducer.hasUpstreamAnswer()) {
                AnswerState.UpstreamVars.Derived upstreamAnswer = responseProducer.upstreamAnswers().next();
                responseProducer.recordProduced(upstreamAnswer.withInitialFiltered());
                ResolutionAnswer answer = new ResolutionAnswer(upstreamAnswer, conjunction.toString(),
                                                               ResolutionAnswer.Derivation.EMPTY, self(), false);
                submitAnswer(answer);
            } else {
                if (responseProducer.hasDownstreamProducer()) {
                    requestFromDownstream(responseProducer.nextDownstreamProducer(), fromUpstream, iteration);
                } else {
                    submitFail(iteration);
                }
            }
        }

        @Override
        protected ResourceIterator<AnswerState.UpstreamVars.Derived> toUpstreamAnswers(Request fromUpstream, ResourceIterator<ConceptMap> downstreamConceptMaps) {
            return downstreamConceptMaps.map(conceptMap -> fromUpstream.partialAnswer().asIdentity()
                    .aggregateToUpstream(conceptMap, this.filter));
        }

        @Override
        protected Optional<AnswerState.UpstreamVars.Derived> toUpstreamAnswer(Request fromUpstream, ConceptMap downstreamConceptMap) {
            return Optional.of(fromUpstream.partialAnswer().asIdentity().aggregateToUpstream(downstreamConceptMap, this.filter));
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
            // TODO, once integrated into the larger flow of executing queries, kill the actors and report and exception to root
        }

    }

    class Disjunction extends Resolver<Disjunction> implements Root {

        private static final Logger LOG = LoggerFactory.getLogger(Disjunction.class);

        private final Actor<ResolutionRecorder> resolutionRecorder;
        private final Consumer<ResolutionAnswer> onAnswer;
        private final Consumer<Integer> onFail;
        private final List<Actor<ConjunctionResolver.Simple>> downstreamResolvers;
        private final grakn.core.pattern.Disjunction disjunction;
        private final Set<Reference.Name> filter;
        private final Long offset;
        private final Long limit;
        private long skipped;
        private long answered;
        private boolean isInitialised;
        private ResponseProducer responseProducer;

        public Disjunction(Actor<Disjunction> self, grakn.core.pattern.Disjunction disjunction, Set<Reference.Name> filter,
                           @Nullable Long offset, @Nullable Long limit, Consumer<ResolutionAnswer> onAnswer,
                           Consumer<Integer> onFail, Actor<ResolutionRecorder> resolutionRecorder, ResolverRegistry registry,
                           TraversalEngine traversalEngine, boolean explanations) {
            super(self, Disjunction.class.getSimpleName() + "(pattern:" + disjunction + ")", registry, traversalEngine, explanations);
            this.disjunction = disjunction;
            this.filter = filter;
            this.offset = offset;
            this.limit = limit;
            this.onAnswer = onAnswer;
            this.onFail = onFail;
            this.resolutionRecorder = resolutionRecorder;
            this.isInitialised = false;
            this.downstreamResolvers = new ArrayList<>();
        }

        @Override
        public void submitAnswer(ResolutionAnswer answer) {
            LOG.debug("Submitting answer: {}", answer.derived());
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
                initialiseDownstreamActors();
                isInitialised = true;
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

            ResolutionAnswer.Derivation derivation;
            if (explanations()) {
                // TODO
                derivation = null;
            } else {
                derivation = null;
            }

            ConceptMap conceptMap = fromDownstream.answer().derived().withInitialFiltered();
            AnswerState.UpstreamVars.Derived answer = fromUpstream.partialAnswer().asIdentity()
                    .aggregateToUpstream(conceptMap, this.filter);
            ConceptMap filteredMap = answer.withInitialFiltered();
            if (!responseProducer.hasProduced(filteredMap)) {
                responseProducer.recordProduced(filteredMap);
                if (offset != null && skipped < offset) {
                    skipped++;
                    nextAnswer(fromUpstream, iteration);
                    return;
                }
                ResolutionAnswer resolutionAnswer = new ResolutionAnswer(answer, disjunction.toString(), derivation, self(),
                                                                         fromDownstream.answer().isInferred());

                if (limit != null && answered >= limit) submitFail(iteration);
                else submitAnswer(resolutionAnswer);
            } else {
                nextAnswer(fromUpstream, iteration);
            }
        }

        @Override
        protected void receiveExhausted(Response.Fail fromDownstream, int iteration) {
            LOG.trace("{}: received Exhausted, with iter {}: {}", name(), iteration, fromDownstream);
            Request toDownstream = fromDownstream.sourceRequest();
            Request fromUpstream = fromUpstream(toDownstream);

            if (iteration < responseProducer.iteration()) {
                // short circuit old iteration exhausted messages back out of the actor model
                submitFail(iteration);
                return;
            }
            responseProducer.removeDownstreamProducer(fromDownstream.sourceRequest());
            nextAnswer(fromUpstream, iteration);
        }

        @Override
        protected void initialiseDownstreamActors() {
            LOG.debug("{}: initialising downstream actors", name());
            for (grakn.core.pattern.Conjunction conjunction : disjunction.conjunctions()) {
                downstreamResolvers.add(registry.conjunction(conjunction));
            }
        }

        @Override
        protected ResponseProducer responseProducerCreate(Request fromUpstream, int iteration) {
            LOG.debug("{}: Creating a new ResponseProducer for request: {}", name(), fromUpstream);
            assert fromUpstream.partialAnswer().isIdentity();
            ResponseProducer responseProducer = new ResponseProducer(Iterators.empty(), iteration);
            assert !downstreamResolvers.isEmpty();
            for (Actor<ConjunctionResolver.Simple> conjunctionResolver : downstreamResolvers) {
                AnswerState.DownstreamVars downstream = AnswerState.UpstreamVars.Initial.of(fromUpstream.partialAnswer().conceptMap())
                        .toDownstreamVars();
                Request request = Request.create(fromUpstream.path().append(conjunctionResolver, downstream),
                                                 downstream, ResolutionAnswer.Derivation.EMPTY, -1);
                responseProducer.addDownstreamProducer(request);
            }
            return responseProducer;
        }

        @Override
        protected ResponseProducer responseProducerReiterate(Request fromUpstream, ResponseProducer responseProducerPrevious, int newIteration) {
            assert newIteration > responseProducerPrevious.iteration();
            LOG.debug("{}: Updating ResponseProducer for iteration '{}'", name(), newIteration);

            assert newIteration > responseProducerPrevious.iteration();
            ResponseProducer responseProducerNewIter = responseProducerPrevious.newIteration(Iterators.empty(), newIteration);
            for (Actor<ConjunctionResolver.Simple> conjunctionResolver : downstreamResolvers) {
                AnswerState.DownstreamVars downstream = AnswerState.UpstreamVars.Initial.of(fromUpstream.partialAnswer().conceptMap())
                        .toDownstreamVars();
                Request request = Request.create(fromUpstream.path().append(conjunctionResolver, downstream),
                                                 downstream, ResolutionAnswer.Derivation.EMPTY, -1);
                responseProducer.addDownstreamProducer(request);
            }
            return responseProducerNewIter;
        }

        @Override
        protected void exception(Throwable e) {
            LOG.error("Actor exception", e);
            // TODO, once integrated into the larger flow of executing queries, kill the actors and report and exception to root
        }

    }
}
