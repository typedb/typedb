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
 *
 */

package grakn.core.reasoner.resolution.resolver;

import grakn.core.common.iterator.Iterators;
import grakn.core.common.iterator.ResourceIterator;
import grakn.core.concept.ConceptManager;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concurrent.actor.Actor;
import grakn.core.logic.LogicManager;
import grakn.core.logic.resolvable.Concludable;
import grakn.core.logic.resolvable.Negated;
import grakn.core.logic.resolvable.Resolvable;
import grakn.core.logic.resolvable.Retrievable;
import grakn.core.pattern.Conjunction;
import grakn.core.pattern.Negation;
import grakn.core.reasoner.resolution.Planner;
import grakn.core.reasoner.resolution.ResolutionRecorder;
import grakn.core.reasoner.resolution.ResolverRegistry;
import grakn.core.reasoner.resolution.answer.AnswerState;
import grakn.core.reasoner.resolution.answer.AnswerState.UpstreamVars.Initial;
import grakn.core.reasoner.resolution.answer.Mapping;
import grakn.core.reasoner.resolution.framework.Request;
import grakn.core.reasoner.resolution.framework.ResolutionAnswer;
import grakn.core.reasoner.resolution.framework.Resolver;
import grakn.core.reasoner.resolution.framework.Response;
import grakn.core.reasoner.resolution.framework.ResponseProducer;
import grakn.core.traversal.TraversalEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static grakn.common.collection.Collections.map;
import static grakn.core.common.iterator.Iterators.iterate;

public abstract class ConjunctionResolver<T extends ConjunctionResolver<T>> extends Resolver<T> {

    private static final Logger LOG = LoggerFactory.getLogger(Conjunction.class);

    private final LogicManager logicMgr;
    private final Planner planner;
    final ConceptManager conceptMgr;
    final Actor<ResolutionRecorder> resolutionRecorder;
    final grakn.core.pattern.Conjunction conjunction;
    final List<Resolvable<?>> plan;
    final Map<Resolvable<?>, ResolverRegistry.MappedResolver> downstreamResolvers;
    final Map<Request, ResponseProducer> responseProducers;
    private boolean isInitialised;

    public ConjunctionResolver(Actor<T> self, String name, grakn.core.pattern.Conjunction conjunction,
                               Actor<ResolutionRecorder> resolutionRecorder, ResolverRegistry registry,
                               TraversalEngine traversalEngine, ConceptManager conceptMgr, LogicManager logicMgr,
                               Planner planner, boolean explanations) {
        super(self, name, registry, traversalEngine, explanations);
        this.conceptMgr = conceptMgr;
        this.logicMgr = logicMgr;
        this.resolutionRecorder = resolutionRecorder;
        this.planner = planner;
        this.conjunction = conjunction;
        this.plan = new ArrayList<>();
        this.responseProducers = new HashMap<>();
        this.isInitialised = false;
        this.downstreamResolvers = new HashMap<>();
    }

    protected abstract void nextAnswer(Request fromUpstream, ResponseProducer responseProducer, int iteration);

    protected abstract ResourceIterator<AnswerState.UpstreamVars.Derived> toUpstreamAnswers(Request fromUpstream,
                                                                                            ResourceIterator<ConceptMap> downstreamConceptMaps);

    protected abstract Optional<AnswerState.UpstreamVars.Derived> toUpstreamAnswer(Request fromUpstream, ConceptMap downstreamConceptMap);

    protected boolean mustOffset() { return false; }

    protected void offsetOccurred() {}

    @Override
    public void receiveRequest(Request fromUpstream, int iteration) {
        LOG.trace("{}: received Request: {}", name(), fromUpstream);

        if (!isInitialised) {
            initialiseDownstreamActors();
            isInitialised = true;
        }

        ResponseProducer responseProducer = mayUpdateAndGetResponseProducer(fromUpstream, iteration);
        if (iteration < responseProducer.iteration()) {
            // short circuit if the request came from a prior iteration
            respondToUpstream(new Response.Fail(fromUpstream), iteration);
        } else {
            assert iteration == responseProducer.iteration();
            nextAnswer(fromUpstream, responseProducer, iteration);
        }
    }

    @Override
    protected void receiveAnswer(Response.Answer fromDownstream, int iteration) {
        LOG.trace("{}: received Answer: {}", name(), fromDownstream);

        Request toDownstream = fromDownstream.sourceRequest();
        Request fromUpstream = fromUpstream(toDownstream);
        ResponseProducer responseProducer = responseProducers.get(fromUpstream);

        ResolutionAnswer.Derivation derivation;
        if (explanations()) {
            // TODO: revisit
            derivation = fromDownstream.sourceRequest().partialResolutions();
            if (fromDownstream.answer().isInferred()) {
                derivation = derivation.withAnswer(fromDownstream.sourceRequest().receiver(), fromDownstream.answer());
            }
        } else {
            derivation = null;
        }


        // TODO: this is a bit of a hack, we want requests to a negation to be "single use", otherwise we can end up in an infinite loop
        // TODO: where the request to the negation never gets removed and we constantly re-request from it!
        // TODO: this could be either implemented with a different response type: FinalAnswer, or splitting Request into ReusableRequest vs SingleRequest
        if (plan.get(toDownstream.planIndex()).isNegated()) responseProducer.removeDownstreamProducer(toDownstream);

        ConceptMap conceptMap = fromDownstream.answer().derived().withInitialFiltered();
        if (fromDownstream.planIndex() == plan.size() - 1) {
            Optional<AnswerState.UpstreamVars.Derived> answer = toUpstreamAnswer(fromUpstream, conceptMap);
            if (answer.isPresent() && !responseProducer.hasProduced(answer.get().withInitialFiltered())) {
                responseProducer.recordProduced(answer.get().withInitialFiltered());
                if (mustOffset()) {
                    offsetOccurred();
                    nextAnswer(fromUpstream, responseProducer, iteration);
                    return;
                }
                ResolutionAnswer resolutionAnswer = new ResolutionAnswer(answer.get(), conjunction.toString(), derivation, self(),
                                                                         fromDownstream.answer().isInferred());
                respondToUpstream(Response.Answer.create(fromUpstream, resolutionAnswer), iteration);
            } else {
                nextAnswer(fromUpstream, responseProducer, iteration);
            }
        } else {
            int planIndex = fromDownstream.planIndex() + 1;
            ResolverRegistry.MappedResolver nextPlannedDownstream = downstreamResolvers.get(plan.get(planIndex));
            AnswerState.DownstreamVars.Mapped downstream = Initial.of(conceptMap).toDownstreamVars(Mapping.of(nextPlannedDownstream.mapping()));
            Request downstreamRequest = Request.create(fromUpstream.path().append(nextPlannedDownstream.resolver(), downstream),
                                                       downstream, derivation, planIndex);
            responseProducer.addDownstreamProducer(downstreamRequest);
            requestFromDownstream(downstreamRequest, fromUpstream, iteration);
        }
    }

    @Override
    protected void receiveExhausted(Response.Fail fromDownstream, int iteration) {
        LOG.trace("{}: Receiving Exhausted: {}", name(), fromDownstream);
        Request toDownstream = fromDownstream.sourceRequest();
        Request fromUpstream = fromUpstream(toDownstream);
        ResponseProducer responseProducer = responseProducers.get(fromUpstream);

        if (iteration < responseProducer.iteration()) {
            // short circuit old iteration exhausted messages to upstream
            respondToUpstream(new Response.Fail(fromUpstream), iteration);
            return;
        }

        responseProducer.removeDownstreamProducer(fromDownstream.sourceRequest());
        nextAnswer(fromUpstream, responseProducer, iteration);
    }

    @Override
    protected void initialiseDownstreamActors() {
        LOG.debug("{}: initialising downstream actors", name());
        Set<Concludable> concludables = Iterators.iterate(Concludable.create(conjunction))
                .filter(c -> c.getApplicableRules(conceptMgr, logicMgr).hasNext()).toSet();
        Set<Retrievable> retrievables = Retrievable.extractFrom(conjunction, concludables);
        Set<Resolvable<?>> resolvables = new HashSet<>();
        resolvables.addAll(concludables);
        resolvables.addAll(retrievables);

        plan.addAll(planner.plan(resolvables));
        iterate(plan).forEachRemaining(resolvable -> {
            downstreamResolvers.put(resolvable, registry.registerResolvable(resolvable));
        });

        // TODO: just adding negations at the end, but we will want to include them in the planner
        for (Negation negation : conjunction.negations()) {
            Negated negated = new Negated(negation);
            plan.add(negated);
            downstreamResolvers.put(negated, registry.negated(negated, conjunction));
        }
    }

    private ResponseProducer mayUpdateAndGetResponseProducer(Request fromUpstream, int iteration) {
        if (!responseProducers.containsKey(fromUpstream)) {
            responseProducers.put(fromUpstream, responseProducerCreate(fromUpstream, iteration));
        } else {
            ResponseProducer responseProducer = responseProducers.get(fromUpstream);
            assert responseProducer.iteration() == iteration ||
                    responseProducer.iteration() + 1 == iteration;

            if (responseProducer.iteration() + 1 == iteration) {
                // when the same request for the next iteration the first time, re-initialise required state
                ResponseProducer responseProducerNextIter = responseProducerReiterate(fromUpstream, responseProducer, iteration);
                responseProducers.put(fromUpstream, responseProducerNextIter);
            }
        }
        return responseProducers.get(fromUpstream);
    }

    @Override
    protected ResponseProducer responseProducerCreate(Request fromUpstream, int iteration) {
        LOG.debug("{}: Creating a new ResponseProducer for request: {}", name(), fromUpstream);

//        ResourceIterator<AnswerState.UpstreamVars.Derived> upstreamAnswers = toUpstreamAnswers(
//                fromUpstream, compatibleBoundAnswers(conceptMgr, conjunction, fromUpstream.partialAnswer().conceptMap()));
//
        ResponseProducer responseProducer = new ResponseProducer(Iterators.empty(), iteration);
        assert !plan.isEmpty();
        AnswerState.DownstreamVars.Mapped downstream = Initial.of(fromUpstream.partialAnswer().conceptMap())
                .toDownstreamVars(Mapping.of(downstreamResolvers.get(plan.get(0)).mapping()));
        Request toDownstream = Request.create(fromUpstream.path().append(downstreamResolvers.get(plan.get(0)).resolver(), downstream),
                                              downstream, new ResolutionAnswer.Derivation(map()), 0);
        responseProducer.addDownstreamProducer(toDownstream);
        return responseProducer;
    }

    @Override
    protected ResponseProducer responseProducerReiterate(Request fromUpstream, ResponseProducer responseProducerPrevious,
                                                         int newIteration) {
        assert newIteration > responseProducerPrevious.iteration();
        LOG.debug("{}: Updating ResponseProducer for iteration '{}'", name(), newIteration);

//        ResourceIterator<AnswerState.UpstreamVars.Derived> upstreamAnswers = toUpstreamAnswers(
//                fromUpstream, compatibleBoundAnswers(conceptMgr, conjunction, fromUpstream.partialAnswer().conceptMap()));

        ResponseProducer responseProducerNewIter = responseProducerPrevious.newIteration(Iterators.empty(), newIteration);
        assert !plan.isEmpty();
        AnswerState.DownstreamVars downstream = Initial.of(fromUpstream.partialAnswer().conceptMap())
                .toDownstreamVars(Mapping.of(downstreamResolvers.get(plan.get(0)).mapping()));
        Request toDownstream = Request.create(fromUpstream.path().append(downstreamResolvers.get(plan.get(0)).resolver(), fromUpstream.partialAnswer()),
                                              downstream, new ResolutionAnswer.Derivation(map()), 0);
        responseProducerNewIter.addDownstreamProducer(toDownstream);
        return responseProducerNewIter;
    }

    public static class Nested extends ConjunctionResolver<Nested> {

        public Nested(Actor<Nested> self, Conjunction conjunction, Actor<ResolutionRecorder> resolutionRecorder, ResolverRegistry registry, TraversalEngine traversalEngine, ConceptManager conceptMgr, LogicManager logicMgr, Planner planner, boolean explanations) {
            super(self, Nested.class.getSimpleName() + "(pattern: " + conjunction + ")", conjunction, resolutionRecorder,
                  registry, traversalEngine, conceptMgr, logicMgr, planner, explanations);
        }

        @Override
        protected void nextAnswer(Request fromUpstream, ResponseProducer responseProducer, int iteration) {
            if (responseProducer.hasUpstreamAnswer()) {
                AnswerState.UpstreamVars.Derived upstreamAnswer = responseProducer.upstreamAnswers().next();
                responseProducer.recordProduced(upstreamAnswer.withInitialFiltered());
                ResolutionAnswer answer = new ResolutionAnswer(upstreamAnswer, conjunction.toString(),
                                                               ResolutionAnswer.Derivation.EMPTY, self(), false);
                respondToUpstream(Response.Answer.create(fromUpstream, answer), iteration);
            } else {
                if (responseProducer.hasDownstreamProducer()) {
                    requestFromDownstream(responseProducer.nextDownstreamProducer(), fromUpstream, iteration);
                } else {
                    respondToUpstream(new Response.Fail(fromUpstream), iteration);
                }
            }
        }

        @Override
        protected ResourceIterator<AnswerState.UpstreamVars.Derived> toUpstreamAnswers(Request fromUpstream,
                                                                                       ResourceIterator<ConceptMap> downstreamConceptMaps) {
            return downstreamConceptMaps.map(conceptMap -> fromUpstream.partialAnswer().asIdentity()
                    .aggregateToUpstream(conceptMap, null));
        }

        @Override
        protected Optional<AnswerState.UpstreamVars.Derived> toUpstreamAnswer(Request fromUpstream, ConceptMap downstreamConceptMap) {
            return Optional.of(fromUpstream.partialAnswer().asIdentity().aggregateToUpstream(downstreamConceptMap, null));
        }

        @Override
        protected void exception(Throwable e) {
            LOG.error("Actor exception", e);
            // TODO, once integrated into the larger flow of executing queries, kill the actors and report and exception to root
        }
    }
}
