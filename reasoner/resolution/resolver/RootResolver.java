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
import grakn.core.logic.resolvable.Resolvable;
import grakn.core.logic.resolvable.Retrievable;
import grakn.core.pattern.Conjunction;
import grakn.core.reasoner.resolution.Planner;
import grakn.core.reasoner.resolution.ResolutionRecorder;
import grakn.core.reasoner.resolution.ResolverRegistry;
import grakn.core.reasoner.resolution.ResolverRegistry.AlphaEquivalentResolver;
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
import java.util.Set;
import java.util.function.Consumer;

import static grakn.common.collection.Collections.map;
import static grakn.core.common.iterator.Iterators.iterate;
import static grakn.core.reasoner.resolution.answer.AnswerState.UpstreamVars;

/**
 * A root resolver is a special resolver: it is aware that it is not the child any resolver, so does not
 * pass any responses upwards. Instead, it can submit Answers or Exhausted statuses to the owner
 * of the Root resolver.
 */
public class RootResolver extends Resolver<RootResolver> {
    private static final Logger LOG = LoggerFactory.getLogger(RootResolver.class);

    private final Conjunction conjunction;
    private final Set<Concludable> concludables;
    private final Consumer<ResolutionAnswer> onAnswer;
    private final Consumer<Integer> onExhausted;
    private List<Resolvable> plan;
    private final Actor<ResolutionRecorder> resolutionRecorder;
    private final ConceptManager conceptMgr;
    private final LogicManager logicMgr;
    private boolean isInitialised;
    private ResponseProducer responseProducer;
    private final Planner planner;
    private final Map<Resolvable, AlphaEquivalentResolver> downstreamResolvers;

    public RootResolver(Actor<RootResolver> self, Conjunction conjunction, Consumer<ResolutionAnswer> onAnswer,
                        Consumer<Integer> onExhausted, Actor<ResolutionRecorder> resolutionRecorder, ResolverRegistry registry,
                        TraversalEngine traversalEngine, ConceptManager conceptMgr, LogicManager logicMgr, Planner planner, boolean explanations) {
        super(self, RootResolver.class.getSimpleName() + "(pattern:" + conjunction + ")", registry, traversalEngine, explanations);
        this.conjunction = conjunction;
        this.onAnswer = onAnswer;
        this.onExhausted = onExhausted;
        this.resolutionRecorder = resolutionRecorder;
        this.conceptMgr = conceptMgr;
        this.logicMgr = logicMgr;
        this.planner = planner;
        this.isInitialised = false;
        this.concludables = Concludable.create(conjunction);
        this.plan = new ArrayList<>();
        this.downstreamResolvers = new HashMap<>();
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
            onExhausted.accept(iteration);
        } else {
            assert iteration == responseProducer.iteration();
            tryAnswer(fromUpstream, iteration);
        }
    }

    @Override
    protected void receiveAnswer(Response.Answer fromDownstream, int iteration) {
        LOG.trace("{}: received answer: {}", name(), fromDownstream);

        Request toDownstream = fromDownstream.sourceRequest();
        Request fromUpstream = fromUpstream(toDownstream);

        ResolutionAnswer.Derivation derivation;
        if (explanations()) {
            derivation = fromDownstream.sourceRequest().partialResolutions();
            if (fromDownstream.answer().isInferred()) {
                derivation = derivation.withAnswer(fromDownstream.sourceRequest().receiver(), fromDownstream.answer());
            }
        } else {
            derivation = null;
        }

        ConceptMap conceptMap = fromDownstream.answer().derived().withInitial();
        if (fromDownstream.planIndex() == plan.size() - 1) {
            if (!responseProducer.hasProduced(conceptMap)) {
                responseProducer.recordProduced(conceptMap);
                ResolutionAnswer answer = new ResolutionAnswer(fromDownstream.answer().derived(),
                                                               conjunction.toString(), derivation, self(),
                                                               fromDownstream.answer().isInferred());
                submitAnswer(answer);
            } else {
                tryAnswer(fromUpstream, iteration);
            }
        } else {
            int planIndex = fromDownstream.planIndex() + 1;
            AlphaEquivalentResolver nextPlannedDownstream = downstreamResolvers.get(plan.get(planIndex));
            Request downstreamRequest = Request.create(fromUpstream.path().append(nextPlannedDownstream.resolver()),
                                                       UpstreamVars.Initial.of(conceptMap).toDownstreamVars(
                                                               Mapping.of(nextPlannedDownstream.mapping())),
                                                       derivation, planIndex);
            responseProducer.addDownstreamProducer(downstreamRequest);
            requestFromDownstream(downstreamRequest, fromUpstream, iteration);
        }
    }

    @Override
    protected void receiveExhausted(Response.Exhausted fromDownstream, int iteration) {
        LOG.trace("{}: received Exhausted, with iter {}: {}", name(), iteration, fromDownstream);
        Request toDownstream = fromDownstream.sourceRequest();
        Request fromUpstream = fromUpstream(toDownstream);

        if (iteration < responseProducer.iteration()) {
            // short circuit old iteration exhausted messages back out of the actor model
            onExhausted.accept(iteration);
            return;
        }

        responseProducer.removeDownstreamProducer(fromDownstream.sourceRequest());
        tryAnswer(fromUpstream, iteration);

    }

    @Override
    protected void initialiseDownstreamActors() {
        Set<Concludable> concludablesWithApplicableRules = Iterators.iterate(concludables)
                .filter(c -> c.getApplicableRules(conceptMgr, logicMgr).hasNext()).toSet();
        Set<Retrievable> retrievables = Retrievable.extractFrom(conjunction, concludablesWithApplicableRules);
        Set<Resolvable> resolvables = new HashSet<>();
        resolvables.addAll(concludablesWithApplicableRules);
        resolvables.addAll(retrievables);
        plan = planner.plan(resolvables);
        iterate(plan).forEachRemaining(resolvable -> {
            downstreamResolvers.put(resolvable, registry.registerResolvable(resolvable));
        });
    }

    @Override
    protected ResponseProducer responseProducerCreate(Request request, int iteration) {
        LOG.debug("{}: Creating a new ResponseProducer for request: {}", name(), request);
        assert request.answerBounds().isRoot(); // We can ignore the empty ConceptMap of the incoming request

        ResourceIterator<ConceptMap> traversalProducer = traversalEngine.iterator(conjunction.traversal())
                .map(conceptMgr::conceptMap);
        ResponseProducer responseProducer = new ResponseProducer(traversalProducer, iteration);
        Request toDownstream = Request.create(request.path().append(downstreamResolvers.get(plan.get(0)).resolver()),
                                              UpstreamVars.Initial.of(request.answerBounds().conceptMap())
                                                      .toDownstreamVars(Mapping.of(downstreamResolvers.get(plan.get(0)).mapping())),
                                              new ResolutionAnswer.Derivation(map()), 0);
        responseProducer.addDownstreamProducer(toDownstream);

        return responseProducer;
    }

    @Override
    protected ResponseProducer responseProducerReiterate(Request request, ResponseProducer responseProducerPrevious, int newIteration) {
        assert newIteration > responseProducerPrevious.iteration();
        LOG.debug("{}: Updating ResponseProducer for iteration '{}'", name(), newIteration);

        assert newIteration > responseProducerPrevious.iteration();
        ResourceIterator<ConceptMap> traversalIterator = traversalEngine.iterator(conjunction.traversal())
                .map(conceptMgr::conceptMap);
        ResponseProducer responseProducerNewIter = responseProducerPrevious.newIteration(traversalIterator, newIteration);
        Request toDownstream = Request.create(request.path().append(downstreamResolvers.get(plan.get(0)).resolver()),
                                              UpstreamVars.Initial.of(request.answerBounds().conceptMap()).
                                                      toDownstreamVars(Mapping.of(downstreamResolvers.get(plan.get(0)).mapping())),
                                              new ResolutionAnswer.Derivation(map()), 0);
        responseProducerNewIter.addDownstreamProducer(toDownstream);
        return responseProducerNewIter;
    }

    @Override
    protected void exception(Throwable e) {
        LOG.error("Actor exception", e);
        // TODO, once integrated into the larger flow of executing queries, kill the actors and report and exception to root
    }

    private void tryAnswer(Request fromUpstream, int iteration) {
        while (responseProducer.hasTraversalProducer()) {
            ConceptMap conceptMap = responseProducer.traversalProducer().next();
            LOG.trace("{}: has found via traversal: {}", name(), conceptMap);
            if (!responseProducer.hasProduced(conceptMap)) {
                responseProducer.recordProduced(conceptMap);
                assert fromUpstream.answerBounds().isRoot();
                ResolutionAnswer answer = new ResolutionAnswer(fromUpstream.answerBounds().asRoot().aggregateToUpstream(conceptMap),
                                                               conjunction.toString(), ResolutionAnswer.Derivation.EMPTY, self(), false);
                submitAnswer(answer);
                return;
            }
        }

        if (responseProducer.hasDownstreamProducer()) {
            requestFromDownstream(responseProducer.nextDownstreamProducer(), fromUpstream, iteration);
        } else {
            onExhausted.accept(iteration);
        }
    }

    private void mayReiterateResponseProducer(Request fromUpstream, int iteration) {
        if (responseProducer.iteration() + 1 == iteration) {
            responseProducer = responseProducerReiterate(fromUpstream, responseProducer, iteration);
        }
    }

    private void submitAnswer(ResolutionAnswer answer) {
        LOG.debug("Submitting root answer: {}", answer.derived());
        if (explanations()) {
            LOG.trace("Recording root answer: {}", answer);
            resolutionRecorder.tell(state -> state.record(answer));
        }
        onAnswer.accept(answer);
    }
}
