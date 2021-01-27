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
import grakn.core.concept.Concept;
import grakn.core.concept.ConceptManager;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concurrent.actor.Actor;
import grakn.core.logic.LogicManager;
import grakn.core.logic.Rule;
import grakn.core.logic.resolvable.Concludable;
import grakn.core.logic.resolvable.Resolvable;
import grakn.core.logic.resolvable.Retrievable;
import grakn.core.reasoner.resolution.MockTransaction;
import grakn.core.reasoner.resolution.Planner;
import grakn.core.reasoner.resolution.ResolverRegistry;
import grakn.core.reasoner.resolution.answer.AnswerState;
import grakn.core.reasoner.resolution.answer.Mapping;
import grakn.core.reasoner.resolution.framework.Request;
import grakn.core.reasoner.resolution.framework.ResolutionAnswer;
import grakn.core.reasoner.resolution.framework.Resolver;
import grakn.core.reasoner.resolution.framework.Response;
import grakn.core.reasoner.resolution.framework.Response.Answer;
import grakn.core.reasoner.resolution.framework.ResponseProducer;
import grakn.core.traversal.TraversalEngine;
import grakn.core.traversal.common.Identifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static grakn.common.collection.Collections.map;
import static grakn.core.common.iterator.Iterators.iterate;

public class RuleResolver extends Resolver<RuleResolver> {
    private static final Logger LOG = LoggerFactory.getLogger(RuleResolver.class);

    private final Map<Request, ResponseProducer> responseProducers;
    private final Rule rule;
    private List<Resolvable> plan;
    private final ConceptManager conceptMgr;
    private final LogicManager logicMgr;
    private boolean isInitialised;
    private final Planner planner;
    private final Map<Resolvable, ResolverRegistry.AlphaEquivalentResolver> downstreamResolvers;

    public RuleResolver(Actor<RuleResolver> self, Rule rule, ResolverRegistry registry, TraversalEngine traversalEngine,
                        ConceptManager conceptMgr, LogicManager logicMgr, Planner planner, boolean explanations) {
        super(self, RuleResolver.class.getSimpleName() + "(rule:" + rule + ")", registry, traversalEngine, explanations);
        this.conceptMgr = conceptMgr;
        this.logicMgr = logicMgr;
        this.planner = planner;
        this.responseProducers = new HashMap<>();
        this.rule = rule;
        this.plan = new ArrayList<>();
        this.isInitialised = false;
        this.downstreamResolvers = new HashMap<>();
    }

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
            respondToUpstream(new Response.Exhausted(fromUpstream), iteration);
        } else {
            tryAnswer(fromUpstream, responseProducer, iteration);
        }
    }

    @Override
    protected void receiveAnswer(Answer fromDownstream, int iteration) {
        LOG.trace("{}: received Answer: {}", name(), fromDownstream);

        Request toDownstream = fromDownstream.sourceRequest();
        Request fromUpstream = fromUpstream(toDownstream);
        ResponseProducer responseProducer = responseProducers.get(fromUpstream);

        ResolutionAnswer.Derivation derivation;
        if (explanations()) {
            derivation = fromDownstream.sourceRequest().partialResolutions();
            if (fromDownstream.answer().isInferred()) {
                derivation = derivation.withAnswer(fromDownstream.sourceRequest().receiver(), fromDownstream.answer());
            }
        } else {
            derivation = null;
        }

        ConceptMap whenAnswer = fromDownstream.answer().derived().withInitial();
        if (fromDownstream.planIndex() == plan.size() - 1) {
            Map<Identifier, Concept> thenMaterialisation = rule.putConclusion(whenAnswer, traversalEngine, conceptMgr);
            assert fromUpstream.answerBounds().isUnified();
            Optional<AnswerState.UpstreamVars.Derived> unifiedAnswer = fromUpstream.answerBounds().asUnified()
                    .aggregateToUpstream(thenMaterialisation);

            if (unifiedAnswer.isPresent() && !responseProducer.hasProduced(unifiedAnswer.get().conceptMap())) {
                responseProducer.recordProduced(unifiedAnswer.get().conceptMap());
                // TODO revisit whether using `rule.when()` is the correct pattern to associate with the unified answer? Variables won't match
                ResolutionAnswer answer = new ResolutionAnswer(unifiedAnswer.get(), rule.when().toString(), derivation, self(), true);
                respondToUpstream(Answer.create(fromUpstream, answer), iteration);
            } else {
                tryAnswer(fromUpstream, responseProducer, iteration);
            }
        } else {
            int planIndex = fromDownstream.planIndex() + 1;
            ResolverRegistry.AlphaEquivalentResolver nextPlannedDownstream = downstreamResolvers.get(plan.get(planIndex));
            Request downstreamRequest = Request.create(fromUpstream.path().append(nextPlannedDownstream.resolver()),
                                                       AnswerState.UpstreamVars.Initial.of(whenAnswer).toDownstreamVars(
                                                               Mapping.of(nextPlannedDownstream.mapping())),
                                                       derivation, planIndex);
            responseProducer.addDownstreamProducer(downstreamRequest);
            requestFromDownstream(downstreamRequest, fromUpstream, iteration);
        }
    }

    @Override
    protected void receiveExhausted(Response.Exhausted fromDownstream, int iteration) {
        LOG.trace("{}: Receiving Exhausted: {}", name(), fromDownstream);
        Request toDownstream = fromDownstream.sourceRequest();
        Request fromUpstream = fromUpstream(toDownstream);
        ResponseProducer responseProducer = responseProducers.get(fromUpstream);

        responseProducer.removeDownstreamProducer(fromDownstream.sourceRequest());
        tryAnswer(fromUpstream, responseProducer, iteration);
    }

    @Override
    protected void initialiseDownstreamActors() {
        LOG.debug("{}: initialising downstream actors", name());

        Set<Concludable> concludablesWithApplicableRules = Iterators.iterate(rule.whenConcludables())
                .filter(c -> c.getApplicableRules(conceptMgr, logicMgr).hasNext()).toSet();
        Set<Retrievable> retrievables = Retrievable.extractFrom(rule.when(), concludablesWithApplicableRules);
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
        Iterator<ConceptMap> traversal = (new MockTransaction(3L)).query(rule.when(), new ConceptMap());
        ResponseProducer responseProducer = new ResponseProducer(traversal, iteration);
        Request toDownstream = Request.create(request.path().append(downstreamResolvers.get(plan.get(0)).resolver()),
                                              AnswerState.UpstreamVars.Initial.of(request.answerBounds().conceptMap())
                                                      .toDownstreamVars(Mapping.of(downstreamResolvers.get(plan.get(0)).mapping())),
                                              new ResolutionAnswer.Derivation(map()), 0);
        responseProducer.addDownstreamProducer(toDownstream);

        return responseProducer;
    }

    @Override
    protected ResponseProducer responseProducerReiterate(Request request, ResponseProducer responseProducerPrevious, int newIteration) {
        assert newIteration > responseProducerPrevious.iteration();
        LOG.debug("{}: Updating ResponseProducer for iteration '{}'", name(), newIteration);

        Iterator<ConceptMap> traversal = (new MockTransaction(3L)).query(rule.when(), new ConceptMap());
        ResponseProducer responseProducerNewIter = responseProducerPrevious.newIteration(traversal, newIteration);
        Request toDownstream = Request.create(request.path().append(downstreamResolvers.get(plan.get(0)).resolver()),
                                              AnswerState.UpstreamVars.Initial.of(request.answerBounds().conceptMap())
                                                      .toDownstreamVars(Mapping.of(downstreamResolvers.get(plan.get(0)).mapping())),
                                              new ResolutionAnswer.Derivation(map()), 0);
        responseProducerNewIter.addDownstreamProducer(toDownstream);
        return responseProducerNewIter;
    }

    @Override
    protected void exception(Throwable e) {
        LOG.error("Actor exception", e);
        // TODO, once integrated into the larger flow of executing queries, kill the actors and report and exception to root
    }

    private void tryAnswer(Request fromUpstream, ResponseProducer responseProducer, int iteration) {
        while (responseProducer.hasTraversalProducer()) {
            ConceptMap conceptMap = responseProducer.traversalProducer().next();
            LOG.trace("{}: has found via traversal: {}", name(), conceptMap);
            if (!responseProducer.hasProduced(conceptMap)) {
                responseProducer.recordProduced(conceptMap);
                assert fromUpstream.answerBounds().isUnified();
                Optional<AnswerState.UpstreamVars.Derived> derivedAnswer = fromUpstream.answerBounds().asUnified()
                        .aggregateToUpstream(MockTransaction.asIdentifiedMap(conceptMap));
                if (derivedAnswer.isPresent()) {
                    ResolutionAnswer answer = new ResolutionAnswer(derivedAnswer.get(), rule.when().toString(),
                                                                   ResolutionAnswer.Derivation.EMPTY, self(), true);
                    respondToUpstream(Answer.create(fromUpstream, answer), iteration);
                }
            }
        }

        if (responseProducer.hasDownstreamProducer()) {
            requestFromDownstream(responseProducer.nextDownstreamProducer(), fromUpstream, iteration);
        } else {
            respondToUpstream(new Response.Exhausted(fromUpstream), iteration);
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
}
