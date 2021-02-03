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

import grakn.core.common.exception.GraknException;
import grakn.core.common.iterator.Iterators;
import grakn.core.common.iterator.ResourceIterator;
import grakn.core.concept.Concept;
import grakn.core.concept.ConceptManager;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concurrent.actor.Actor;
import grakn.core.logic.LogicManager;
import grakn.core.logic.Rule;
import grakn.core.logic.resolvable.Concludable;
import grakn.core.logic.resolvable.Resolvable;
import grakn.core.logic.resolvable.Retrievable;
import grakn.core.reasoner.resolution.Planner;
import grakn.core.reasoner.resolution.ResolverRegistry;
import grakn.core.reasoner.resolution.answer.AnswerState.UpstreamVars;
import grakn.core.reasoner.resolution.answer.Mapping;
import grakn.core.reasoner.resolution.framework.Request;
import grakn.core.reasoner.resolution.framework.ResolutionAnswer;
import grakn.core.reasoner.resolution.framework.Resolver;
import grakn.core.reasoner.resolution.framework.Response;
import grakn.core.reasoner.resolution.framework.Response.Answer;
import grakn.core.reasoner.resolution.framework.ResponseProducer;
import grakn.core.traversal.Traversal;
import grakn.core.traversal.TraversalEngine;
import grakn.core.traversal.common.Identifier;
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
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
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

        ConceptMap whenAnswer = fromDownstream.answer().derived().withInitialFiltered();
        if (fromDownstream.planIndex() == plan.size() - 1) {
            ResourceIterator<UpstreamVars.Derived> newAnswers = materialisations(fromUpstream, whenAnswer);
            if (newAnswers.hasNext()) {
                UpstreamVars.Derived upstreamAnswer = newAnswers.next();
                responseProducer.recordProduced(upstreamAnswer.withInitialFiltered());
                // TODO revisit whether using `rule.when()` is the correct pattern to associate with the unified answer? Variables won't match
                ResolutionAnswer answer = new ResolutionAnswer(upstreamAnswer, rule.when().toString(), derivation, self(), true);
                respondToUpstream(Answer.create(fromUpstream, answer), iteration);
            } else {
                tryAnswer(fromUpstream, responseProducer, iteration);
            }
        } else {
            int planIndex = fromDownstream.planIndex() + 1;
            ResolverRegistry.AlphaEquivalentResolver nextPlannedDownstream = downstreamResolvers.get(plan.get(planIndex));
            Request downstreamRequest = Request.create(fromUpstream.path().append(nextPlannedDownstream.resolver()),
                                                       UpstreamVars.Initial.of(whenAnswer).toDownstreamVars(
                                                               Mapping.of(nextPlannedDownstream.mapping())),
                                                       derivation, planIndex, null);
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

        if (iteration < responseProducer.iteration()) {
            // short circuit old iteration exhausted messages to upstream
            respondToUpstream(new Response.Exhausted(fromUpstream), iteration);
            return;
        }

        responseProducer.removeDownstreamProducer(fromDownstream.sourceRequest());
        tryAnswer(fromUpstream, responseProducer, iteration);
    }

    @Override
    protected void initialiseDownstreamActors() {
        LOG.debug("{}: initialising downstream actors", name());
        Set<Concludable> concludables = Iterators.iterate(Concludable.create(rule.when()))
                .filter(c -> c.getApplicableRules(conceptMgr, logicMgr).hasNext()).toSet();
        if (concludables.size() > 0) {
            Set<Retrievable> retrievables = Retrievable.extractFrom(rule.when(), concludables);
            Set<Resolvable> resolvables = new HashSet<>();
            resolvables.addAll(concludables);
            resolvables.addAll(retrievables);

            plan = planner.plan(resolvables);
            iterate(plan).forEachRemaining(resolvable -> {
                downstreamResolvers.put(resolvable, registry.registerResolvable(resolvable));
            });
        }
    }

    @Override
    protected ResponseProducer responseProducerCreate(Request fromUpstream, int iteration) {
        Traversal traversal = boundTraversal(rule.when().traversal(), fromUpstream.partialAnswer().conceptMap());
        ResourceIterator<UpstreamVars.Derived> upstreamAnswers = traversalEngine.iterator(traversal).map(conceptMgr::conceptMap)
                .flatMap(whenAnswer -> materialisations(fromUpstream, whenAnswer));
        ResponseProducer responseProducer = new ResponseProducer(upstreamAnswers, iteration);

        if (!plan.isEmpty()) {
            Request toDownstream = Request.create(fromUpstream.path().append(downstreamResolvers.get(plan.get(0)).resolver()),
                                                  UpstreamVars.Initial.of(fromUpstream.partialAnswer().conceptMap())
                                                          .toDownstreamVars(Mapping.of(downstreamResolvers.get(plan.get(0)).mapping())),
                                                  new ResolutionAnswer.Derivation(map()), 0, null);
            responseProducer.addDownstreamProducer(toDownstream);
        }
        return responseProducer;
    }

    @Override
    protected ResponseProducer responseProducerReiterate(Request fromUpstream, ResponseProducer responseProducerPrevious, int newIteration) {
        assert newIteration > responseProducerPrevious.iteration();
        LOG.debug("{}: Updating ResponseProducer for iteration '{}'", name(), newIteration);

        Traversal traversal = boundTraversal(rule.when().traversal(), fromUpstream.partialAnswer().conceptMap());
        ResourceIterator<UpstreamVars.Derived> upstreamAnswers = traversalEngine.iterator(traversal).map(conceptMgr::conceptMap)
                .flatMap(whenAnswer -> materialisations(fromUpstream, whenAnswer));
        ResponseProducer responseProducerNewIter = responseProducerPrevious.newIteration(upstreamAnswers, newIteration);

        if (!plan.isEmpty()) {
            Request toDownstream = Request.create(fromUpstream.path().append(downstreamResolvers.get(plan.get(0)).resolver()),
                                                  UpstreamVars.Initial.of(fromUpstream.partialAnswer().conceptMap())
                                                          .toDownstreamVars(Mapping.of(downstreamResolvers.get(plan.get(0)).mapping())),
                                                  new ResolutionAnswer.Derivation(map()), 0, null);
            responseProducerNewIter.addDownstreamProducer(toDownstream);
        }
        return responseProducerNewIter;
    }

    @Override
    protected void exception(Throwable e) {
        LOG.error("Actor exception", e);
        // TODO, once integrated into the larger flow of executing queries, kill the actors and report and exception to root
    }

    private void tryAnswer(Request fromUpstream, ResponseProducer responseProducer, int iteration) {
        if (responseProducer.hasUpstreamAnswer()) {
            UpstreamVars.Derived upstreamAnswer = responseProducer.upstreamAnswers().next();
            responseProducer.recordProduced(upstreamAnswer.withInitialFiltered());
            ResolutionAnswer answer = new ResolutionAnswer(upstreamAnswer, rule.when().toString(),
                                                           new ResolutionAnswer.Derivation(map()), self(), true);
            respondToUpstream(Answer.create(fromUpstream, answer), iteration);
        } else {
            if (responseProducer.hasDownstreamProducer()) {
                requestFromDownstream(responseProducer.nextDownstreamProducer(), fromUpstream, iteration);
            } else {
                respondToUpstream(new Response.Exhausted(fromUpstream), iteration);
            }
        }
    }

    private ResourceIterator<UpstreamVars.Derived> materialisations(Request fromUpstream, ConceptMap whenAnswer) {
        ResourceIterator<Map<Identifier, Concept>> materialisations = rule.conclusion()
                .materialise(whenAnswer, traversalEngine, conceptMgr);
        if (!materialisations.hasNext()) throw GraknException.of(ILLEGAL_STATE);

        assert fromUpstream.partialAnswer().isUnified();
        ResourceIterator<UpstreamVars.Derived> upstreamAnswers = materialisations
                .map(concepts -> fromUpstream.partialAnswer().asUnified().unifyToUpstream(concepts))
                .filter(Optional::isPresent)
                .map(Optional::get);
        return upstreamAnswers;
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
