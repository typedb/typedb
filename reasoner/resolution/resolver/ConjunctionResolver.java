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
import java.util.Set;

import static grakn.common.collection.Collections.map;
import static grakn.core.common.iterator.Iterators.iterate;

public abstract class ConjunctionResolver<T extends ConjunctionResolver<T>> extends Resolver<T> {

    private static final Logger LOG = LoggerFactory.getLogger(Root.Conjunction.class);

    private final ConceptManager conceptMgr;
    private final LogicManager logicMgr;
    private final Planner planner;
    final grakn.core.pattern.Conjunction conjunction;
    final Actor<ResolutionRecorder> resolutionRecorder;
    final List<Resolvable> plan;
    final Map<Request, ResponseProducer> responseProducers;
    final Map<Resolvable, ResolverRegistry.AlphaEquivalentResolver> downstreamResolvers;
    boolean isInitialised;

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

    @Override
    public abstract void receiveRequest(Request fromUpstream, int iteration);


    @Override
    protected abstract void receiveAnswer(Response.Answer fromDownstream, int iteration);

    @Override
    protected abstract void receiveExhausted(Response.Fail fromDownstream, int iteration);

    @Override
    protected void initialiseDownstreamActors() {
        LOG.debug("{}: initialising downstream actors", name());
        Set<Concludable> concludables = Iterators.iterate(Concludable.create(conjunction))
                .filter(c -> c.getApplicableRules(conceptMgr, logicMgr).hasNext()).toSet();
        if (concludables.size() > 0) {
            Set<Retrievable> retrievables = Retrievable.extractFrom(conjunction, concludables);
            Set<Resolvable> resolvables = new HashSet<>();
            resolvables.addAll(concludables);
            resolvables.addAll(retrievables);

            plan.addAll(planner.plan(resolvables));
            iterate(plan).forEachRemaining(resolvable -> {
                downstreamResolvers.put(resolvable, registry.registerResolvable(resolvable));
            });
        }
    }

    @Override
    protected ResponseProducer responseProducerCreate(Request fromUpstream, int iteration) {
        LOG.debug("{}: Creating a new ResponseProducer for request: {}", name(), fromUpstream);

        assert fromUpstream.partialAnswer().isIdentity();
        ResourceIterator<AnswerState.UpstreamVars.Derived> upstreamAnswers = traversalEngine.iterator(conjunction.traversal())
                .map(conceptMgr::conceptMap)
                .map(conceptMap -> fromUpstream.partialAnswer().asIdentity().aggregateToUpstream(conceptMap, fromUpstream.filter()));

        ResponseProducer responseProducer = new ResponseProducer(upstreamAnswers, iteration);

        if (!plan.isEmpty()) {
            Request toDownstream = Request.create(fromUpstream.path().append(downstreamResolvers.get(plan.get(0)).resolver()),
                                                  Initial.of(fromUpstream.partialAnswer().conceptMap())
                                                          .toDownstreamVars(Mapping.of(downstreamResolvers.get(plan.get(0)).mapping())),
                                                  new ResolutionAnswer.Derivation(map()), 0, null);
            responseProducer.addDownstreamProducer(toDownstream);
        }
        return responseProducer;
    }


    @Override
    protected ResponseProducer responseProducerReiterate(Request fromUpstream, ResponseProducer responseProducerPrevious,
                                                         int newIteration) {
        assert newIteration > responseProducerPrevious.iteration() && fromUpstream.partialAnswer().isIdentity();
        LOG.debug("{}: Updating ResponseProducer for iteration '{}'", name(), newIteration);

        ResourceIterator<AnswerState.UpstreamVars.Derived> upstreamAnswers = traversalEngine.iterator(conjunction.traversal())
                .map(conceptMgr::conceptMap)
                .map(conceptMap -> fromUpstream.partialAnswer().asIdentity().aggregateToUpstream(conceptMap, null));
        ResponseProducer responseProducerNewIter = responseProducerPrevious.newIteration(upstreamAnswers, newIteration);
        if (!plan.isEmpty()) {
            Request toDownstream = Request.create(fromUpstream.path().append(downstreamResolvers.get(plan.get(0)).resolver()),
                                                  Initial.of(fromUpstream.partialAnswer().conceptMap())
                                                          .toDownstreamVars(Mapping.of(downstreamResolvers.get(plan.get(0)).mapping())),
                                                  new ResolutionAnswer.Derivation(map()), 0, null);
            responseProducerNewIter.addDownstreamProducer(toDownstream);
        }
        return responseProducerNewIter;
    }

    public static class Simple extends ConjunctionResolver<Simple> {

        public Simple(Actor<Simple> self, Conjunction conjunction, Actor<ResolutionRecorder> resolutionRecorder, ResolverRegistry registry, TraversalEngine traversalEngine, ConceptManager conceptMgr, LogicManager logicMgr, Planner planner, boolean explanations) {
            super(self, Simple.class.getSimpleName() + "(pattern: " + conjunction + ")", conjunction, resolutionRecorder,
                  registry, traversalEngine, conceptMgr, logicMgr, planner, explanations);
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
                respondToUpstream(new Response.Fail(fromUpstream), iteration);
            } else {
                assert iteration == responseProducer.iteration();
                tryAnswer(fromUpstream, responseProducer, iteration);
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
                // TODO
                derivation = null;
            } else {
                derivation = null;
            }

            ConceptMap conceptMap = fromDownstream.answer().derived().withInitialFiltered();
            if (fromDownstream.planIndex() == plan.size() - 1) {
                assert fromUpstream.partialAnswer().isIdentity() && fromUpstream.filter() != null;
                AnswerState.UpstreamVars.Derived answer = fromUpstream.partialAnswer().asIdentity()
                        .aggregateToUpstream(conceptMap, null);
                ConceptMap filteredMap = answer.withInitialFiltered();
                if (!responseProducer.hasProduced(filteredMap)) {
                    responseProducer.recordProduced(filteredMap);
                    ResolutionAnswer resolutionAnswer = new ResolutionAnswer(answer, conjunction.toString(), derivation, self(),
                                                                             fromDownstream.answer().isInferred());
                    respondToUpstream(Response.Answer.create(fromUpstream, resolutionAnswer), iteration);
                } else {
                    tryAnswer(fromUpstream, responseProducer, iteration);
                }
            } else {
                int planIndex = fromDownstream.planIndex() + 1;
                ResolverRegistry.AlphaEquivalentResolver nextPlannedDownstream = downstreamResolvers.get(plan.get(planIndex));
                Request downstreamRequest = Request.create(fromUpstream.path().append(nextPlannedDownstream.resolver()),
                                                           Initial.of(conceptMap).toDownstreamVars(
                                                                   Mapping.of(nextPlannedDownstream.mapping())),
                                                           derivation, planIndex, null);
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
            tryAnswer(fromUpstream, responseProducer, iteration);
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

        void tryAnswer(Request fromUpstream, ResponseProducer responseProducer, int iteration) {
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
        protected void exception(Throwable e) {
            LOG.error("Actor exception", e);
            // TODO, once integrated into the larger flow of executing queries, kill the actors and report and exception to root
        }
    }
}
