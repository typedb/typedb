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

import grakn.core.common.iterator.ResourceIterator;
import grakn.core.concept.ConceptManager;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concurrent.actor.Actor;
import grakn.core.logic.LogicManager;
import grakn.core.logic.resolvable.Concludable;
import grakn.core.logic.resolvable.Unifier;
import grakn.core.reasoner.resolution.ResolutionRecorder;
import grakn.core.reasoner.resolution.ResolverRegistry;
import grakn.core.reasoner.resolution.answer.AnswerState;
import grakn.core.reasoner.resolution.framework.Request;
import grakn.core.reasoner.resolution.framework.ResolutionAnswer;
import grakn.core.reasoner.resolution.framework.Response;
import grakn.core.reasoner.resolution.framework.Response.Answer;
import grakn.core.reasoner.resolution.framework.ResponseProducer;
import grakn.core.traversal.Traversal;
import grakn.core.traversal.TraversalEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static grakn.common.collection.Collections.map;
import static grakn.common.collection.Collections.pair;

public class ConcludableResolver extends ResolvableResolver<ConcludableResolver> {
    private static final Logger LOG = LoggerFactory.getLogger(ConcludableResolver.class);

    private final Map<Actor<RuleResolver>, Set<Unifier>> applicableRules;
    private final Concludable concludable;
    private final ConceptManager conceptMgr;
    private final LogicManager logicMgr;
    private final Map<Actor<RootResolver>, IterationState> iterationStates;
    private final Actor<ResolutionRecorder> resolutionRecorder;
    private final Map<Request, ResponseProducer> responseProducers;
    private boolean isInitialised;

    public ConcludableResolver(Actor<ConcludableResolver> self, Concludable concludable,
                               Actor<ResolutionRecorder> resolutionRecorder, ResolverRegistry registry,
                               TraversalEngine traversalEngine, ConceptManager conceptMgr, LogicManager logicMgr,
                               boolean explanations) {
        super(self, ConcludableResolver.class.getSimpleName() + "(pattern: " + concludable.conjunction() + ")",
              registry, traversalEngine, explanations);
        this.concludable = concludable;
        this.resolutionRecorder = resolutionRecorder;
        this.conceptMgr = conceptMgr;
        this.logicMgr = logicMgr;
        this.applicableRules = new HashMap<>();
        this.iterationStates = new HashMap<>();
        this.responseProducers = new HashMap<>();
        this.isInitialised = false;
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
            assert iteration == responseProducer.iteration();
            tryAnswer(fromUpstream, responseProducer, iteration);
        }
    }

    @Override
    protected void receiveAnswer(Answer fromDownstream, int iteration) {
        LOG.trace("{}: received Answer: {}", name(), fromDownstream);

        Request toDownstream = fromDownstream.sourceRequest();
        Request fromUpstream = fromUpstream(toDownstream);
        ResponseProducer responseProducer = responseProducers.get(fromUpstream);

        ConceptMap conceptMap = fromDownstream.answer().derived().withInitial();
        if (!responseProducer.hasProduced(conceptMap)) {
            responseProducer.recordProduced(conceptMap);

            ResolutionAnswer.Derivation derivation;
            if (explanations()) { // TODO this way of turning explanations on and off is both error prone and unelegant - can we centralise?
                // update partial derivation provided from upstream to carry derivations sideways
                derivation = new ResolutionAnswer.Derivation(map(pair(fromDownstream.sourceRequest().receiver(),
                                                                      fromDownstream.answer())));
            } else {
                derivation = null;
            }

            ResolutionAnswer answer = new ResolutionAnswer(fromUpstream.answerBounds().asMapped().mapToUpstream(conceptMap),
                                                           concludable.toString(), derivation, self(), fromDownstream.answer().isInferred());

            respondToUpstream(Answer.create(fromUpstream, answer), iteration);
        } else {
            if (explanations()) {
                ResolutionAnswer.Derivation derivation = new ResolutionAnswer.Derivation(map(pair(fromDownstream.sourceRequest().receiver(),
                                                                                                  fromDownstream.answer())));
                ResolutionAnswer deduplicated = new ResolutionAnswer(fromDownstream.answer().derived(), concludable.toString(),
                                                                     derivation, self(), fromDownstream.answer().isInferred());
                LOG.trace("{}: Recording deduplicated answer derivation: {}", name(), deduplicated);
                resolutionRecorder.tell(actor -> actor.record(deduplicated));
            }
            tryAnswer(fromUpstream, responseProducer, iteration);
        }
    }

    @Override
    protected void receiveExhausted(Response.Exhausted fromDownstream, int iteration) {
        LOG.trace("{}: received Exhausted: {}", name(), fromDownstream);
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
        concludable.getApplicableRules(conceptMgr, logicMgr).forEachRemaining(rule -> concludable.getUnifiers(rule)
                .forEachRemaining(unifier -> {
                    Actor<RuleResolver> ruleActor = registry.registerRule(rule);
                    applicableRules.putIfAbsent(ruleActor, new HashSet<>());
                    applicableRules.get(ruleActor).add(unifier);
                }));
    }

    @Override
    protected ResponseProducer responseProducerCreate(Request request, int iteration) {
        LOG.debug("{}: Creating a new ResponseProducer for request: {}", name(), request);
        Actor<RootResolver> root = request.path().root();
        iterationStates.putIfAbsent(root, new IterationState(iteration));
        IterationState iterationState = iterationStates.get(root);

        Traversal traversal = boundTraversal(concludable.conjunction().traversal(), request.answerBounds().conceptMap());
        ResourceIterator<ConceptMap> traversalProducer = traversalEngine.iterator(traversal).map(conceptMgr::conceptMap);

        ResponseProducer responseProducer = new ResponseProducer(traversalProducer, iteration);
        mayRegisterRules(request, iterationState, responseProducer);
        return responseProducer;
    }

    @Override
    protected ResponseProducer responseProducerReiterate(Request request, ResponseProducer responseProducerPrevious, int newIteration) {
        assert newIteration > responseProducerPrevious.iteration();
        LOG.debug("{}: Updating ResponseProducer for iteration '{}'", name(), newIteration);

        Actor<RootResolver> root = request.path().root();
        assert iterationStates.containsKey(root);
        IterationState iterationState = iterationStates.get(root);
        if (iterationState.iteration() > newIteration) {
            iterationState.nextIteration(newIteration);
        }

        Traversal traversal = boundTraversal(concludable.conjunction().traversal(), request.answerBounds().conceptMap());
        ResourceIterator<ConceptMap> traversalProducer = traversalEngine.iterator(traversal).map(conceptMgr::conceptMap);

        ResponseProducer responseProducerNewIter = responseProducerPrevious.newIteration(traversalProducer, newIteration);
        mayRegisterRules(request, iterationState, responseProducerNewIter);
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
            assert fromUpstream.answerBounds().isMapped();
            AnswerState.UpstreamVars.Derived derivedAnswer = fromUpstream.answerBounds().asMapped().mapToUpstream(conceptMap);
            LOG.trace("{}: has found via traversal: {}", name(), conceptMap);
            if (!responseProducer.hasProduced(conceptMap)) {
                responseProducer.recordProduced(conceptMap);
                ResolutionAnswer answer = new ResolutionAnswer(derivedAnswer, concludable.toString(), new ResolutionAnswer.Derivation(map()), self(), false);
                respondToUpstream(Answer.create(fromUpstream, answer), iteration);
                return;
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

    private void mayRegisterRules(Request request, IterationState iterationState, ResponseProducer responseProducer) {
        // loop termination: when receiving a new request, we check if we have seen it before from this root query
        // if we have, we do not allow rules to be registered as possible downstreams
        if (!iterationState.hasReceived(request.answerBounds().conceptMap())) {
            for (Map.Entry<Actor<RuleResolver>, Set<Unifier>> entry : applicableRules.entrySet()) {
                Actor<RuleResolver> ruleActor = entry.getKey();
                for (Unifier unifier : entry.getValue()) {
                    AnswerState.UpstreamVars.Initial initial = AnswerState.UpstreamVars.Initial.of(request.answerBounds().conceptMap());
                    Optional<AnswerState.DownstreamVars.Unified> unified = initial.toDownstreamVars(unifier);
                    if (unified.isPresent()) {
                        Request toDownstream = Request.create(request.path().append(ruleActor), unified.get(),
                                                              ResolutionAnswer.Derivation.EMPTY);
                        responseProducer.addDownstreamProducer(toDownstream);
                    }
                }
            }
            iterationState.recordReceived(request.answerBounds().conceptMap());
        }
    }

    /**
     * Maintain iteration state per root query
     * This allows us to share actors across different queries
     * while maintaining the ability to do loop termination within a single query
     */
    private static class IterationState {
        private Set<ConceptMap> receivedMaps;
        private int iteration;

        IterationState(int iteration) {
            this.iteration = iteration;
            this.receivedMaps = new HashSet<>();
        }

        public int iteration() {
            return iteration;
        }

        public void nextIteration(int newIteration) {
            assert newIteration > iteration;
            iteration = newIteration;
            receivedMaps = new HashSet<>();
        }

        public void recordReceived(ConceptMap conceptMap) {
            receivedMaps.add(conceptMap);
        }

        public boolean hasReceived(ConceptMap conceptMap) {
            return receivedMaps.contains(conceptMap);
        }
    }
}

