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
import grakn.core.reasoner.resolution.answer.AnswerState.UpstreamVars;
import grakn.core.reasoner.resolution.framework.Request;
import grakn.core.reasoner.resolution.framework.ResolutionAnswer;
import grakn.core.reasoner.resolution.framework.Resolver;
import grakn.core.reasoner.resolution.framework.Response;
import grakn.core.reasoner.resolution.framework.Response.Answer;
import grakn.core.reasoner.resolution.framework.ResponseProducer;
import grakn.core.traversal.TraversalEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static grakn.common.collection.Collections.map;
import static grakn.common.collection.Collections.pair;

public class ConcludableResolver extends Resolver<ConcludableResolver> {
    private static final Logger LOG = LoggerFactory.getLogger(ConcludableResolver.class);

    private final LinkedHashMap<Actor<RuleResolver>, Set<Unifier>> applicableRules;
    private final Concludable concludable;
    private final ConceptManager conceptMgr;
    private final LogicManager logicMgr;
    private final Map<Actor<? extends Resolver<?>>, RecursionState> recursionStates;
    private final Actor<ResolutionRecorder> resolutionRecorder;
    private final Map<Request, ResponseProducer> responseProducers;
    private boolean isInitialised;

    public ConcludableResolver(Actor<ConcludableResolver> self, Concludable concludable,
                               Actor<ResolutionRecorder> resolutionRecorder, ResolverRegistry registry,
                               TraversalEngine traversalEngine, ConceptManager conceptMgr, LogicManager logicMgr,
                               boolean explanations) {
        super(self, ConcludableResolver.class.getSimpleName() + "(pattern: " + concludable.pattern() + ")",
              registry, traversalEngine, explanations);
        this.conceptMgr = conceptMgr;
        this.logicMgr = logicMgr;
        this.resolutionRecorder = resolutionRecorder;
        this.concludable = concludable;
        this.applicableRules = new LinkedHashMap<>();
        this.recursionStates = new HashMap<>();
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
            respondToUpstream(new Response.Fail(fromUpstream), iteration);
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

        ConceptMap conceptMap = fromDownstream.answer().derived().withInitialFiltered();
        UpstreamVars.Derived upstreamAnswer = fromUpstream.partialAnswer().asMapped().mapToUpstream(conceptMap);
        if (!responseProducer.hasProduced(upstreamAnswer.withInitialFiltered())) {
            responseProducer.recordProduced(upstreamAnswer.withInitialFiltered());

            ResolutionAnswer.Derivation derivation;
            if (explanations()) { // TODO: this way of turning explanations on and off is both error prone and unelegant - can we centralise?
                // update partial derivation provided from upstream to carry derivations sideways
                derivation = new ResolutionAnswer.Derivation(map(pair(fromDownstream.sourceRequest().receiver(),
                                                                      fromDownstream.answer())));
            } else {
                derivation = null;
            }

            ResolutionAnswer answer = new ResolutionAnswer(upstreamAnswer, concludable.toString(), derivation, self(),
                                                           fromDownstream.answer().isInferred());

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
    protected void receiveExhausted(Response.Fail fromDownstream, int iteration) {
        LOG.trace("{}: received Exhausted: {}", name(), fromDownstream);
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
    protected ResponseProducer responseProducerCreate(Request fromUpstream, int iteration) {
        LOG.debug("{}: Creating a new ResponseProducer for request: {}", name(), fromUpstream);
        Actor<? extends Resolver<?>> root = fromUpstream.path().root();
        recursionStates.putIfAbsent(root, new RecursionState(iteration));
        RecursionState iterationState = recursionStates.get(root);

        assert fromUpstream.partialAnswer().isMapped();
        ResourceIterator<UpstreamVars.Derived> upstreamAnswers =
                compatibleBoundAnswers(conceptMgr, concludable.pattern(), fromUpstream.partialAnswer().conceptMap())
                        .map(conceptMap -> fromUpstream.partialAnswer().asMapped().mapToUpstream(conceptMap));

        ResponseProducer responseProducer = new ResponseProducer(upstreamAnswers, iteration);
        mayRegisterRules(fromUpstream, iterationState, responseProducer);
        return responseProducer;
    }

    @Override
    protected ResponseProducer responseProducerReiterate(Request fromUpstream, ResponseProducer responseProducerPrevious,
                                                         int newIteration) {
        assert newIteration > responseProducerPrevious.iteration();
        LOG.debug("{}: Updating ResponseProducer for iteration '{}'", name(), newIteration);

        Actor<? extends Resolver<?>> root = fromUpstream.path().root();
        assert recursionStates.containsKey(root);
        RecursionState iterationState = recursionStates.get(root);
        if (iterationState.iteration() < newIteration) {
            iterationState.nextIteration(newIteration);
        }

        assert fromUpstream.partialAnswer().isMapped();
        ResourceIterator<UpstreamVars.Derived> upstreamAnswers =
                compatibleBoundAnswers(conceptMgr, concludable.pattern(), fromUpstream.partialAnswer().conceptMap())
                .map(conceptMap -> fromUpstream.partialAnswer().asMapped().mapToUpstream(conceptMap));

        ResponseProducer responseProducerNewIter = responseProducerPrevious.newIteration(upstreamAnswers, newIteration);
        mayRegisterRules(fromUpstream, iterationState, responseProducerNewIter);
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
            ResolutionAnswer answer = new ResolutionAnswer(upstreamAnswer, concludable.toString(),
                                                           new ResolutionAnswer.Derivation(map()), self(), false);
            respondToUpstream(Answer.create(fromUpstream, answer), iteration);
        } else {
            if (responseProducer.hasDownstreamProducer()) {
                requestFromDownstream(responseProducer.nextDownstreamProducer(), fromUpstream, iteration);
            } else {
                respondToUpstream(new Response.Fail(fromUpstream), iteration);
            }
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

    private void mayRegisterRules(Request fromUpstream, RecursionState recursionState, ResponseProducer responseProducer) {
        // loop termination: when receiving a new request, we check if we have seen it before from this root query
        // if we have, we do not allow rules to be registered as possible downstreams
        if (!recursionState.hasReceived(fromUpstream.partialAnswer().conceptMap())) {
            for (Map.Entry<Actor<RuleResolver>, Set<Unifier>> entry : applicableRules.entrySet()) {
                Actor<RuleResolver> ruleActor = entry.getKey();
                for (Unifier unifier : entry.getValue()) {
                    UpstreamVars.Initial initial = UpstreamVars.Initial.of(fromUpstream.partialAnswer().conceptMap());
                    Optional<AnswerState.DownstreamVars.Unified> unified = initial.toDownstreamVars(unifier);
                    if (unified.isPresent()) {
                        Request toDownstream = Request.create(fromUpstream.path().append(ruleActor, unified.get()), unified.get(),
                                                              ResolutionAnswer.Derivation.EMPTY);
                        responseProducer.addDownstreamProducer(toDownstream);
                    }
                }
            }
            recursionState.recordReceived(fromUpstream.partialAnswer().conceptMap());
        }
    }

    /**
     * Maintain iteration state per root query
     * This allows us to share actors across different queries
     * while maintaining the ability to do loop termination within a single query
     */
    private static class RecursionState {
        private Set<ConceptMap> receivedMaps;
        private int iteration;

        RecursionState(int iteration) {
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

