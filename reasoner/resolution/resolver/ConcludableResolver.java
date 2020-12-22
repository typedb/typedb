/*
 * Copyright (C) 2020 Grakn Labs
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

import grakn.common.concurrent.actor.Actor;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.logic.resolvable.Concludable;
import grakn.core.logic.transformer.Unifier;
import grakn.core.reasoner.resolution.MockTransaction;
import grakn.core.reasoner.resolution.ResolutionRecorder;
import grakn.core.reasoner.resolution.ResolverRegistry;
import grakn.core.reasoner.resolution.answer.AnswerState;
import grakn.core.reasoner.resolution.framework.Request;
import grakn.core.reasoner.resolution.framework.ResolutionAnswer;
import grakn.core.reasoner.resolution.framework.Resolver;
import grakn.core.reasoner.resolution.framework.Response;
import grakn.core.reasoner.resolution.framework.ResponseProducer;
import grakn.core.traversal.TraversalEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static grakn.common.collection.Collections.map;
import static grakn.common.collection.Collections.pair;

public class ConcludableResolver extends Resolver<ConcludableResolver> {
    private static final Logger LOG = LoggerFactory.getLogger(ConcludableResolver.class);

    private final Concludable<?> concludable;
    private final Map<Unifier, Actor<RuleResolver>> availableRules;
    private final Map<Actor<RootResolver>, IterationState> iterationStates;
    private final Actor<ResolutionRecorder> resolutionRecorder;
    private final Map<Request, ResponseProducer> responseProducers;
    private boolean isInitialised;

    public ConcludableResolver(Actor<ConcludableResolver> self, Concludable<?> concludable,
                               Actor<ResolutionRecorder> resolutionRecorder, ResolverRegistry registry,
                               TraversalEngine traversalEngine) {
        super(self, ConcludableResolver.class.getSimpleName() + "(pattern: " + concludable + ")", registry, traversalEngine);
        this.concludable = concludable;
        this.resolutionRecorder = resolutionRecorder;
        this.availableRules = new HashMap<>();
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
    protected void receiveAnswer(Response.Answer fromDownstream, int iteration) {
        LOG.trace("{}: received Answer: {}", name(), fromDownstream);

        Request toDownstream = fromDownstream.sourceRequest();
        Request fromUpstream = fromUpstream(toDownstream);
        ResponseProducer responseProducer = responseProducers.get(fromUpstream);

        ConceptMap conceptMap = fromDownstream.answer().derived().map();
        if (!responseProducer.hasProduced(conceptMap)) {
            responseProducer.recordProduced(conceptMap);

            // update partial derivation provided from upstream to carry derivations sideways
            ResolutionAnswer.Derivation derivation = new ResolutionAnswer.Derivation(map(pair(fromDownstream.sourceRequest().receiver(),
                                                                                              fromDownstream.answer())));
            ResolutionAnswer answer = new ResolutionAnswer(fromUpstream.initialAnswer().aggregateWith(conceptMap).asMapped().toUpstreamVars(),
                                                           concludable.toString(), derivation, self(), fromDownstream.answer().isInferred());

            respondToUpstream(new Response.Answer(fromUpstream, answer), iteration);
        } else {
            ResolutionAnswer.Derivation derivation = new ResolutionAnswer.Derivation(map(pair(fromDownstream.sourceRequest().receiver(),
                                                                                              fromDownstream.answer())));
            ResolutionAnswer deduplicated = new ResolutionAnswer(fromUpstream.initialAnswer().aggregateWith(conceptMap).asMapped().toUpstreamVars(),
                                                                 concludable.toString(), derivation, self(), fromDownstream.answer().isInferred());
            LOG.trace("{}: Recording deduplicated answer derivation: {}", name(), deduplicated);
            resolutionRecorder.tell(actor -> actor.record(deduplicated));

            tryAnswer(fromUpstream, responseProducer, iteration);
        }
    }

    @Override
    protected void receiveExhausted(Response.Exhausted fromDownstream, int iteration) {
        LOG.trace("{}: received Exhausted: {}", name(), fromDownstream);
        Request toDownstream = fromDownstream.sourceRequest();
        Request fromUpstream = fromUpstream(toDownstream);
        ResponseProducer responseProducer = responseProducers.get(fromUpstream);

        responseProducer.removeDownstreamProducer(fromDownstream.sourceRequest());
        tryAnswer(fromUpstream, responseProducer, iteration);
    }

    @Override
    protected void initialiseDownstreamActors() {
        LOG.debug("{}: initialising downstream actors", name());
        concludable.getApplicableRules().forEach(rule -> concludable.getUnifiers(rule).forEach(unifier -> {
            Actor<RuleResolver> ruleActor = registry.registerRule(rule);
            availableRules.put(unifier, ruleActor);
        }));
    }

    @Override
    protected ResponseProducer responseProducerCreate(Request request, int iteration) {
        LOG.debug("{}: Creating a new ResponseProducer for request: {}", name(), request);
        Actor<RootResolver> root = request.path().root();
        iterationStates.putIfAbsent(root, new IterationState(iteration));
        IterationState iterationState = iterationStates.get(root);

        Iterator<ConceptMap> traversal = (new MockTransaction(3L)).query(concludable.conjunction(), request.initialAnswer().map());
        ResponseProducer responseProducer = new ResponseProducer(traversal, iteration);
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

        Iterator<ConceptMap> traversal = (new MockTransaction(3L)).query(concludable.conjunction(), request.initialAnswer().map());
        ResponseProducer responseProducerNewIter = responseProducerPrevious.newIteration(traversal, newIteration);
        mayRegisterRules(request, iterationState, responseProducerNewIter);
        return responseProducerNewIter;
    }

    @Override
    protected void exception(Exception e) {
        LOG.error("Actor exception", e);
        // TODO, once integrated into the larger flow of executing queries, kill the actors and report and exception to root
    }

    private void tryAnswer(Request fromUpstream, ResponseProducer responseProducer, int iteration) {
        while (responseProducer.hasTraversalProducer()) {
            ConceptMap conceptMap = responseProducer.traversalProducer().next();
            AnswerState.UpstreamVars.Derived derivedAnswer = fromUpstream.initialAnswer().aggregateWith(conceptMap).asMapped().toUpstreamVars();
            LOG.trace("{}: has found via traversal: {}", name(), conceptMap);
            if (!responseProducer.hasProduced(conceptMap)) {
                responseProducer.recordProduced(conceptMap);
                ResolutionAnswer answer = new ResolutionAnswer(derivedAnswer, concludable.toString(), new ResolutionAnswer.Derivation(map()), self(), false);
                respondToUpstream(new Response.Answer(fromUpstream, answer), iteration);
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
        if (!iterationState.hasReceived(request.initialAnswer().map())) {
            for (Map.Entry<Unifier, Actor<RuleResolver>> entry : availableRules.entrySet()) {
                Optional<AnswerState.DownstreamVars.Initial> unified = AnswerState.UpstreamVars.Initial.of(request.initialAnswer().map()).toDownstreamVars(entry.getKey());
                if (unified.isPresent()) {
                    Request toDownstream = new Request(request.path().append(entry.getValue()), unified.get(),
                                                       ResolutionAnswer.Derivation.EMPTY);
                    responseProducer.addDownstreamProducer(toDownstream);
                }
            }
            iterationState.recordReceived(request.initialAnswer().map());
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

