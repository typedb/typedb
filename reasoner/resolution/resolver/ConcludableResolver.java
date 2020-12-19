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
import grakn.core.logic.concludable.ConjunctionConcludable;
import grakn.core.reasoner.resolution.MockTransaction;
import grakn.core.reasoner.resolution.ResolutionRecorder;
import grakn.core.reasoner.resolution.ResolverRegistry;
import grakn.core.reasoner.resolution.answer.Aggregator;
import grakn.core.reasoner.resolution.answer.UnifyingAggregator;
import grakn.core.reasoner.resolution.framework.Request;
import grakn.core.reasoner.resolution.framework.ResolutionAnswer;
import grakn.core.reasoner.resolution.framework.Resolver;
import grakn.core.reasoner.resolution.framework.Response;
import grakn.core.reasoner.resolution.framework.ResponseProducer;
import graql.lang.pattern.variable.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static grakn.common.collection.Collections.map;
import static grakn.common.collection.Collections.pair;

public class ConcludableResolver extends Resolver<ConcludableResolver> {
    private static final Logger LOG = LoggerFactory.getLogger(ConcludableResolver.class);

    private final ConjunctionConcludable<?, ?> concludable;
    private final Map<Map<Reference.Name, Set<Reference.Name>>, Actor<RuleResolver>> availableRules;
    private final Map<Actor<Root>, IterationState> iterationStates;
    private final Actor<ResolutionRecorder> resolutionRecorder;
    private final Map<Request, ResponseProducer> responseProducers;
    private boolean isInitialised;

    public ConcludableResolver(Actor<ConcludableResolver> self, ConjunctionConcludable<?, ?> concludable,
                               Actor<ResolutionRecorder> resolutionRecorder, ResolverRegistry registry) {
        super(self, ConcludableResolver.class.getSimpleName() + "(pattern: " + concludable + ")", registry);
        this.concludable = concludable;
        this.resolutionRecorder = resolutionRecorder;
        this.availableRules = new HashMap<>();
        this.iterationStates = new HashMap<>();
        this.responseProducers = new HashMap<>();
        this.isInitialised = false;
    }

    @Override
    protected void initialiseDownstreamActors() {
        concludable.getApplicableRules().forEach(rule -> concludable.getUnifiers(rule).forEach(unifier -> {
            Actor<RuleResolver> ruleActor = registry.registerRule(rule);
            availableRules.put(unifier, ruleActor);
        }));
    }

    @Override
    protected ResponseProducer responseProducerCreate(Request request, int iteration) {
        Actor<Root> root = request.path().root();
        iterationStates.putIfAbsent(root, new IterationState(iteration));
        IterationState iterationState = iterationStates.get(root);

        Iterator<ConceptMap> traversal = (new MockTransaction(3L)).query(concludable.conjunction(), request.partialConceptMap().map());
        ResponseProducer responseProducer = new ResponseProducer(traversal, iteration);
        mayRegisterRules(request, iterationState, responseProducer);
        return responseProducer;
    }

    @Override
    protected ResponseProducer responseProducerReiterate(Request request, ResponseProducer responseProducerPrevious, int newIteration) {
        assert newIteration > responseProducerPrevious.iteration();

        Actor<Root> root = request.path().root();
        assert iterationStates.containsKey(root);
        IterationState iterationState = iterationStates.get(root);
        if (iterationState.iteration() > newIteration) {
            iterationState.nextIteration(newIteration);
        }

        Iterator<ConceptMap> traversal = (new MockTransaction(3L)).query(concludable.conjunction(), request.partialConceptMap().map());
        ResponseProducer responseProducerNewIter = responseProducerPrevious.newIteration(traversal, newIteration);
        mayRegisterRules(request, iterationState, responseProducerNewIter);
        return responseProducerNewIter;
    }

    @Override
    public void receiveRequest(Request fromUpstream, int iteration) {
        if (!isInitialised) {
            LOG.debug("{}: initialising downstream actors", name());
            initialiseDownstreamActors();
            isInitialised = true;
        }
        ResponseProducer responseProducer = mayUpdateAndGetResponseProducer(fromUpstream, iteration);

        if (iteration < responseProducer.iteration()) {
            // short circuit if the request came from a prior iteration
            respondToUpstream(new Response.Exhausted(fromUpstream), iteration);
        }
        else {
            assert iteration == responseProducer.iteration();
            tryAnswer(fromUpstream, responseProducer, iteration);
        }
    }

    @Override
    protected void receiveAnswer(Response.Answer fromDownstream, int iteration) {
        Request toDownstream = fromDownstream.sourceRequest();
        Request fromUpstream = fromUpstream(toDownstream);
        ResponseProducer responseProducer = responseProducers.get(fromUpstream);

        final ConceptMap conceptMap = fromDownstream.answer().aggregated().conceptMap();

        LOG.trace("{}: received answer: {}", name(), conceptMap);
        if (!responseProducer.hasProduced(conceptMap)) {
            responseProducer.recordProduced(conceptMap);

            // update partial derivation provided from upstream to carry derivations sideways
            ResolutionAnswer.Derivation derivation = new ResolutionAnswer.Derivation(map(pair(fromDownstream.sourceRequest().receiver(),
                                                                                              fromDownstream.answer())));
            ResolutionAnswer answer = new ResolutionAnswer(fromUpstream.partialConceptMap().aggregateWith(conceptMap),
                                                           concludable.toString(), derivation, self(), fromDownstream.answer().isInferred());

            respondToUpstream(new Response.Answer(fromUpstream, answer), iteration);
        } else {
            ResolutionAnswer.Derivation derivation = new ResolutionAnswer.Derivation(map(pair(fromDownstream.sourceRequest().receiver(),
                                                                                              fromDownstream.answer())));
            ResolutionAnswer deduplicated = new ResolutionAnswer(fromUpstream.partialConceptMap().aggregateWith(conceptMap),
                                                                 concludable.toString(), derivation, self(), fromDownstream.answer().isInferred());
            LOG.trace("{}: Recording deduplicated answer derivation: {}", name(), deduplicated);
            resolutionRecorder.tell(actor -> actor.record(deduplicated));

            tryAnswer(fromUpstream, responseProducer, iteration);
        }
    }

    @Override
    protected void receiveExhausted(Response.Exhausted fromDownstream, int iteration) {
        LOG.trace("{}: Receiving a new Exhausted from downstream: {}", name(), fromDownstream);
        Request toDownstream = fromDownstream.sourceRequest();
        Request fromUpstream = fromUpstream(toDownstream);
        ResponseProducer responseProducer = responseProducers.get(fromUpstream);

        responseProducer.removeDownstreamProducer(fromDownstream.sourceRequest());
        tryAnswer(fromUpstream, responseProducer, iteration);
    }

    private void tryAnswer(Request fromUpstream, ResponseProducer responseProducer, int iteration) {
        while (responseProducer.hasTraversalProducer()) {
            ConceptMap conceptMap = responseProducer.traversalProducer().next();
            Aggregator.Aggregated aggregated = fromUpstream.partialConceptMap().aggregateWith(conceptMap);
            if (aggregated == null) {
                // TODO this should be the only place that aggregation can fail, but can we make this explicit?
                continue;
            }

            LOG.trace("{}: has found via traversal: {}", name(), conceptMap);
            if (!responseProducer.hasProduced(conceptMap)) {
                responseProducer.recordProduced(conceptMap);
                ResolutionAnswer answer = new ResolutionAnswer(aggregated, concludable.toString(), new ResolutionAnswer.Derivation(map()), self(), false);
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
            LOG.debug("{}: Creating a new ResponseProducer for request: {}", name(), fromUpstream);
            responseProducers.put(fromUpstream, responseProducerCreate(fromUpstream, iteration));
        } else {
            ResponseProducer responseProducer = responseProducers.get(fromUpstream);

            assert responseProducer.iteration() == iteration ||
                    responseProducer.iteration() + 1 == iteration;

            if (responseProducer.iteration() + 1 == iteration) {
                LOG.debug("{}: Initialising ResponseProducer iteration '{}'  for request: {}", name(), iteration, fromUpstream);
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
        if (!iterationState.hasReceived(request.partialConceptMap().map())) {
            for (Map.Entry<Map<Reference.Name, Set<Reference.Name>>, Actor<RuleResolver>> entry : availableRules.entrySet()) {
                Request toDownstream = new Request(request.path().append(entry.getValue()),
                                                   UnifyingAggregator.of(request.partialConceptMap().map(), entry.getKey()),
                                                   ResolutionAnswer.Derivation.EMPTY);
                responseProducer.addDownstreamProducer(toDownstream);
            }
            iterationState.recordReceived(request.partialConceptMap().map());
        }
    }

    @Override
    protected void exception(Exception e) {
        LOG.error("Actor exception", e);
        // TODO, once integrated into the larger flow of executing queries, kill the actors and report and exception to root
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

