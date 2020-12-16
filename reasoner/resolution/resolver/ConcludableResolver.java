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

import grakn.common.collection.Either;
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
    private final Map<Map<Reference.Name, Set<Reference.Name>>, Actor<RuleResolver>> ruleActorSources;
    private final Map<Actor<RootResolver>, IterationState> iterationStates;
    private Actor<ResolutionRecorder> resolutionRecorder;

    public ConcludableResolver(Actor<ConcludableResolver> self, ConjunctionConcludable<?, ?> concludable) {
        super(self, ConcludableResolver.class.getSimpleName() + "(pattern: " + concludable + ")");
        this.concludable = concludable;
        this.ruleActorSources = new HashMap<>();
        this.iterationStates = new HashMap<>();
    }

    @Override
    public Either<Request, Response> receiveRequest(Request fromUpstream, ResponseProducer responseProducer) {
        return messageToSend(fromUpstream, responseProducer);
    }

    @Override
    public Either<Request, Response> receiveAnswer(Request fromUpstream, Response.Answer fromDownstream,
                                                   ResponseProducer responseProducer) {
        final ConceptMap conceptMap = fromDownstream.answer().aggregated().conceptMap();

        LOG.trace("{}: hasProduced: {}", name, conceptMap);
        if (!responseProducer.hasProduced(conceptMap)) {
            responseProducer.recordProduced(conceptMap);

            // update partial derivation provided from upstream to carry derivations sideways
            ResolutionAnswer.Derivation derivation = new ResolutionAnswer.Derivation(map(pair(fromDownstream.sourceRequest().receiver(),
                                                                                              fromDownstream.answer())));
            ResolutionAnswer answer = new ResolutionAnswer(fromUpstream.partialConceptMap().aggregateWith(conceptMap),
                                                           concludable.toString(), derivation, self(), fromDownstream.answer().isInferred());

            return Either.second(new Response.Answer(fromUpstream, answer));
        } else {
            ResolutionAnswer.Derivation derivation = new ResolutionAnswer.Derivation(map(pair(fromDownstream.sourceRequest().receiver(),
                                                                                              fromDownstream.answer())));
            ResolutionAnswer deduplicated = new ResolutionAnswer(fromUpstream.partialConceptMap().aggregateWith(conceptMap),
                                                                 concludable.toString(), derivation, self(), fromDownstream.answer().isInferred());
            LOG.debug("Recording deduplicated answer: {}", deduplicated);
            resolutionRecorder.tell(actor -> actor.record(deduplicated));

            return messageToSend(fromUpstream, responseProducer);
        }
    }

    @Override
    public Either<Request, Response> receiveExhausted(Request fromUpstream, Response.Exhausted fromDownstream, ResponseProducer responseProducer) {
        responseProducer.removeDownstreamProducer(fromDownstream.sourceRequest());
        return messageToSend(fromUpstream, responseProducer);
    }

    @Override
    protected ResponseProducer responseProducerCreate(Request request) {
        Actor<RootResolver> root = request.path().root();
        iterationStates.putIfAbsent(root, new IterationState(request.getIteration()));
        IterationState iterationState = iterationStates.get(root);
        assert iterationState.iteration() == request.getIteration();

        Iterator<ConceptMap> traversal = (new MockTransaction(3L)).query(concludable.conjunction(), request.partialConceptMap().map());
        ResponseProducer responseProducer = new ResponseProducer(traversal, request.getIteration());

        if (!iterationState.hasReceived(request.partialConceptMap().map())) {
            registerDownstreamRules(responseProducer, request.path(), request.partialConceptMap().map());
            iterationState.recordReceived(request.partialConceptMap().map());
        }
        return responseProducer;
    }

    @Override
    protected ResponseProducer responseProducerReiterate(Request request, ResponseProducer responseProducerPrevious) {
        Actor<RootResolver> root = request.path().root();
        assert iterationStates.containsKey(root);
        IterationState iterationState = iterationStates.get(root);
        if (iterationState.iteration() != request.getIteration()) {
            assert iterationState.iteration + 1 == request.getIteration();
            iterationState.nextIteration();
        }

        Iterator<ConceptMap> traversal = (new MockTransaction(3L)).query(concludable.conjunction(), request.partialConceptMap().map());
        ResponseProducer responseProducerNewIter = responseProducerPrevious.newIteration(traversal, request.getIteration());

        if (!iterationState.hasReceived(request.partialConceptMap().map())) {
            registerDownstreamRules(responseProducerNewIter, request.path(), request.partialConceptMap().map());
            iterationState.recordReceived(request.partialConceptMap().map());
        }
        return responseProducerNewIter;
    }


    @Override
    protected void initialiseDownstreamActors(ResolverRegistry registry) {
        resolutionRecorder = registry.resolutionRecorder();
        concludable.getApplicableRules().forEach(rule -> concludable.getUnifiers(rule).forEach(unifier -> {
            Actor<RuleResolver> ruleActor = registry.registerRule(rule);
            ruleActorSources.put(unifier, ruleActor);
        }));
    }

    private Either<Request, Response> messageToSend(Request fromUpstream, ResponseProducer responseProducer) {
        while (responseProducer.hasTraversalProducer()) {
            ConceptMap conceptMap = responseProducer.traversalProducer().next();
            Aggregator.Aggregated aggregated = fromUpstream.partialConceptMap().aggregateWith(conceptMap);
            if (aggregated == null) {
                // TODO this should be the only place that aggregation can fail, but can we make this explicit?
                continue;
            }

            LOG.trace("{}: hasProduced: {}", name, conceptMap);
            if (!responseProducer.hasProduced(conceptMap)) {
                responseProducer.recordProduced(conceptMap);
                ResolutionAnswer answer = new ResolutionAnswer(aggregated, concludable.toString(), new ResolutionAnswer.Derivation(map()), self(), false);
                return Either.second(new Response.Answer(fromUpstream, answer));
            }
        }

        if (responseProducer.hasDownstreamProducer()) {
            return Either.first(responseProducer.nextDownstreamProducer());
        } else {
            return Either.second(new Response.Exhausted(fromUpstream));
        }
    }

    private void registerDownstreamRules(ResponseProducer responseProducer, Request.Path path, ConceptMap partialConceptMap) {
        for (Map.Entry<Map<Reference.Name, Set<Reference.Name>>, Actor<RuleResolver>> entry : ruleActorSources.entrySet()) {
            Request toDownstream = new Request(path.append(entry.getValue()), UnifyingAggregator.of(partialConceptMap, entry.getKey()), ResolutionAnswer.Derivation.EMPTY);
            responseProducer.addDownstreamProducer(toDownstream);
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

        public void nextIteration() {
            iteration++;
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

