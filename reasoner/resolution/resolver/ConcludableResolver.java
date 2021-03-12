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
import grakn.core.common.iterator.FunctionalIterator;
import grakn.core.common.iterator.Iterators;
import grakn.core.concept.ConceptManager;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.logic.LogicManager;
import grakn.core.logic.Rule;
import grakn.core.logic.resolvable.Unifier;
import grakn.core.pattern.Conjunction;
import grakn.core.reasoner.resolution.ResolverRegistry;
import grakn.core.reasoner.resolution.answer.AnswerState.Partial;
import grakn.core.reasoner.resolution.framework.Request;
import grakn.core.reasoner.resolution.framework.Resolver;
import grakn.core.reasoner.resolution.framework.Response;
import grakn.core.reasoner.resolution.framework.Response.Answer;
import grakn.core.traversal.TraversalEngine;
import grakn.core.traversal.common.Identifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static grakn.core.common.iterator.Iterators.iterate;

public class ConcludableResolver extends Resolver<ConcludableResolver> {

    private static final Logger LOG = LoggerFactory.getLogger(ConcludableResolver.class);

    private final LinkedHashMap<Driver<ConclusionResolver>, Set<Unifier>> applicableRules;
    private final Map<Driver<ConclusionResolver>, Rule> resolverRules;
    private final grakn.core.logic.resolvable.Concludable concludable;
    private final LogicManager logicMgr;
    private final Map<Driver<? extends Resolver<?>>, RecursionState> recursionStates;
    private final Map<Request, RequestState> requestStates;
    private final Set<Identifier.Variable.Retrievable> unboundVars;
    private boolean isInitialised;

    public ConcludableResolver(Driver<ConcludableResolver> driver, grakn.core.logic.resolvable.Concludable concludable,
                               ResolverRegistry registry, TraversalEngine traversalEngine, ConceptManager conceptMgr,
                               LogicManager logicMgr, boolean resolutionTracing) {
        super(driver, ConcludableResolver.class.getSimpleName() + "(pattern: " + concludable.pattern() + ")",
              registry, traversalEngine, conceptMgr, resolutionTracing);
        this.logicMgr = logicMgr;
        this.concludable = concludable;
        this.applicableRules = new LinkedHashMap<>();
        this.resolverRules = new HashMap<>();
        this.recursionStates = new HashMap<>();
        this.requestStates = new HashMap<>();
        this.unboundVars = unboundVars(concludable.pattern());
        this.isInitialised = false;
    }

    @Override
    public void receiveRequest(Request fromUpstream, int iteration) {
        LOG.trace("{}: received Request: {}", name(), fromUpstream);
        if (!isInitialised) initialiseDownstreamResolvers();
        if (isTerminated()) return;

        RequestState requestState = getOrUpdateRequestState(fromUpstream, iteration);
        if (iteration < requestState.iteration()) {
            // short circuit if the request came from a prior iteration
            failToUpstream(fromUpstream, iteration);
        } else {
            assert iteration == requestState.iteration();
            nextAnswer(fromUpstream, requestState, iteration);
        }
    }

    @Override
    protected void receiveAnswer(Answer fromDownstream, int iteration) {
        LOG.trace("{}: received Answer: {}", name(), fromDownstream);
        if (isTerminated()) return;

        Request toDownstream = fromDownstream.sourceRequest();
        Request fromUpstream = fromUpstream(toDownstream);
        RequestState requestState = this.requestStates.get(fromUpstream);

        Partial.Compound<?, ?> upstreamAnswer = fromDownstream.answer().asConcludable().toUpstreamInferred();

        if (upstreamAnswer.isExplainRoot()) {
            answerFound(upstreamAnswer, fromUpstream, iteration);
        } else if (!requestState.hasProduced(upstreamAnswer.conceptMap())) {
            requestState.recordProduced(upstreamAnswer.conceptMap());
            answerFound(upstreamAnswer, fromUpstream, iteration);
        } else {
            nextAnswer(fromUpstream, requestState, iteration);
        }
    }

    /*
    When we only require 1 answer (eg. when the conjunction is already fully bound), we can short circuit
    and prevent exploration of further rules.

    One latency optimisation we could do here, is keep track of how many N repeated requests are received,
    forward them downstream (to parallelise searching for the single answer), and when the first one finds an answer,
    we respond for all N ahead of time. Then, when the rules actually return an answer to this concludable, we do nothing.
     */
    private void answerFound(Partial.Compound<?, ?> upstreamAnswer, Request fromUpstream, int iteration) {
        RequestState requestState = this.requestStates.get(fromUpstream);
        if (requestState.singleAnswerRequired() && !upstreamAnswer.isExplainRoot()) {
            requestState.clearDownstreamProducers();
        }
        answerToUpstream(upstreamAnswer, fromUpstream, iteration);
    }

    @Override
    protected void receiveFail(Response.Fail fromDownstream, int iteration) {
        LOG.trace("{}: received Fail: {}", name(), fromDownstream);
        if (isTerminated()) return;

        Request toDownstream = fromDownstream.sourceRequest();
        Request fromUpstream = fromUpstream(toDownstream);
        RequestState requestState = this.requestStates.get(fromUpstream);

        if (iteration < requestState.iteration()) {
            // short circuit old iteration failed messages to upstream
            failToUpstream(fromUpstream, iteration);
            return;
        }

        requestState.removeDownstreamProducer(fromDownstream.sourceRequest());
        nextAnswer(fromUpstream, requestState, iteration);
    }

    @Override
    public void terminate(Throwable cause) {
        super.terminate(cause);
        requestStates.clear();
        recursionStates.clear();
    }

    @Override
    protected void initialiseDownstreamResolvers() {
        LOG.debug("{}: initialising downstream resolvers", name());
        concludable.getApplicableRules(conceptMgr, logicMgr).forEachRemaining(rule -> concludable.getUnifiers(rule)
                .forEachRemaining(unifier -> {
                    if (isTerminated()) return;
                    try {
                        Driver<ConclusionResolver> conclusionResolver = registry.registerConclusion(rule.conclusion());
                        applicableRules.putIfAbsent(conclusionResolver, new HashSet<>());
                        applicableRules.get(conclusionResolver).add(unifier);
                        resolverRules.put(conclusionResolver, rule);
                    } catch (GraknException e) {
                        terminate(e);
                    }
                }));
        if (!isTerminated()) isInitialised = true;
    }

    private void nextAnswer(Request fromUpstream, RequestState requestState, int iteration) {
        if (requestState.hasUpstreamAnswer()) {
            Partial.Compound<?, ?> upstreamAnswer = requestState.upstreamAnswers().next();
            requestState.recordProduced(upstreamAnswer.conceptMap());
            answerFound(upstreamAnswer, fromUpstream, iteration);
        } else {
            if (requestState.hasDownstreamProducer()) {
                requestFromDownstream(requestState.nextDownstreamProducer(), fromUpstream, iteration);
            } else {
                failToUpstream(fromUpstream, iteration);
            }
        }
    }

    private RequestState getOrUpdateRequestState(Request fromUpstream, int iteration) {
        if (!requestStates.containsKey(fromUpstream)) {
            requestStates.put(fromUpstream, requestStateCreate(fromUpstream, iteration));
        } else {
            RequestState requestState = this.requestStates.get(fromUpstream);
            assert requestState.iteration() == iteration || requestState.iteration() + 1 == iteration;

            if (requestState.iteration() + 1 == iteration) {
                // when the same request for the next iteration the first time, re-initialise required state
                RequestState newRequestState = requestStateCreate(fromUpstream, iteration);
                this.requestStates.put(fromUpstream, newRequestState);
            }
        }
        return requestStates.get(fromUpstream);
    }

    protected RequestState requestStateCreate(Request fromUpstream, int iteration) {
        LOG.debug("{}: Creating new Responses for iteration{}, request: {}", name(), iteration, fromUpstream);
        Driver<? extends Resolver<?>> root = fromUpstream.partialAnswer().root();
        recursionStates.putIfAbsent(root, new RecursionState(iteration));
        RecursionState iterationState = recursionStates.get(root);
        if (iterationState.iteration() < iteration) {
            iterationState.nextIteration(iteration);
        }

        assert fromUpstream.partialAnswer().isConcludable();
        FunctionalIterator<Partial.Compound<?, ?>> upstreamAnswers = fromUpstream.partialAnswer().asConcludable().isExplain() ?
                Iterators.empty() :
                traversalIterator(concludable.pattern(), fromUpstream.partialAnswer().conceptMap())
                        .map(conceptMap -> fromUpstream.partialAnswer().asConcludable().toUpstreamLookup(conceptMap, concludable.isInferredAnswer(conceptMap)));

        boolean singleAnswerRequired = fromUpstream.partialAnswer().conceptMap().concepts().keySet().containsAll(unboundVars());
        RequestState requestState = new RequestState(upstreamAnswers, iteration, singleAnswerRequired);
        mayRegisterRules(fromUpstream, iterationState, requestState);
        return requestState;
    }

    private void mayRegisterRules(Request fromUpstream, RecursionState recursionState, RequestState requestState) {
        // loop termination: when receiving a new request, we check if we have seen it before from this root query
        // if we have, we do not allow rules to be registered as possible downstreams
        if (!recursionState.hasReceived(fromUpstream.partialAnswer().conceptMap())) {
            Partial.Concludable<?, ?> partialAnswer = fromUpstream.partialAnswer().asConcludable();
            for (Map.Entry<Driver<ConclusionResolver>, Set<Unifier>> entry : applicableRules.entrySet()) {
                Driver<ConclusionResolver> conclusionResolver = entry.getKey();
                for (Unifier unifier : entry.getValue()) {
                    Optional<? extends Partial.Conclusion<?, ?>> unified = partialAnswer.toDownstream(unifier, resolverRules.get(conclusionResolver));
                    if (unified.isPresent()) {
                        Request toDownstream = Request.create(driver(), conclusionResolver, unified.get());
                        requestState.addDownstreamProducer(toDownstream);
                    }
                }
            }
            recursionState.recordReceived(fromUpstream.partialAnswer().conceptMap());
        }
    }

    private Set<Identifier.Variable.Retrievable> unboundVars() {
        return unboundVars;
    }

    Set<Identifier.Variable.Retrievable> unboundVars(Conjunction conjunction) {
        Set<Identifier.Variable.Retrievable> missingBounds = new HashSet<>();
        iterate(conjunction.variables()).filter(var -> var.id().isRetrievable()).forEachRemaining(var -> {
            if (var.isType() && !var.asType().label().isPresent()) missingBounds.add(var.asType().id().asRetrievable());
            else if (var.isThing() && !var.asThing().iid().isPresent())
                missingBounds.add(var.asThing().id().asRetrievable());
        });
        return missingBounds;
    }

    private static class RequestState {
        private final Set<ConceptMap> produced;
        private final FunctionalIterator<Partial.Compound<?, ?>> newUpstreamAnswers;
        private final LinkedHashSet<Request> downstreamProducer;
        private final int iteration;
        private final boolean singleAnswerRequired;
        private Iterator<Request> downstreamProducerSelector;

        public RequestState(FunctionalIterator<Partial.Compound<?, ?>> upstreamAnswers, int iteration, boolean singleAnswerRequired) {
            this(upstreamAnswers, iteration, singleAnswerRequired, new HashSet<>());
        }

        private RequestState(FunctionalIterator<Partial.Compound<?, ?>> upstreamAnswers, int iteration, boolean singleAnswerRequired,
                             Set<ConceptMap> produced) {
            this.newUpstreamAnswers = upstreamAnswers.filter(partial -> !hasProduced(partial.conceptMap()));
            this.iteration = iteration;
            this.produced = produced;
            this.singleAnswerRequired = singleAnswerRequired;
            downstreamProducer = new LinkedHashSet<>();
            downstreamProducerSelector = downstreamProducer.iterator();
        }

        public void recordProduced(ConceptMap conceptMap) {
            produced.add(conceptMap);
        }

        public boolean hasProduced(ConceptMap conceptMap) {
            return produced.contains(conceptMap);
        }

        public boolean hasUpstreamAnswer() {
            return newUpstreamAnswers.hasNext();
        }

        public FunctionalIterator<Partial.Compound<?, ?>> upstreamAnswers() {
            return newUpstreamAnswers;
        }

        public boolean hasDownstreamProducer() {
            return !downstreamProducer.isEmpty();
        }

        public Request nextDownstreamProducer() {
            if (!downstreamProducerSelector.hasNext()) downstreamProducerSelector = downstreamProducer.iterator();
            return downstreamProducerSelector.next();
        }

        public void addDownstreamProducer(Request request) {
            assert !(downstreamProducer.contains(request)) : "downstream answer producer already contains this request";

            downstreamProducer.add(request);
            downstreamProducerSelector = downstreamProducer.iterator();
        }

        public void removeDownstreamProducer(Request request) {
            boolean removed = downstreamProducer.remove(request);
            // only update the iterator when removing an element, to avoid resetting and reusing first request too often
            // note: this is a large performance win when processing large batches of requests
            if (removed) downstreamProducerSelector = downstreamProducer.iterator();
        }

        public void clearDownstreamProducers() {
            downstreamProducer.clear();
            downstreamProducerSelector = Iterators.empty();
        }

        public boolean singleAnswerRequired() {
            return singleAnswerRequired;
        }

        public int iteration() {
            return iteration;
        }
    }

    /**
     * Maintain iteration state per root query
     * This allows us to share resolvers across different queries
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

