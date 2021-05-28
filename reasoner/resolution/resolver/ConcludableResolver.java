/*
 * Copyright (C) 2021 Vaticle
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

package com.vaticle.typedb.core.reasoner.resolution.resolver;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.iterator.Iterators;
import com.vaticle.typedb.core.concept.ConceptManager;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.logic.LogicManager;
import com.vaticle.typedb.core.logic.Rule;
import com.vaticle.typedb.core.logic.resolvable.Unifier;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.reasoner.resolution.ResolverRegistry;
import com.vaticle.typedb.core.reasoner.resolution.answer.AnswerState.Partial;
import com.vaticle.typedb.core.reasoner.resolution.framework.AnswerCache;
import com.vaticle.typedb.core.reasoner.resolution.framework.AnswerCache.ConcludableAnswerCache;
import com.vaticle.typedb.core.reasoner.resolution.framework.AnswerCache.SubsumptionAnswerCache.ConceptMapCache;
import com.vaticle.typedb.core.reasoner.resolution.framework.ReiterationQuery;
import com.vaticle.typedb.core.reasoner.resolution.framework.Request;
import com.vaticle.typedb.core.reasoner.resolution.framework.RequestState.CachingRequestState;
import com.vaticle.typedb.core.reasoner.resolution.framework.RequestState.Exploration;
import com.vaticle.typedb.core.reasoner.resolution.framework.Resolver;
import com.vaticle.typedb.core.reasoner.resolution.framework.Response;
import com.vaticle.typedb.core.reasoner.resolution.framework.Response.Answer;
import com.vaticle.typedb.core.traversal.TraversalEngine;
import com.vaticle.typedb.core.traversal.common.Identifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public class ConcludableResolver extends Resolver<ConcludableResolver> {

    private static final Logger LOG = LoggerFactory.getLogger(ConcludableResolver.class);

    private final LinkedHashMap<Driver<ConclusionResolver>, Set<Unifier>> applicableRules;
    private final Map<Driver<ConclusionResolver>, Rule> resolverRules;
    private final com.vaticle.typedb.core.logic.resolvable.Concludable concludable;
    private final LogicManager logicMgr;
    private final Map<Request, CachingRequestState<?, ConceptMap>> requestStates;
    private final Set<Identifier.Variable.Retrievable> unboundVars;
    private boolean isInitialised;
    protected final Map<Driver<? extends Resolver<?>>, Map<ConceptMap, AnswerCache<?, ConceptMap>>> cacheRegistersByRoot;
    protected final Map<Driver<? extends Resolver<?>>, Integer> iterationByRoot;

    public ConcludableResolver(Driver<ConcludableResolver> driver, com.vaticle.typedb.core.logic.resolvable.Concludable concludable,
                               ResolverRegistry registry, TraversalEngine traversalEngine, ConceptManager conceptMgr,
                               LogicManager logicMgr, boolean resolutionTracing) {
        super(driver, ConcludableResolver.class.getSimpleName() + "(pattern: " + concludable.pattern() + ")",
              registry, traversalEngine, conceptMgr, resolutionTracing);
        this.logicMgr = logicMgr;
        this.concludable = concludable;
        this.applicableRules = new LinkedHashMap<>();
        this.resolverRules = new HashMap<>();
        this.requestStates = new HashMap<>();
        this.unboundVars = unboundVars(concludable.pattern());
        this.isInitialised = false;
        this.cacheRegistersByRoot = new HashMap<>();
        this.iterationByRoot = new HashMap<>();
    }

    @Override
    public void receiveRequest(Request fromUpstream, int iteration) {
        LOG.trace("{}: received Request: {}", name(), fromUpstream);
        if (!isInitialised) initialiseDownstreamResolvers();
        if (isTerminated()) return;

        Driver<? extends Resolver<?>> root = fromUpstream.partialAnswer().root();
        iterationByRoot.putIfAbsent(root, iteration);
        if (iteration > iterationByRoot.get(root)) prepareNextIteration(root, iteration);

        if (iteration < iterationByRoot.get(root)) {
            // short circuit if the request came from a prior iteration
            failToUpstream(fromUpstream, iteration);
        } else {
            CachingRequestState<?, ConceptMap> requestState = getOrReplaceRequestState(fromUpstream, iteration);
            assert iteration == requestState.iteration();
            assert cacheRegistersByRoot.get(root).containsKey(fromUpstream.partialAnswer().conceptMap());
            nextAnswer(fromUpstream, requestState, iteration);
        }
    }

    private void prepareNextIteration(Driver<? extends Resolver<?>> root, int iteration) {
        iterationByRoot.put(root, iteration);
        cacheRegistersByRoot.get(root).clear();
    }

    @Override
    protected void receiveAnswer(Answer fromDownstream, int iteration) {
        LOG.trace("{}: received Answer: {}", name(), fromDownstream);
        if (isTerminated()) return;

        Request toDownstream = fromDownstream.sourceRequest();
        Request fromUpstream = fromUpstream(toDownstream);
        Driver<? extends Resolver<?>> root = fromUpstream.partialAnswer().root();

        if (iteration < iterationByRoot.get(root)) {
            // short circuit if the request came from a prior iteration
            failToUpstream(fromUpstream, iteration);
        } else {
            CachingRequestState<?, ConceptMap> requestState = this.requestStates.get(fromUpstream);
            assert requestState.isExploration();
            requestState.asExploration().newAnswer(fromDownstream.answer());
            assert iteration == requestState.iteration();
            assert cacheRegistersByRoot.get(root).containsKey(fromUpstream.partialAnswer().conceptMap());
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
        CachingRequestState<?, ConceptMap> requestState = this.requestStates.get(fromUpstream);
        if (requestState.isExploration() && requestState.asExploration().singleAnswerRequired()) {
            requestState.asExploration().downstreamManager().clearDownstreams();
            requestState.answerCache().setComplete();
        }
        answerToUpstream(upstreamAnswer, fromUpstream, iteration);
    }

    @Override
    protected void receiveFail(Response.Fail fromDownstream, int iteration) {
        LOG.trace("{}: received Fail: {}", name(), fromDownstream);
        if (isTerminated()) return;

        Request toDownstream = fromDownstream.sourceRequest();
        Request fromUpstream = fromUpstream(toDownstream);

        if (iteration < iterationByRoot.get(fromUpstream.partialAnswer().root())) {
            // short circuit old iteration failed messages to upstream
            failToUpstream(fromUpstream, iteration);
        } else {
            CachingRequestState<?, ConceptMap> requestState = this.requestStates.get(fromUpstream);
            assert iteration == requestState.iteration();
            if (requestState.isExploration()) {
                requestState.asExploration().downstreamManager().removeDownstream(fromDownstream.sourceRequest());
            }
            nextAnswer(fromUpstream, requestState, iteration);
        }
    }

    public void receiveReiterationQuery(ReiterationQuery.Request request) {
        boolean reiterate = cacheRegistersByRoot.containsKey(request.sender()) &&
                iterate(cacheRegistersByRoot.get(request.sender()).values())
                    .filter(AnswerCache::requiresReiteration).first().isPresent();
        driver().execute(actor -> actor.respondToReiterationQuery(request, reiterate));
    }

    private void respondToReiterationQuery(ReiterationQuery.Request request, boolean reiterate) {
        request.onResponse().accept(ReiterationQuery.Response.create(driver(), reiterate));
    }

    @Override
    public void terminate(Throwable cause) {
        super.terminate(cause);
        requestStates.clear();
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
                    } catch (TypeDBException e) {
                        terminate(e);
                    }
                }));
        if (!isTerminated()) isInitialised = true;
    }

    private void nextAnswer(Request fromUpstream, CachingRequestState<?, ConceptMap> requestState, int iteration) {
        Optional<Partial.Compound<?, ?>> upstreamAnswer = requestState.nextAnswer().map(Partial::asCompound);
        if (upstreamAnswer.isPresent()) {
            answerFound(upstreamAnswer.get(), fromUpstream, iteration);
        } else {
            Exploration exploration;
            if (requestState.isExploration() && !requestState.answerCache().isComplete()) {
                if ((exploration = requestState.asExploration()).downstreamManager().hasDownstream()) {
                    requestFromDownstream(exploration.downstreamManager().nextDownstream(), fromUpstream, iteration);
                } else {
                    requestState.answerCache().setComplete(); // TODO: The cache should not be set as complete during recursion
                    failToUpstream(fromUpstream, iteration);
                }
            } else {
                failToUpstream(fromUpstream, iteration);
            }
        }
    }

    private CachingRequestState<?, ConceptMap> getOrReplaceRequestState(Request fromUpstream, int iteration) {
        if (!requestStates.containsKey(fromUpstream)) {
            CachingRequestState<?, ConceptMap> requestState = createRequestState(fromUpstream, iteration);
            requestStates.put(fromUpstream, requestState);
        } else {
            CachingRequestState<?, ConceptMap> requestState = this.requestStates.get(fromUpstream);

            if (requestState.iteration() < iteration) {
                // when the same request for the next iteration the first time, re-initialise required state
                CachingRequestState<?, ConceptMap> newRequestState = createRequestState(fromUpstream, iteration);
                this.requestStates.put(fromUpstream, newRequestState);
            }
        }
        return requestStates.get(fromUpstream);
    }

    protected CachingRequestState<?, ConceptMap> createRequestState(Request fromUpstream, int iteration) {
        LOG.debug("{}: Creating new Responses for iteration{}, request: {}", name(), iteration, fromUpstream);
        Driver<? extends Resolver<?>> root = fromUpstream.partialAnswer().root();
        cacheRegistersByRoot.putIfAbsent(root, new HashMap<>());
        Map<ConceptMap, AnswerCache<?, ConceptMap>> cacheRegister = cacheRegistersByRoot.get(root);
        ConceptMap answerFromUpstream = fromUpstream.partialAnswer().conceptMap();
        CachingRequestState<?, ConceptMap> requestState;
        assert fromUpstream.partialAnswer().isConcludable();
        if (fromUpstream.partialAnswer().asConcludable().isExplain()) {
            if (cacheRegister.containsKey(answerFromUpstream)) {
                if (cacheRegister.get(answerFromUpstream).isConceptMapCache()) {
                    // We have a cache already which we must evict to use a cache suitable for explaining
                    ConcludableAnswerCache answerCache = new ConcludableAnswerCache(cacheRegister, answerFromUpstream);
                    cacheRegister.put(answerFromUpstream, answerCache);
                    requestState = new Explain(fromUpstream, answerCache, iteration);
                    registerRules(fromUpstream, requestState.asExploration());
                } else if (cacheRegister.get(answerFromUpstream).isConcludableAnswerCache()) {
                    ConcludableAnswerCache answerCache = cacheRegister.get(answerFromUpstream).asConcludableAnswerCache();
                    requestState = new FollowingExplain(fromUpstream, answerCache, iteration);
                } else {
                    throw TypeDBException.of(ILLEGAL_STATE);
                }
            } else {
                ConcludableAnswerCache answerCache = new ConcludableAnswerCache(cacheRegister, answerFromUpstream);
                cacheRegister.put(answerFromUpstream, answerCache);
                requestState = new Explain(fromUpstream, answerCache, iteration);
                registerRules(fromUpstream, requestState.asExploration());
            }
        } else if (fromUpstream.partialAnswer().asConcludable().isMatch()) {
            if (cacheRegister.containsKey(answerFromUpstream)) {
                if (cacheRegister.get(answerFromUpstream).isConceptMapCache()) {
                    ConceptMapCache answerCache = cacheRegister.get(answerFromUpstream).asConceptMapCache();
                    requestState = new FollowingMatch(fromUpstream, answerCache, iteration);
                } else if (cacheRegister.get(answerFromUpstream).isConcludableAnswerCache()) {
                    ConcludableAnswerCache answerCache = cacheRegister.get(answerFromUpstream).asConcludableAnswerCache();
                    requestState = new FollowingExplain(fromUpstream, answerCache, iteration);
                } else {
                    throw TypeDBException.of(ILLEGAL_STATE);
                }
            } else {
                ConceptMapCache answerCache = new ConceptMapCache(cacheRegister, answerFromUpstream);
                cacheRegister.put(answerFromUpstream, answerCache);
                if (!answerCache.completeIfSubsumerComplete()) {
                    answerCache.addSource(traversalIterator(concludable.pattern(), answerFromUpstream));
                }
                boolean singleAnswerRequired = answerFromUpstream.concepts().keySet().containsAll(unboundVars);
                requestState = new Match(fromUpstream, answerCache, iteration, singleAnswerRequired);
                registerRules(fromUpstream, requestState.asExploration());
            }
        } else {
            throw TypeDBException.of(ILLEGAL_STATE);
        }
        return requestState;
    }

    private void registerRules(Request fromUpstream, Exploration exploration) {
        // loop termination: when receiving a new request, we check if we have seen it before from this root query
        // if we have, we do not allow rules to be registered as possible downstreams
        Partial.Concludable<?> partialAnswer = fromUpstream.partialAnswer().asConcludable();
        for (Map.Entry<Driver<ConclusionResolver>, Set<Unifier>> entry : applicableRules.entrySet()) {
            Driver<ConclusionResolver> conclusionResolver = entry.getKey();
            for (Unifier unifier : entry.getValue()) {
                Optional<? extends Partial.Conclusion<?, ?>> unified = partialAnswer.toDownstream(
                        unifier, resolverRules.get(conclusionResolver));
                if (unified.isPresent()) {
                    Request toDownstream = Request.create(driver(), conclusionResolver, unified.get());
                    exploration.downstreamManager().addDownstream(toDownstream);
                }
            }
        }
    }

    private static Set<Identifier.Variable.Retrievable> unboundVars(Conjunction conjunction) {
        Set<Identifier.Variable.Retrievable> missingBounds = new HashSet<>();
        iterate(conjunction.variables()).filter(var -> var.id().isRetrievable()).forEachRemaining(var -> {
            if (var.isType() && !var.asType().label().isPresent()) missingBounds.add(var.asType().id().asRetrievable());
            else if (var.isThing() && !var.asThing().iid().isPresent())
                missingBounds.add(var.asThing().id().asRetrievable());
        });
        return missingBounds;
    }

    private class FollowingMatch extends CachingRequestState<ConceptMap, ConceptMap> {

        public FollowingMatch(Request fromUpstream, AnswerCache<ConceptMap, ConceptMap> answerCache, int iteration) {
            super(fromUpstream, answerCache, iteration, true, true);
        }

        @Override
        protected FunctionalIterator<? extends Partial<?>> toUpstream(ConceptMap conceptMap) {
            return Iterators.single(fromUpstream.partialAnswer().asConcludable().asMatch().toUpstreamLookup(
                    conceptMap, concludable.isInferredAnswer(conceptMap)));
        }

    }

    private class Match extends CachingRequestState<ConceptMap, ConceptMap> implements Exploration {

        private final DownstreamManager downstreamManager;
        private final boolean singleAnswerRequired;

        public Match(Request fromUpstream, AnswerCache<ConceptMap, ConceptMap> answerCache,
                     int iteration, boolean singleAnswerRequired) {
            super(fromUpstream, answerCache, iteration, true, false);
            this.downstreamManager = new DownstreamManager();
            this.singleAnswerRequired = singleAnswerRequired;
        }

        @Override
        public boolean isExploration() {
            return true;
        }

        @Override
        public Exploration asExploration() {
            return this;
        }

        @Override
        public DownstreamManager downstreamManager() {
            return downstreamManager;
        }

        @Override
        public void newAnswer(Partial<?> partial) {
            if (!answerCache.isComplete()) answerCache.add(partial.conceptMap());
        }

        @Override
        public boolean singleAnswerRequired() {
            return singleAnswerRequired;
        }

        @Override
        protected FunctionalIterator<? extends Partial<?>> toUpstream(ConceptMap conceptMap) {
            return Iterators.single(fromUpstream.partialAnswer().asConcludable().asMatch().toUpstreamLookup(
                    conceptMap, concludable.isInferredAnswer(conceptMap)));
        }
    }

    private static class FollowingExplain extends CachingRequestState<Partial.Concludable<?>, ConceptMap> {

        public FollowingExplain(Request fromUpstream, AnswerCache<Partial.Concludable<?>, ConceptMap> answerCache,
                                int iteration) {
            super(fromUpstream, answerCache, iteration, true, true);
        }

        @Override
        protected FunctionalIterator<? extends Partial<?>> toUpstream(Partial.Concludable<?> partial) {
            return Iterators.single(partial.asExplain().toUpstreamInferred());
        }
    }

    private static class Explain extends CachingRequestState<Partial.Concludable<?>, ConceptMap> implements Exploration {

        private final DownstreamManager downstreamManager;

        public Explain(Request fromUpstream, AnswerCache<Partial.Concludable<?>, ConceptMap> answerCache,
                       int iteration) {
            super(fromUpstream, answerCache, iteration, false, false);
            this.downstreamManager = new DownstreamManager();
        }

        @Override
        public boolean isExploration() {
            return true;
        }

        @Override
        public Exploration asExploration() {
            return this;
        }

        @Override
        public DownstreamManager downstreamManager() {
            return downstreamManager;
        }

        @Override
        public void newAnswer(Partial<?> partial) {
            if (!answerCache.isComplete()) answerCache.add(partial.asConcludable());
        }

        @Override
        public boolean singleAnswerRequired() {
            return false;
        }

        @Override
        protected FunctionalIterator<? extends Partial<?>> toUpstream(Partial.Concludable<?> partial) {
            return Iterators.single(partial.asExplain().toUpstreamInferred());
        }

    }

}
