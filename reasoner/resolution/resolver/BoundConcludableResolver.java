package com.vaticle.typedb.core.reasoner.resolution.resolver;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.iterator.Iterators;
import com.vaticle.typedb.core.concept.ConceptManager;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.logic.Rule;
import com.vaticle.typedb.core.logic.resolvable.Concludable;
import com.vaticle.typedb.core.logic.resolvable.Unifier;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.reasoner.resolution.ResolverRegistry;
import com.vaticle.typedb.core.reasoner.resolution.answer.AnswerState;
import com.vaticle.typedb.core.reasoner.resolution.framework.AnswerCache;
import com.vaticle.typedb.core.reasoner.resolution.framework.AnswerCache.SubsumptionAnswerCache.ConceptMapCache;
import com.vaticle.typedb.core.reasoner.resolution.framework.ReiterationQuery;
import com.vaticle.typedb.core.reasoner.resolution.framework.Request;
import com.vaticle.typedb.core.reasoner.resolution.framework.RequestState;
import com.vaticle.typedb.core.reasoner.resolution.framework.Resolver;
import com.vaticle.typedb.core.reasoner.resolution.framework.Response;
import com.vaticle.typedb.core.traversal.TraversalEngine;
import com.vaticle.typedb.core.traversal.common.Identifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public class BoundConcludableResolver extends Resolver<BoundConcludableResolver>  {

    private static final Logger LOG = LoggerFactory.getLogger(BoundConcludableResolver.class);
    private AnswerCache<?, ConceptMap> cache;
    private final Concludable concludable;
    private final Set<Identifier.Variable.Retrievable> unboundVars;
    private final ConceptMap bounds;
    private final Map<Driver<ConclusionResolver>, Rule> resolverRules;
    private final LinkedHashMap<Driver<ConclusionResolver>, Set<Unifier>> applicableRules;
    private final Map<Request, RequestState.CachingRequestState<?, ConceptMap>> requestStates;
    private RequestState exploringRequestState;

    public BoundConcludableResolver(Driver<BoundConcludableResolver> driver, Concludable concludable, ConceptMap bounds,
                                    Map<Driver<ConclusionResolver>, Rule> resolverRules,
                                    LinkedHashMap<Driver<ConclusionResolver>, Set<Unifier>> applicableRules,
                                    ResolverRegistry registry, TraversalEngine traversalEngine,
                                    ConceptManager conceptMgr, boolean resolutionTracing) {
        super(driver, initName(concludable, bounds), registry, traversalEngine, conceptMgr, resolutionTracing);
        this.concludable = concludable;
        this.unboundVars = unboundVars(concludable.pattern());
        this.bounds = bounds;
        this.resolverRules = resolverRules;
        this.applicableRules = applicableRules;
        this.cache = new ConceptMapCache(new HashMap<>(), bounds, () -> traversalIterator(concludable.pattern(), bounds));
        this.requestStates = new HashMap<>();
        this.exploringRequestState = null;
    }

    private static String initName(Concludable concludable, ConceptMap bounds) {
        return BoundConcludableResolver.class.getSimpleName() + "(pattern: " + concludable.pattern() + " bounds: " +
                bounds.toString() + ")";
    }

    @Override
    public void receiveRequest(Request fromUpstream, int iteration) {
        LOG.trace("{}: received Request: {}", name(), fromUpstream);
        if (isTerminated()) return;
        if (fromUpstream.isToSubsumed()) {
            assert fromUpstream.partialAnswer().conceptMap().equals(bounds);
            receiveSubsumedRequest(fromUpstream.asToSubsumed(), iteration);
        } else if (fromUpstream.isToSubsumer()) {
            receiveSubsumerRequest(fromUpstream.asToSubsumer(), iteration);
        } else {
            assert fromUpstream.partialAnswer().conceptMap().equals(bounds);
            receiveDirectRequest(fromUpstream, iteration);
        }
    }

    private void receiveSubsumedRequest(Request.ToSubsumed fromUpstream, int iteration) {
        throw TypeDBException.of(ILLEGAL_STATE);
//        RequestState requestState = requestStates.computeIfAbsent(
//                fromUpstream, request -> new BoundRetrievableResolver.BoundRequestState(request, cache, iteration));
//        if (cache.sourceExhausted()) {
//            sendAnswerOrFail(fromUpstream, iteration, requestState);
//        } else {
//            cache.clearSource();
//            Optional<? extends AnswerState.Partial<?>> upstreamAnswer;
//            upstreamAnswer = requestState.nextAnswer();
//            if (upstreamAnswer.isPresent()) {
//                answerToUpstream(upstreamAnswer.get(), fromUpstream, iteration);
//            } else {
//                requestFromSubsumer(fromUpstream, iteration);
//            }
//        }
    }

    private void receiveSubsumerRequest(Request.ToSubsumer fromUpstream, int iteration) {
        throw TypeDBException.of(ILLEGAL_STATE);
//        sendAnswerOrFail(fromUpstream, iteration, requestStates.computeIfAbsent(
//                fromUpstream, request -> new BoundRetrievableResolver.SubsumerRequestState(request, cache, iteration)));
    }

    private void receiveDirectRequest(Request fromUpstream, int iteration) {
//        sendAnswerOrFail(fromUpstream, iteration, requestStates.computeIfAbsent(
//                fromUpstream, request -> new BoundRetrievableResolver.BoundRequestState(request, cache, iteration)));
        sendAnswerOrSearchRulesOrFail(fromUpstream, iteration, requestStates.computeIfAbsent(
                fromUpstream, request -> createRequestState(fromUpstream, iteration)));
    }

    @Override
    protected void receiveAnswer(Response.Answer fromDownstream, int iteration) {
        if (isTerminated()) return;
        Request fromUpstream = fromUpstream(fromDownstream.sourceRequest());
        RequestState.CachingRequestState<?, ConceptMap> requestState = this.requestStates.get(fromUpstream);
        assert requestState.isExploration();
        requestState.asExploration().newAnswer(fromDownstream.answer());
        sendAnswerOrSearchRulesOrFail(fromUpstream, iteration, requestState);
    }

    @Override
    protected void receiveFail(Response.Fail fromDownstream, int iteration) {
        LOG.trace("{}: received Fail: {}", name(), fromDownstream);
        if (isTerminated()) return;

        Request toDownstream = fromDownstream.sourceRequest();
        Request fromUpstream = fromUpstream(toDownstream);

        RequestState.CachingRequestState<?, ConceptMap> requestState = this.requestStates.get(fromUpstream);
        assert iteration == requestState.iteration();
        if (requestState.isExploration()) {
            requestState.asExploration().downstreamManager().removeDownstream(fromDownstream.sourceRequest());
        }
        sendAnswerOrSearchRulesOrFail(fromUpstream, iteration, requestState);
    }

    private void sendAnswerOrSearchRulesOrFail(Request fromUpstream, int iteration, RequestState.CachingRequestState<?, ConceptMap> requestState) {
        Optional<AnswerState.Partial.Compound<?, ?>> upstreamAnswer = requestState.nextAnswer().map(AnswerState.Partial::asCompound);
        if (upstreamAnswer.isPresent()) {
            /*
            When we only require 1 answer (eg. when the conjunction is already fully bound), we can short circuit
            and prevent exploration of further rules.

            One latency optimisation we could do here, is keep track of how many N repeated requests are received,
            forward them downstream (to parallelise searching for the single answer), and when the first one finds an answer,
            we respond for all N ahead of time. Then, when the rules actually return an answer to this concludable, we do nothing.
             */
            RequestState.CachingRequestState<?, ConceptMap> requestState1 = this.requestStates.get(fromUpstream);
            if (requestState1.isExploration() && requestState1.asExploration().singleAnswerRequired()) {
                requestState1.asExploration().downstreamManager().clearDownstreams();
                requestState1.answerCache().setComplete();
            }
            answerToUpstream(upstreamAnswer.get(), fromUpstream, iteration);
        } else {
            RequestState.Exploration exploration;
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


    private void requestFromSubsumer(Request.ToSubsumed fromUpstream, int iteration) {
        Request toSubsumer = Request.ToSubsumer.create(driver(), fromUpstream.subsumer(),
                                                       fromUpstream, fromUpstream.partialAnswer());
        requestFromDownstream(toSubsumer, fromUpstream, iteration);
    }

    @Override
    protected void initialiseDownstreamResolvers() {
        throw TypeDBException.of(ILLEGAL_STATE);
    }

    public void receiveReiterationQuery(ReiterationQuery.Request request) {
        driver().execute(actor -> actor.respondToReiterationQuery(request, cache.requiresReiteration()));
    }

    private void respondToReiterationQuery(ReiterationQuery.Request request, boolean reiterate) {
        request.onResponse().accept(ReiterationQuery.Response.create(driver(), reiterate));
    }

    @Override
    public void terminate(Throwable cause) {
        super.terminate(cause);
        requestStates.clear();
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

    protected RequestState.CachingRequestState<?, ConceptMap> createRequestState(Request fromUpstream, int iteration) {
        LOG.debug("{}: Creating new Responses for iteration{}, request: {}", name(), iteration, fromUpstream);
        assert fromUpstream.partialAnswer().isConcludable();
        RequestState.CachingRequestState<?, ConceptMap> requestState;
        if (fromUpstream.partialAnswer().asConcludable().isExplain()) {
            requestState = createExplainRequestState(fromUpstream, iteration);
        } else if (fromUpstream.partialAnswer().asConcludable().isMatch()) {
            requestState = createMatchRequestState(fromUpstream, iteration);
        } else {
            throw TypeDBException.of(ILLEGAL_STATE);
        }
        if (exploringRequestState == null) {
            assert requestState.isExploration();
            exploringRequestState = requestState;
        }
        return requestState;
    }

    protected RequestState.CachingRequestState<?, ConceptMap> createExplainRequestState(Request fromUpstream, int iteration) {
        RequestState.CachingRequestState<?, ConceptMap> requestState;
        if (exploringRequestState != null) {
            if (cache.isConceptMapCache()) {
                // We have a cache already which we must evict to use a cache suitable for explaining
                cache = new AnswerCache.ConcludableAnswerCache(new HashMap<>(), bounds);
                requestState = new Explain(fromUpstream, cache.asConcludableAnswerCache(), iteration);
                requestState.asExploration().downstreamManager().addDownstreams(ruleDownstreams(fromUpstream));
            } else if (cache.isConcludableAnswerCache()) {
                requestState = new FollowingExplain(fromUpstream, cache.asConcludableAnswerCache(), iteration);
            } else {
                throw TypeDBException.of(ILLEGAL_STATE);
            }
        } else {
            cache = new AnswerCache.ConcludableAnswerCache(new HashMap<>(), bounds);
            requestState = new Explain(fromUpstream, cache.asConcludableAnswerCache(), iteration);
            requestState.asExploration().downstreamManager().addDownstreams(ruleDownstreams(fromUpstream));
        }
        return requestState;
    }

    protected RequestState.CachingRequestState<?, ConceptMap> createMatchRequestState(Request fromUpstream, int iteration) {
        RequestState.CachingRequestState<?, ConceptMap> requestState;
        if (exploringRequestState != null) {
            if (cache.isConceptMapCache()) {
                requestState = new FollowingMatch(fromUpstream, cache.asConceptMapCache(), iteration);
            } else if (cache.isConcludableAnswerCache()) {
                requestState = new FollowingExplain(fromUpstream, cache.asConcludableAnswerCache(), iteration);
            } else {
                throw TypeDBException.of(ILLEGAL_STATE);
            }
        } else {
            cache = new ConceptMapCache(new HashMap<>(), bounds, () -> traversalIterator(concludable.pattern(), bounds));
            boolean singleAnswerRequired = bounds.concepts().keySet().containsAll(unboundVars);
            requestState = new Match(fromUpstream, cache.asConceptMapCache(), iteration, singleAnswerRequired);
            requestState.asExploration().downstreamManager().addDownstreams(ruleDownstreams(fromUpstream));
        }
        return requestState;
    }

    private List<Request> ruleDownstreams(Request fromUpstream) {
        // loop termination: when receiving a new request, we check if we have seen it before from this root query
        // if we have, we do not allow rules to be registered as possible downstreams
        List<Request> downstreams = new ArrayList<>();
        AnswerState.Partial.Concludable<?> partialAnswer = fromUpstream.partialAnswer().asConcludable();
        for (Map.Entry<Driver<ConclusionResolver>, Set<Unifier>> entry : applicableRules.entrySet()) {
            Driver<ConclusionResolver> conclusionResolver = entry.getKey();
            for (Unifier unifier : entry.getValue()) {
                Optional<? extends AnswerState.Partial.Conclusion<?, ?>> unified = partialAnswer.toDownstream(
                        unifier, resolverRules.get(conclusionResolver));
                unified.ifPresent(
                        conclusion -> downstreams.add(Request.create(driver(), conclusionResolver, conclusion)));
            }
        }
        return downstreams;
    }

    private class FollowingMatch extends RequestState.CachingRequestState<ConceptMap, ConceptMap> {

        public FollowingMatch(Request fromUpstream, AnswerCache<ConceptMap, ConceptMap> answerCache, int iteration) {
            super(fromUpstream, answerCache, iteration, true, true);
        }

        @Override
        protected FunctionalIterator<? extends AnswerState.Partial<?>> toUpstream(ConceptMap conceptMap) {
            return Iterators.single(fromUpstream.partialAnswer().asConcludable().asMatch().toUpstreamLookup(
                    conceptMap, concludable.isInferredAnswer(conceptMap)));
        }

    }

    private class Match extends RequestState.CachingRequestState<ConceptMap, ConceptMap> implements RequestState.Exploration {

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
        public void newAnswer(AnswerState.Partial<?> partial) {
            if (!answerCache.isComplete()) answerCache.add(partial.conceptMap());
        }

        @Override
        public boolean singleAnswerRequired() {
            return singleAnswerRequired;
        }

        @Override
        protected FunctionalIterator<? extends AnswerState.Partial<?>> toUpstream(ConceptMap conceptMap) {
            return Iterators.single(fromUpstream.partialAnswer().asConcludable().asMatch().toUpstreamLookup(
                    conceptMap, concludable.isInferredAnswer(conceptMap)));
        }
    }

    private static class FollowingExplain extends RequestState.CachingRequestState<AnswerState.Partial.Concludable<?>, ConceptMap> {

        public FollowingExplain(Request fromUpstream, AnswerCache<AnswerState.Partial.Concludable<?>, ConceptMap> answerCache,
                                int iteration) {
            super(fromUpstream, answerCache, iteration, true, true);
        }

        @Override
        protected FunctionalIterator<? extends AnswerState.Partial<?>> toUpstream(AnswerState.Partial.Concludable<?> partial) {
            return Iterators.single(partial.asExplain().toUpstreamInferred());
        }
    }

    private static class Explain extends RequestState.CachingRequestState<AnswerState.Partial.Concludable<?>, ConceptMap> implements RequestState.Exploration {

        private final DownstreamManager downstreamManager;

        public Explain(Request fromUpstream, AnswerCache<AnswerState.Partial.Concludable<?>, ConceptMap> answerCache,
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
        public void newAnswer(AnswerState.Partial<?> partial) {
            if (!answerCache.isComplete()) answerCache.add(partial.asConcludable());
        }

        @Override
        public boolean singleAnswerRequired() {
            return false;
        }

        @Override
        protected FunctionalIterator<? extends AnswerState.Partial<?>> toUpstream(AnswerState.Partial.Concludable<?> partial) {
            return Iterators.single(partial.asExplain().toUpstreamInferred());
        }

    }
}
