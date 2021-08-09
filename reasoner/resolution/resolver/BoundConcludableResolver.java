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
import com.vaticle.typedb.core.concept.ConceptManager;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.logic.Rule;
import com.vaticle.typedb.core.logic.resolvable.Concludable;
import com.vaticle.typedb.core.logic.resolvable.Unifier;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.reasoner.resolution.ResolverRegistry;
import com.vaticle.typedb.core.reasoner.resolution.answer.AnswerState;
import com.vaticle.typedb.core.reasoner.resolution.framework.AnswerCache;
import com.vaticle.typedb.core.reasoner.resolution.framework.ReiterationQuery;
import com.vaticle.typedb.core.reasoner.resolution.framework.Request;
import com.vaticle.typedb.core.reasoner.resolution.framework.RequestState;
import com.vaticle.typedb.core.reasoner.resolution.framework.RequestState.CachingRequestState;
import com.vaticle.typedb.core.reasoner.resolution.framework.Resolver;
import com.vaticle.typedb.core.reasoner.resolution.framework.Response;
import com.vaticle.typedb.core.traversal.TraversalEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;

public abstract class BoundConcludableResolver extends Resolver<BoundConcludableResolver>  {

    private static final Logger LOG = LoggerFactory.getLogger(BoundConcludableResolver.class);

    private final Map<Request, CachingRequestState<?, ConceptMap>> requestStates;
    protected final Concludable concludable;
    protected final ConceptMap bounds;

    protected AnswerCache<?, ConceptMap> cache;
    protected CachingRequestState<?, ConceptMap> exploringRequestState;

    public BoundConcludableResolver(Driver<BoundConcludableResolver> driver, Concludable concludable, ConceptMap bounds,
                                    ResolverRegistry registry, TraversalEngine traversalEngine,
                                    ConceptManager conceptMgr, boolean resolutionTracing) {
        super(driver, BoundConcludableResolver.class.getSimpleName() + "(pattern: " + concludable.pattern() +
                " bounds: " + bounds.toString() + ")", registry, traversalEngine, conceptMgr, resolutionTracing);
        this.concludable = concludable;
        this.bounds = bounds;
        this.cache = createCache(concludable.pattern(), bounds);
        this.requestStates = new HashMap<>();
        this.exploringRequestState = null;
    }

    abstract AnswerCache<?, ConceptMap> createCache(Conjunction conjunction, ConceptMap bounds);

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
        assert fromUpstream.partialAnswer().isConcludable();
        CachingRequestState<?, ConceptMap> requestState;
        if (exploringRequestState == null) {
            requestState = createExploringRequestState(fromUpstream, iteration);
            exploringRequestState = requestState;
            assert requestStates.get(fromUpstream) == null;
            requestStates.put(fromUpstream, requestState);
        } else {
            requestState = requestStates.computeIfAbsent(
                    fromUpstream, request -> createRequestState(fromUpstream, iteration));
        }
        sendAnswerOrSearchRulesOrFail(fromUpstream, iteration, requestState);
    }

    abstract CachingRequestState<?, ConceptMap> createRequestState(Request fromUpstream, int iteration);

    abstract CachingRequestState<?, ConceptMap> createExploringRequestState(Request fromUpstream, int iteration);

    @Override
    protected void receiveAnswer(Response.Answer fromDownstream, int iteration) {
        if (isTerminated()) return;
        Request fromUpstream = fromUpstream(fromDownstream.sourceRequest());
        CachingRequestState<?, ConceptMap> requestState = this.requestStates.get(fromUpstream);
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

        CachingRequestState<?, ConceptMap> requestState = this.requestStates.get(fromUpstream);
        assert iteration == requestState.iteration();
        if (requestState.isExploration()) {
            requestState.asExploration().downstreamManager().removeDownstream(fromDownstream.sourceRequest());
        }
        sendAnswerOrSearchRulesOrFail(fromUpstream, iteration, requestState);
    }

    private void sendAnswerOrSearchRulesOrFail(Request fromUpstream, int iteration, CachingRequestState<?, ConceptMap> requestState) {
        Optional<AnswerState.Partial.Compound<?, ?>> upstreamAnswer = requestState.nextAnswer().map(AnswerState.Partial::asCompound);
        if (upstreamAnswer.isPresent()) {
            /*
            When we only require 1 answer (eg. when the conjunction is already fully bound), we can short circuit
            and prevent exploration of further rules.

            One latency optimisation we could do here, is keep track of how many N repeated requests are received,
            forward them downstream (to parallelise searching for the single answer), and when the first one finds an answer,
            we respond for all N ahead of time. Then, when the rules actually return an answer to this concludable, we do nothing.
             */
            if (requestState.isExploration() && requestState.asExploration().singleAnswerRequired()) {
                requestState.asExploration().downstreamManager().clearDownstreams();
                requestState.answerCache().setComplete();
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
        driver().execute(actor -> request.onResponse().accept(ReiterationQuery.Response.create(
                actor.driver(), cache.requiresReiteration())));
    }

    @Override
    public void terminate(Throwable cause) {
        super.terminate(cause);
        requestStates.clear();
    }

    protected List<Request> ruleDownstreams(Request fromUpstream) {
        List<Request> downstreams = new ArrayList<>();
        AnswerState.Partial.Concludable<?> partialAnswer = fromUpstream.partialAnswer().asConcludable();
        ConcludableResolver concludableResolver = registry.concludableResolvers(concludable).actor();  // TODO: Reaching through to the actor is not ideal
        for (Map.Entry<Driver<ConclusionResolver>, Set<Unifier>> entry:
                concludableResolver.conclusionResolvers().entrySet()) {
            Driver<ConclusionResolver> conclusionResolver = entry.getKey();
            Rule rule = conclusionResolver.actor().conclusion().rule();  // TODO: Reaching through to the actor is not ideal
            for (Unifier unifier : entry.getValue()) {
                partialAnswer.toDownstream(unifier, rule).ifPresent(
                        conclusion -> downstreams.add(Request.create(driver(), conclusionResolver, conclusion)));
            }
        }
        return downstreams;
    }

}
