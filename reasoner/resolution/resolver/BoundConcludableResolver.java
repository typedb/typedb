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
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.logic.Rule;
import com.vaticle.typedb.core.logic.resolvable.Unifier;
import com.vaticle.typedb.core.reasoner.resolution.ResolverRegistry;
import com.vaticle.typedb.core.reasoner.resolution.answer.AnswerState;
import com.vaticle.typedb.core.reasoner.resolution.framework.AnswerCache;
import com.vaticle.typedb.core.reasoner.resolution.framework.ReiterationQuery;
import com.vaticle.typedb.core.reasoner.resolution.framework.Request;
import com.vaticle.typedb.core.reasoner.resolution.framework.RequestState;
import com.vaticle.typedb.core.reasoner.resolution.framework.RequestState.CachingRequestState;
import com.vaticle.typedb.core.reasoner.resolution.framework.Resolver;
import com.vaticle.typedb.core.reasoner.resolution.framework.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;

public abstract class BoundConcludableResolver extends Resolver<BoundConcludableResolver>  {

    private static final Logger LOG = LoggerFactory.getLogger(BoundConcludableResolver.class);

    private final Map<Request, CachingRequestState<?>> requestStates;
    private final Driver<ConcludableResolver> parent;
    protected final ConceptMap bounds;
    private boolean requiresReiteration;
    private final Set<Request> recursiveRequests;

    public BoundConcludableResolver(Driver<BoundConcludableResolver> driver, Driver<ConcludableResolver> parent,
                                    ConceptMap bounds, ResolverRegistry registry) {
        super(driver, BoundConcludableResolver.class.getSimpleName() + "(bounds: " + bounds.toString() + ")", registry);
        this.parent = parent;
        this.bounds = bounds;
        this.requestStates = new HashMap<>();
        this.requiresReiteration = false;
        this.recursiveRequests = new HashSet<>();
    }

    protected Driver<ConcludableResolver> parent() {
        return parent;
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
        assert fromUpstream.partialAnswer().isConcludable();
        CachingRequestState<?> requestState;

        if (isRecursion(fromUpstream.partialAnswer()).isPresent()) {
            recursiveRequests.add(fromUpstream);
        }

        requestState = requestStates.computeIfAbsent(
                fromUpstream, request -> createExploringRequestState(fromUpstream, iteration));

        sendAnswerOrSearchRulesOrFail(fromUpstream, iteration, requestState);
    }

    private Optional<AnswerState.Partial.Concludable<?>> isRecursion(AnswerState.Partial<?> partialAnswer) {
        AnswerState.Partial<?> a = partialAnswer;
        while (a.parent().isPartial()) {
            a = a.parent().asPartial();
            if (a.isConcludable()
                    && a.asConcludable().concludable().alphaEquals(parent().actor().concludable()).isValid()
//                    && registry.concludableResolver(a.asConcludable().concludable()).equals(parent()) // TODO: This is more optimal as alpha equivalence is already done, but requires all concludable -> actor pairs to be stored in the registry
                    && a.conceptMap().equals(bounds)) {
                return Optional.of(a.asConcludable());
            }
        }
        return Optional.empty();
    }

    abstract CachingRequestState<?> createExploringRequestState(Request fromUpstream, int iteration);

    @Override
    protected void receiveAnswer(Response.Answer fromDownstream, int iteration) {
        if (isTerminated()) return;
        Request fromUpstream = fromUpstream(fromDownstream.sourceRequest());
        CachingRequestState<?> requestState = this.requestStates.get(fromUpstream);
        assert requestState.isExploration();
        boolean ansIsNew = requestState.asExploration().newAnswer(fromDownstream.answer());
        if (!recursiveRequests.isEmpty() && ansIsNew) {
            requiresReiteration = true;
        }
        sendAnswerOrSearchRulesOrFail(fromUpstream, iteration, requestState);
    }

    @Override
    protected void receiveFail(Response.Fail fromDownstream, int iteration) {
        LOG.trace("{}: received Fail: {}", name(), fromDownstream);
        if (isTerminated()) return;

        Request toDownstream = fromDownstream.sourceRequest();
        Request fromUpstream = fromUpstream(toDownstream);

        CachingRequestState<?> requestState = this.requestStates.get(fromUpstream);
        assert iteration == requestState.iteration();
        assert requestState.isExploration();
        requestState.asExploration().downstreamManager().removeDownstream(fromDownstream.sourceRequest());
        sendAnswerOrSearchRulesOrFail(fromUpstream, iteration, requestState);
    }

    private void sendAnswerOrSearchRulesOrFail(Request fromUpstream, int iteration, CachingRequestState<?> requestState) {
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
            if (requestState.isExploration() && !requestState.answerCache().isComplete() && !recursiveRequests.contains(fromUpstream)) {
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
                actor.driver(), requiresReiteration)));
    }

    protected abstract AnswerCache<?> cache();

    @Override
    public void terminate(Throwable cause) {
        super.terminate(cause);
        requestStates.clear();
    }

    protected List<Request> ruleDownstreams(Request fromUpstream) {
        List<Request> downstreams = new ArrayList<>();
        AnswerState.Partial.Concludable<?> partialAnswer = fromUpstream.partialAnswer().asConcludable();
        for (Map.Entry<Driver<ConclusionResolver>, Set<Unifier>> entry:
                parent().actor().conclusionResolvers().entrySet()) { // TODO: Reaching through to the actor is not ideal
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
