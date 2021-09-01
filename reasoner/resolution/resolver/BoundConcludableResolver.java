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
import com.vaticle.typedb.core.common.iterator.Iterators;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.logic.Rule;
import com.vaticle.typedb.core.logic.resolvable.Unifier;
import com.vaticle.typedb.core.reasoner.resolution.ResolverRegistry;
import com.vaticle.typedb.core.reasoner.resolution.answer.AnswerState;
import com.vaticle.typedb.core.reasoner.resolution.answer.AnswerState.Partial;
import com.vaticle.typedb.core.reasoner.resolution.framework.AnswerCache;
import com.vaticle.typedb.core.reasoner.resolution.framework.Downstream;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public abstract class BoundConcludableResolver extends Resolver<BoundConcludableResolver>  {

    private static final Logger LOG = LoggerFactory.getLogger(BoundConcludableResolver.class);

    protected final ConceptMap bounds;
    private final Driver<ConcludableResolver> parent;
    private final Map<Request, ExploringRequestState<?>> requestStates;
    private boolean requiresReiteration;

    public BoundConcludableResolver(Driver<BoundConcludableResolver> driver, Driver<ConcludableResolver> parent,
                                    ConceptMap bounds, ResolverRegistry registry) {
        super(driver, BoundConcludableResolver.class.getSimpleName() + "(pattern: " +
                parent.actor().concludable().pattern() + ", bounds: " + bounds.concepts().toString() + ")", registry);
        this.parent = parent;
        this.bounds = bounds;
        this.requestStates = new HashMap<>();
        this.requiresReiteration = false;
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
        ExploringRequestState<?> requestState = requestStates.computeIfAbsent(
                fromUpstream, request -> createExploringRequestState(fromUpstream, iteration));
        Optional<Partial.Compound<?, ?>> upstreamAnswer = upstreamAnswer(requestState);
        if (upstreamAnswer.isPresent()) {
            answerToUpstream(upstreamAnswer.get(), fromUpstream, requestState, iteration);
        } else if (cache().isComplete()) {
            failToUpstream(fromUpstream, iteration);
        } else if (isRecursion(fromUpstream.partialAnswer()).isPresent()) {
            cycleToUpstream(fromUpstream, cache().size(), iteration);
        } else if (requestState.downstreamManager().hasNextUnblocked()) {
            requestFromDownstream(requestState.downstreamManager().nextUnblocked(), fromUpstream, iteration);
        } else if (requestState.downstreamManager().allDownstreamsCycleToHere()) {
            cache().setComplete();
            failToUpstream(fromUpstream, iteration);
        } else {
            Downstream downstream = requestState.downstreamManager().next();
            requestState.downstreamManager().unblock(downstream);
            requestFromDownstream(downstream, fromUpstream, iteration);
        }
    }

    private Optional<Partial.Concludable<?>> isRecursion(Partial<?> partialAnswer) {
        // TODO Convert to boolean
        Partial<?> a = partialAnswer;
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

    abstract ExploringRequestState<?> createExploringRequestState(Request fromUpstream, int iteration);

    @Override
    protected void receiveAnswer(Response.Answer fromDownstream, int iteration) {
        if (isTerminated()) return;
        Request fromUpstream = fromUpstream(fromDownstream.sourceRequest());
        ExploringRequestState<?> requestState = this.requestStates.get(fromUpstream);
        if (requestState.newAnswer(fromDownstream.answer())) requestState.downstreamManager().clearOutdatedBlocks();
        answerUpstreamOrSearchDownstreamOrFail(fromUpstream, requestState, iteration);
    }

    @Override
    protected void receiveFail(Response.Fail fromDownstream, int iteration) {
        LOG.trace("{}: received Fail: {}", name(), fromDownstream);
        if (isTerminated()) return;

        Request toDownstream = fromDownstream.sourceRequest();
        Request fromUpstream = fromUpstream(toDownstream);

        ExploringRequestState<?> requestState = this.requestStates.get(fromUpstream);
        assert iteration == requestState.iteration();
        requestState.downstreamManager().remove(Downstream.of(fromDownstream.sourceRequest()));
        answerUpstreamOrSearchDownstreamOrFail(fromUpstream, requestState, iteration);
    }

    private void answerUpstreamOrSearchDownstreamOrFail(Request fromUpstream, ExploringRequestState<?> requestState,
                                                        int iteration) {
        Optional<Partial.Compound<?, ?>> upstreamAnswer = upstreamAnswer(requestState);
        if (upstreamAnswer.isPresent()) {
            answerToUpstream(upstreamAnswer.get(), fromUpstream, requestState, iteration);
        } else if (cache().isComplete()) {
            failToUpstream(fromUpstream, iteration);
        } else if (requestState.downstreamManager().hasNextUnblocked()) {
            requestFromDownstream(requestState.downstreamManager().nextUnblocked(), fromUpstream, iteration);
        } else if (requestState.downstreamManager().allDownstreamsCycleToHere()) {
            cache().setComplete();
            failToUpstream(fromUpstream, iteration);
        } else {
            Downstream downstream = requestState.downstreamManager().next();
            requestState.downstreamManager().unblock(downstream);
            requestFromDownstream(downstream, fromUpstream, iteration);
        }
    }

    @Override
    protected void receiveCycle(Response.Cycle fromDownstream, int iteration) {
        LOG.trace("{}: received Cycle: {}", name(), fromDownstream);
        if (isTerminated()) return;

        Downstream cyclingDownstream = Downstream.of(fromDownstream.sourceRequest());
        Request fromUpstream = fromUpstream(fromDownstream.sourceRequest());

        ExploringRequestState<?> requestState = this.requestStates.get(fromUpstream);
        assert iteration == requestState.iteration();
        if (requestState.downstreamManager().contains(cyclingDownstream)) {
            iterate(fromDownstream.origins())
                    .filter(blocker -> !requestState.downstreamManager().isOutdated(blocker))
                    .toSet() // TODO: Goes away if we have .forEach() on iterator
                    .forEach(blocker -> requestState.downstreamManager().block(cyclingDownstream, blocker));
        }
        Optional<Partial.Compound<?, ?>> upstreamAnswer = upstreamAnswer(requestState);
        if (upstreamAnswer.isPresent()) {
            answerToUpstream(upstreamAnswer.get(), fromUpstream, requestState, iteration);
        } else if (cache().isComplete()) {
            failToUpstream(fromUpstream, iteration);
        } else if (requestState.downstreamManager().hasNextUnblocked()) {
            requestFromDownstream(requestState.downstreamManager().nextUnblocked(), fromUpstream, iteration);
        } else if (requestState.downstreamManager().allDownstreamsCycleToHere()) {
            cache().setComplete();
            failToUpstream(fromUpstream, iteration);
        } else {
            cycleToUpstream(fromUpstream, requestState.downstreamManager().visitable(), iteration);
        }
    }

    private static Optional<Partial.Compound<?, ?>> upstreamAnswer(ExploringRequestState<?> requestState) {
        return requestState.nextAnswer().map(Partial::asCompound);
    }

    protected void answerToUpstream(AnswerState answer, Request fromUpstream, ExploringRequestState<?> requestState, int iteration) {
        /*
        When we only require 1 answer (eg. when the conjunction is already fully bound), we can short circuit
        and prevent exploration of further rules.

        One latency optimisation we could do here, is keep track of how many N repeated requests are received,
        forward them downstream (to parallelise searching for the single answer), and when the first one finds an answer,
        we respond for all N ahead of time. Then, when the rules actually return an answer to this concludable, we do nothing.
         */
        if (requestState.singleAnswerRequired()) {
            requestState.downstreamManager().clear();
            cache().setComplete();
        }
        answerToUpstream(answer, fromUpstream, iteration);
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

    protected List<Downstream> ruleDownstreams(Request fromUpstream) {
        List<Downstream> downstreams = new ArrayList<>();
        Partial.Concludable<?> partialAnswer = fromUpstream.partialAnswer().asConcludable();
        for (Map.Entry<Driver<ConclusionResolver>, Set<Unifier>> entry:
                parent().actor().conclusionResolvers().entrySet()) { // TODO: Reaching through to the actor is not ideal
            Driver<ConclusionResolver> conclusionResolver = entry.getKey();
            Rule rule = conclusionResolver.actor().conclusion().rule();  // TODO: Reaching through to the actor is not ideal
            for (Unifier unifier : entry.getValue()) {
                partialAnswer.toDownstream(unifier, rule).ifPresent(
                        conclusion -> downstreams.add(Downstream.create(driver(), conclusionResolver, conclusion)));
            }
        }
        return downstreams;
    }

    protected abstract class ExploringRequestState<ANSWER> extends CachingRequestState<ANSWER> implements RequestState.Exploration {

        private final ConcludableDownstreamManager downstreamManager;

        protected ExploringRequestState(Request fromUpstream, AnswerCache<ANSWER> answerCache, int iteration,
                                        List<Downstream> ruleDownstreams, boolean deduplicate) {
            super(fromUpstream, answerCache, iteration, deduplicate);
            this.downstreamManager = new ConcludableDownstreamManager(ruleDownstreams);
        }

        @Override
        public ConcludableDownstreamManager downstreamManager() {
            return downstreamManager;
        }

        abstract ANSWER answerFromPartial(Partial<?> partial);

        @Override
        public boolean newAnswer(Partial<?> partial) {
            // TODO: Method internals are not specific to the requestState any more
            return !answerCache.isComplete() && answerCache.add(answerFromPartial(partial));
        }

        class ConcludableDownstreamManager extends DownstreamManager.Blockable {

            public ConcludableDownstreamManager(List<Downstream> ruleDownstreams) {
                super(ruleDownstreams);
            }

            public void clearOutdatedBlocks() {
                // TODO: Find the most idiomatic way to do this, ideally the map should be final
                // TODO: .remove() is unsupported on iterators
                Map<Downstream, Set<Response.Cycle.Origin>> newBlocked = new LinkedHashMap<>();
                blocked.forEach((downstream, blockers) -> {
                    Set<Response.Cycle.Origin> newBlockers =
                            iterate(blockers).filter(blocker -> !isOutdated(blocker)).toSet();
                    if (newBlockers.isEmpty()) downstreams.add(downstream);
                    else newBlocked.put(downstream, newBlockers);
                });
                blocked = newBlocked;
            }

            private boolean originatedHere(Response.Cycle.Origin cycleOrigin) {
                return cycleOrigin.sender().equals(driver());
            }

            public boolean isOutdated(Response.Cycle.Origin cycleOrigin) {
                return originatedHere(cycleOrigin) && cycleOrigin.numAnswersSeen() < cache().size();
            }

            public Set<Response.Cycle.Origin> visitable() {
                return iterate(blocked.values()).flatMap(Iterators::iterate).filter(o -> !originatedHere(o)).toSet();
            }

            public boolean allDownstreamsCycleToHere() {
                return iterate(blocked.values())
                        .filter(cycleOrigins -> iterate(cycleOrigins)
                                .filter(this::originatedHere)
                                .first()
                                .isEmpty())
                        .first()
                        .isEmpty();
            }
        }
    }
}
