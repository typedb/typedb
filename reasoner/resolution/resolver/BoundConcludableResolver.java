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
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.logic.Rule;
import com.vaticle.typedb.core.logic.resolvable.Concludable;
import com.vaticle.typedb.core.logic.resolvable.Unifier;
import com.vaticle.typedb.core.reasoner.resolution.ResolverRegistry;
import com.vaticle.typedb.core.reasoner.resolution.answer.AnswerState.Partial;
import com.vaticle.typedb.core.reasoner.resolution.framework.AnswerCache;
import com.vaticle.typedb.core.reasoner.resolution.framework.Request;
import com.vaticle.typedb.core.reasoner.resolution.framework.ResolutionTracer.Trace;
import com.vaticle.typedb.core.reasoner.resolution.framework.Resolver;
import com.vaticle.typedb.core.reasoner.resolution.framework.Response;
import com.vaticle.typedb.core.reasoner.resolution.framework.Response.Blocked.Cycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public abstract class BoundConcludableResolver extends Resolver<BoundConcludableResolver>  {

    private static final Logger LOG = LoggerFactory.getLogger(BoundConcludableResolver.class);

    protected final ConceptMap bounds;
    private final Map<Request, BoundConcludableRequestState<?>> requestStates;
    protected final BoundConcludableContext context;

    protected BoundConcludableResolver(Driver<BoundConcludableResolver> driver, BoundConcludableContext context,
                                       ConceptMap bounds, ResolverRegistry registry) {
        super(driver, BoundConcludableResolver.class.getSimpleName() + "(pattern: " +
                context.concludable().pattern() + ", bounds: " + bounds.concepts().toString() + ")", registry);
        this.context = context;
        this.bounds = bounds;
        this.requestStates = new HashMap<>();
    }

    @Override
    public void receiveVisit(Request.Visit fromUpstream) {
        LOG.trace("{}: received Visit: {}", name(), fromUpstream);
        if (isTerminated()) return;
        assert fromUpstream.partialAnswer().isConcludable();
        getOrCreateRequestState(fromUpstream).receiveVisit(fromUpstream.trace());
    }

    @Override
    protected void receiveRevisit(Request.Revisit fromUpstream) {
        assert fromUpstream.visit().partialAnswer().isConcludable();
        getOrCreateRequestState(fromUpstream.visit()).receiveRevisit(fromUpstream.trace(), fromUpstream.cycles());
    }

    private BoundConcludableRequestState<?> getOrCreateRequestState(Request.Visit fromUpstream) {
        // TODO: shouldn't check for cycling every time, can search for a requestState first
        BoundConcludableRequestState<?> requestState = requestStates.get(fromUpstream);
        if (requestState != null) {
            return requestState;
        } else {
            for (BoundConcludableRequestState<?> rs : requestStates.values()) {
                // Linear search, optimise using state
                if (fromUpstream.factory().equals(rs.fromUpstream())) {
                    requestStates.put(fromUpstream, rs);
                    return rs;
                }
            }
            BoundConcludableRequestState<?> rs;
            if (isCycle(fromUpstream.partialAnswer())) rs = createBlockedRequestState(fromUpstream.factory());
            else rs = createExploringRequestState(fromUpstream.factory());
            requestStates.put(fromUpstream, rs);
            return rs;
        }
    }

    private boolean isCycle(Partial<?> partialAnswer) {
        Partial<?> a = partialAnswer;
        while (a.parent().isPartial()) {
            a = a.parent().asPartial();
            if (a.isConcludable()
                    && registry.concludables(context.parent()).contains(a.asConcludable().concludable())
                    && a.conceptMap().equals(bounds)) {
                return true;
            }
        }
        return false;
    }

    abstract ExploringRequestState<?> createExploringRequestState(Request.Factory fromUpstream);

    abstract BlockedRequestState<?> createBlockedRequestState(Request.Factory fromUpstream);

    @Override
    protected void receiveAnswer(Response.Answer fromDownstream) {
        if (isTerminated()) return;
        this.requestStates.get(upstreamRequest(fromDownstream)).receiveAnswer(fromDownstream);
    }

    @Override
    protected void receiveFail(Response.Fail fromDownstream) {
        LOG.trace("{}: received Fail: {}", name(), fromDownstream);
        if (isTerminated()) return;
        this.requestStates.get(upstreamRequest(fromDownstream)).receiveFail(fromDownstream);
    }

    @Override
    protected void receiveBlocked(Response.Blocked fromDownstream) {
        LOG.trace("{}: received Blocked: {}", name(), fromDownstream);
        if (isTerminated()) return;
        this.requestStates.get(upstreamRequest(fromDownstream)).receiveBlocked(fromDownstream);
    }

    @Override
    protected void initialiseDownstreamResolvers() {
        throw TypeDBException.of(ILLEGAL_STATE);
    }

    protected abstract AnswerCache<?> cache();

    @Override
    public void terminate(Throwable cause) {
        super.terminate(cause);
        requestStates.clear();
    }

    protected List<Request.Factory> ruleDownstreams(Request.Factory fromUpstream) {
        List<Request.Factory> downstreams = new ArrayList<>();
        Partial.Concludable<?> partialAnswer = fromUpstream.partialAnswer().asConcludable();
        for (Map.Entry<Driver<ConclusionResolver>, Set<Unifier>> entry: context.conclusionResolvers().entrySet()) {
            Driver<ConclusionResolver> conclusionResolver = entry.getKey();
            Rule rule = registry.conclusionRule(conclusionResolver);
            for (Unifier unifier : entry.getValue()) {
                partialAnswer.toDownstream(unifier, rule).ifPresent(
                        conclusion -> downstreams.add(Request.Factory.create(driver(), conclusionResolver, conclusion)));
            }
        }
        return downstreams;
    }

    protected interface UpstreamBehaviour<ANSWER> {

        ANSWER answerFromPartial(Partial<?> partial);

        FunctionalIterator<? extends Partial<?>> toUpstream(Request.Factory fromUpstream, ANSWER partial);

    }

    abstract static class BoundConcludableRequestState<ANSWER> extends CachingRequestState<ANSWER> {
        protected final UpstreamBehaviour<ANSWER> upstreamBehaviour;
        protected final boolean singleAnswerRequired;

        protected BoundConcludableRequestState(Request.Factory fromUpstream, AnswerCache<ANSWER> answerCache,
                                               boolean deduplicate, UpstreamBehaviour<ANSWER> upstreamBehaviour,
                                               boolean singleAnswerRequired) {
            super(fromUpstream, answerCache, deduplicate);
            this.upstreamBehaviour = upstreamBehaviour;
            this.singleAnswerRequired = singleAnswerRequired;
        }

        public boolean singleAnswerRequired() {
            return singleAnswerRequired;
        }

        public boolean newAnswer(Partial<?> partial) {
            return !answerCache.isComplete() && answerCache.add(upstreamBehaviour.answerFromPartial(partial));
        }

        @Override
        protected FunctionalIterator<? extends Partial<?>> toUpstream(ANSWER answer) {
            return upstreamBehaviour.toUpstream(fromUpstream, answer);
        }

        abstract void sendNextMessage(Trace trace);

        protected void receiveVisit(Trace trace) {
            sendNextMessage(trace);
        }

        abstract void receiveRevisit(Trace trace, Set<Cycle> cycles);

        abstract void receiveAnswer(Response.Answer fromDownstream);

        abstract void receiveFail(Response.Fail fromDownstream);

        abstract void receiveBlocked(Response.Blocked fromDownstream);

        protected Optional<Partial.Compound<?, ?>> upstreamAnswer() {
            return nextAnswer().map(Partial::asCompound);
        }

    }

    protected class BlockedRequestState<ANSWER> extends BoundConcludableRequestState<ANSWER> {

        public BlockedRequestState(Request.Factory fromUpstream, AnswerCache<ANSWER> answerCache, boolean deduplicate,
                                   UpstreamBehaviour<ANSWER> upstreamBehaviour, boolean singleAnswerRequired) {
            super(fromUpstream, answerCache, deduplicate, upstreamBehaviour, singleAnswerRequired);
        }

        @Override
        void sendNextMessage(Trace trace) {
            Optional<Partial.Compound<?, ?>> upstreamAnswer = upstreamAnswer();
            if (upstreamAnswer.isPresent()) {
                if (singleAnswerRequired()) cache().setComplete();
                answerToUpstream(upstreamAnswer.get(), fromUpstream().createVisit(trace));
            } else if (cache().isComplete()) {
                failToUpstream(fromUpstream().createVisit(trace));
            } else {
                blockToUpstream(fromUpstream().createVisit(trace), cache().size());
            }
        }

        @Override
        void receiveRevisit(Trace trace, Set<Cycle> cycles) {
            sendNextMessage(trace);
        }

        @Override
        void receiveAnswer(Response.Answer fromDownstream) {
            throw TypeDBException.of(ILLEGAL_STATE);
        }

        @Override
        void receiveFail(Response.Fail fromDownstream) {
            throw TypeDBException.of(ILLEGAL_STATE);
        }

        @Override
        void receiveBlocked(Response.Blocked fromDownstream) {
            throw TypeDBException.of(ILLEGAL_STATE);
        }
    }

    protected class ExploringRequestState<ANSWER> extends BoundConcludableRequestState<ANSWER> implements RequestState.Exploration {

        private final DownstreamManager downstreamManager;

        protected ExploringRequestState(Request.Factory fromUpstream, AnswerCache<ANSWER> answerCache,
                                        List<Request.Factory> ruleDownstreams, boolean deduplicate,
                                        UpstreamBehaviour<ANSWER> upstreamBehaviour, boolean singleAnswerRequired) {
            super(fromUpstream, answerCache, deduplicate, upstreamBehaviour, singleAnswerRequired);
            this.downstreamManager = new DownstreamManager(ruleDownstreams);
        }

        @Override
        public DownstreamManager downstreamManager() {
            return downstreamManager;
        }

        @Override
        void receiveRevisit(Trace trace, Set<Cycle> cycles) {
            downstreamManager().unblock(cycles);
            sendNextMessage(trace);
        }

        @Override
        void receiveAnswer(Response.Answer fromDownstream) {
            if (newAnswer(fromDownstream.answer())) {
                Set<Cycle> outdated = outdatedCycles(downstreamManager().cycles());
                if (!outdated.isEmpty()) downstreamManager().unblock(outdated);
            }
            sendNextMessage(fromDownstream.trace());
        }

        @Override
        void receiveFail(Response.Fail fromDownstream) {
            downstreamManager().remove(fromDownstream.sourceRequest());
            sendNextMessage(fromDownstream.trace());
        }

        @Override
        void receiveBlocked(Response.Blocked fromDownstream) {
            Request.Factory blockingDownstream = fromDownstream.sourceRequest();
            if (downstreamManager().contains(blockingDownstream)) {
                downstreamManager().block(blockingDownstream, fromDownstream.cycles());
                Set<Cycle> outdated = outdatedCycles(downstreamManager().cycles());
                if (!outdated.isEmpty()) downstreamManager().unblock(outdated);
            }
            sendNextMessage(fromDownstream.trace());
        }

        @Override
        protected void sendNextMessage(Trace trace) {
            Optional<Partial.Compound<?, ?>> upstreamAnswer = upstreamAnswer();
            if (upstreamAnswer.isPresent()) {
                if (singleAnswerRequired()) {
                    /*
                    When we only require 1 answer (eg. when the conjunction is already fully bound), we can short
                    circuit and prevent exploration of further rules.

                    One latency optimisation we could do here, is keep track of how many N repeated requests are
                    received, forward them downstream (to parallelise searching for the single answer), and when the
                    first one finds an answer, we respond for all N ahead of time. Then, when the rules actually
                    return an answer to this concludable, we do nothing.
                     */
                    downstreamManager().clear();
                    cache().setComplete();
                }
                answerToUpstream(upstreamAnswer.get(), fromUpstream().createVisit(trace));
            } else if (cache().isComplete()) {
                failToUpstream(fromUpstream().createVisit(trace));
            } else if (downstreamManager().hasNextVisit()) {
                visitDownstream(downstreamManager().nextVisit(trace), fromUpstream().createVisit(trace));
            } else if (downstreamManager().hasNextRevisit()) {
                revisitDownstream(downstreamManager().nextRevisit(trace), fromUpstream().createVisit(trace));
            } else if (startsHere(downstreamManager().cycles())) {
                cache().setComplete();
                failToUpstream(fromUpstream().createVisit(trace));
            } else {
                blockToUpstream(fromUpstream().createVisit(trace), startingElsewhere(downstreamManager().cycles()));
            }
        }

        private Set<Cycle> outdatedCycles(Set<Cycle> cycles) {
            return iterate(cycles).filter(this::isOutdated).toSet();
        }

        private Set<Cycle> startingElsewhere(Set<Cycle> cycles) {
            return iterate(cycles).filter(c -> !startsHere(c)).toSet();
        }

        private boolean isOutdated(Cycle cycle) {
            return startsHere(cycle) && cycle.numAnswersSeen() < cache().size();
        }

        private boolean startsHere(Set<Cycle> cycles) {
            return iterate(cycles).filter(c -> !startsHere(c)).first().isEmpty();
        }

        private boolean startsHere(Cycle cycle) {
            return cycle.end().equals(driver());
        }

    }

    public static class BoundConcludableContext {

        private final Driver<ConcludableResolver> parent;
        private final Concludable concludable;
        private final Map<Driver<ConclusionResolver>, Set<Unifier>> conclusionUnifiers;

        BoundConcludableContext(Driver<ConcludableResolver> parent,
                                Concludable concludable,
                                Map<Driver<ConclusionResolver>, Set<Unifier>> conclusionResolvers) {

            this.parent = parent;
            this.concludable = concludable;
            this.conclusionUnifiers = conclusionResolvers;
        }

        Driver<ConcludableResolver> parent() {
            return parent;
        }

        public Concludable concludable() {
            return concludable;
        }

        public Map<Driver<ConclusionResolver>, Set<Unifier>> conclusionResolvers() {
            return conclusionUnifiers;
        }
    }

}
