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
    private final Map<Partial.Concludable<?>, BoundConcludableResolutionState<?>> resolutionStates;
    protected final BoundConcludableContext context;

    protected BoundConcludableResolver(Driver<BoundConcludableResolver> driver, BoundConcludableContext context,
                                       ConceptMap bounds, ResolverRegistry registry) {
        super(driver, BoundConcludableResolver.class.getSimpleName() + "(pattern: " +
                context.concludable().pattern() + ", bounds: " + bounds.concepts().toString() + ")", registry);
        this.context = context;
        this.bounds = bounds;
        this.resolutionStates = new HashMap<>();
    }

    @Override
    public void receiveVisit(Request.Visit fromUpstream) {
        LOG.trace("{}: received Visit: {}", name(), fromUpstream);
        if (isTerminated()) return;
        assert fromUpstream.partialAnswer().isConcludable();
        getOrCreateResolutionState(fromUpstream).receiveVisit(fromUpstream);
    }

    @Override
    protected void receiveRevisit(Request.Revisit fromUpstream) {
        assert fromUpstream.visit().partialAnswer().isConcludable();
        getOrCreateResolutionState(fromUpstream.visit()).receiveRevisit(fromUpstream, fromUpstream.cycles());
    }

    private BoundConcludableResolutionState<?> getOrCreateResolutionState(Request.Visit fromUpstream) {
        return resolutionStates.computeIfAbsent(fromUpstream.partialAnswer().asConcludable(), partial -> {
            if (isCycle(partial)) return createBlockedResolutionState(fromUpstream.partialAnswer());
            else return createExploringResolutionState(fromUpstream.partialAnswer());
        });
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

    abstract ExploringResolutionState<?> createExploringResolutionState(Partial<?> fromUpstream);

    abstract BlockedResolutionState<?> createBlockedResolutionState(Partial<?> fromUpstream);

    @Override
    protected void receiveAnswer(Response.Answer fromDownstream) {
        if (isTerminated()) return;
        this.resolutionStates.get(partialFromUpstream(fromDownstream).asConcludable()).receiveAnswer(fromDownstream);
    }

    @Override
    protected void receiveFail(Response.Fail fromDownstream) {
        LOG.trace("{}: received Fail: {}", name(), fromDownstream);
        if (isTerminated()) return;
        this.resolutionStates.get(partialFromUpstream(fromDownstream).asConcludable()).receiveFail(fromDownstream);
    }

    @Override
    protected void receiveBlocked(Response.Blocked fromDownstream) {
        LOG.trace("{}: received Blocked: {}", name(), fromDownstream);
        if (isTerminated()) return;
        this.resolutionStates.get(partialFromUpstream(fromDownstream).asConcludable()).receiveBlocked(fromDownstream);
    }

    @Override
    protected void initialiseDownstreamResolvers() {
        throw TypeDBException.of(ILLEGAL_STATE);
    }

    protected abstract AnswerCache<?> cache();

    @Override
    public void terminate(Throwable cause) {
        super.terminate(cause);
        resolutionStates.clear();
    }

    protected List<Request.Factory> ruleDownstreams(Partial<?> fromUpstream) {
        List<Request.Factory> downstreams = new ArrayList<>();
        Partial.Concludable<?> partialAnswer = fromUpstream.asConcludable();
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

        FunctionalIterator<? extends Partial<?>> toUpstream(Partial<?> fromUpstream, ANSWER partial);

    }

    abstract static class BoundConcludableResolutionState<ANSWER> extends CachingResolutionState<ANSWER> {
        protected final UpstreamBehaviour<ANSWER> upstreamBehaviour;
        protected final boolean singleAnswerRequired;

        protected BoundConcludableResolutionState(Partial<?> fromUpstream, AnswerCache<ANSWER> answerCache,
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

        abstract void sendNextMessage(Request.Visit visit);

        protected void receiveVisit(Request.Visit visit) {
            sendNextMessage(visit);
        }

        abstract void receiveRevisit(Request.Revisit revisit, Set<Cycle> cycles);

        abstract void receiveAnswer(Response.Answer fromDownstream);

        abstract void receiveFail(Response.Fail fromDownstream);

        abstract void receiveBlocked(Response.Blocked fromDownstream);

        protected Optional<Partial.Compound<?, ?>> upstreamAnswer() {
            return nextAnswer().map(Partial::asCompound);
        }

    }

    protected class BlockedResolutionState<ANSWER> extends BoundConcludableResolutionState<ANSWER> {

        public BlockedResolutionState(Partial<?> fromUpstream, AnswerCache<ANSWER> answerCache, boolean deduplicate,
                                      UpstreamBehaviour<ANSWER> upstreamBehaviour, boolean singleAnswerRequired) {
            super(fromUpstream, answerCache, deduplicate, upstreamBehaviour, singleAnswerRequired);
        }

        @Override
        void sendNextMessage(Request.Visit visit) {
            Optional<Partial.Compound<?, ?>> upstreamAnswer = upstreamAnswer();
            if (upstreamAnswer.isPresent()) {
                if (singleAnswerRequired()) cache().setComplete();
                answerToUpstream(upstreamAnswer.get(), visit);
            } else if (cache().isComplete()) {
                failToUpstream(visit);
            } else {
                blockToUpstream(visit, cache().size());
            }
        }

        @Override
        void receiveRevisit(Request.Revisit revisit, Set<Cycle> cycles) {
            sendNextMessage(revisit.visit());
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

    protected class ExploringResolutionState<ANSWER> extends BoundConcludableResolutionState<ANSWER> implements ResolutionState.Exploration {

        private final DownstreamManager downstreamManager;

        protected ExploringResolutionState(Partial<?> fromUpstream, AnswerCache<ANSWER> answerCache,
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
        void receiveRevisit(Request.Revisit revisit, Set<Cycle> cycles) {
            downstreamManager().unblock(cycles);
            sendNextMessage(revisit.visit());
        }

        @Override
        void receiveAnswer(Response.Answer fromDownstream) {
            if (newAnswer(fromDownstream.answer())) {
                Set<Cycle> outdated = outdatedCycles(downstreamManager().cycles());
                if (!outdated.isEmpty()) downstreamManager().unblock(outdated);
            }
            sendNextMessage(upstreamRequest(fromDownstream).visit());
        }

        @Override
        void receiveFail(Response.Fail fromDownstream) {
            downstreamManager().remove(fromDownstream.sourceRequest());
            sendNextMessage(upstreamRequest(fromDownstream).visit());
        }

        @Override
        void receiveBlocked(Response.Blocked fromDownstream) {
            Request.Factory blockingDownstream = fromDownstream.sourceRequest();
            if (downstreamManager().contains(blockingDownstream)) {
                downstreamManager().block(blockingDownstream, fromDownstream.cycles());
                Set<Cycle> outdated = outdatedCycles(downstreamManager().cycles());
                if (!outdated.isEmpty()) downstreamManager().unblock(outdated);
            }
            sendNextMessage(upstreamRequest(fromDownstream).visit());
        }

        @Override
        protected void sendNextMessage(Request.Visit visit) {
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
                answerToUpstream(upstreamAnswer.get(), visit);
            } else if (cache().isComplete()) {
                failToUpstream(visit);
            } else if (downstreamManager().hasNextVisit()) {
                visitDownstream(downstreamManager().nextVisit(visit.trace()), visit);
            } else if (downstreamManager().hasNextRevisit()) {
                revisitDownstream(downstreamManager().nextRevisit(visit.trace()), visit);
            } else if (startsHere(downstreamManager().cycles())) {
                cache().setComplete();
                failToUpstream(visit);
            } else {
                blockToUpstream(visit, startingElsewhere(downstreamManager().cycles()));
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
