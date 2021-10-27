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
import com.vaticle.typedb.core.traversal.common.Identifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public abstract class BoundConcludableResolver<RESOLVER extends BoundConcludableResolver<RESOLVER>> extends Resolver<RESOLVER>  {

    private static final Logger LOG = LoggerFactory.getLogger(BoundConcludableResolver.class);

    protected final ConceptMap bounds;
    protected final BoundConcludableContext context;
    protected final AnswerCache<ConceptMap> matchCache;

    protected BoundConcludableResolver(Driver<RESOLVER> driver, String name, BoundConcludableContext context,
                                       ConceptMap bounds, ResolverRegistry registry) {
        super(driver, name, registry);
        this.context = context;
        this.bounds = bounds;
        this.matchCache = new AnswerCache<>(() -> traversalIterator(context.concludable().pattern(), bounds));
    }

    @Override
    protected void initialiseDownstreamResolvers() {
        throw TypeDBException.of(ILLEGAL_STATE);
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

    private Set<Identifier.Variable.Retrievable> unboundVars() {
        Set<Identifier.Variable.Retrievable> missingBounds = new HashSet<>();
        iterate(context.concludable().pattern().variables())
                .filter(var -> var.id().isRetrievable()).forEachRemaining(var -> {
            if (var.isType() && !var.asType().label().isPresent()) {
                missingBounds.add(var.asType().id().asRetrievable());
            } else if (var.isThing() && !var.asThing().iid().isPresent()) {
                missingBounds.add(var.asThing().id().asRetrievable());
            }
        });
        return missingBounds;
    }

    protected BoundConcludableResolutionState<?> createResolutionState(Partial<?> fromUpstream) {
        if (fromUpstream.asConcludable().isExplain()) {
            return new ExplainResolutionState(fromUpstream, new AnswerCache<>(Iterators::empty));  // TODO: This cache is only useful when the same exact explain request is made more than once. Delete it when answers are used directly as soon as they are found.
        } else {
            boolean singleAnswerRequired = bounds.concepts().keySet().containsAll(unboundVars());
            return new MatchResolutionState(fromUpstream, matchCache, singleAnswerRequired);
        }
    }

    abstract static class BoundConcludableResolutionState<ANSWER> extends CachingResolutionState<ANSWER> {
        protected final boolean singleAnswerRequired;

        protected BoundConcludableResolutionState(Partial<?> fromUpstream, AnswerCache<ANSWER> answerCache,
                                                  boolean deduplicate, boolean singleAnswerRequired) {
            super(fromUpstream, answerCache, deduplicate);
            this.singleAnswerRequired = singleAnswerRequired;
        }

        public boolean singleAnswerRequired() {
            return singleAnswerRequired;
        }

        public boolean newAnswer(Partial<?> partial) {
            return !answerCache.isComplete() && answerCache.add(answerFromPartial(partial));
        }

        protected abstract ANSWER answerFromPartial(Partial<?> partial);

        protected Optional<Partial.Compound<?, ?>> upstreamAnswer() {
            return nextAnswer().map(Partial::asCompound);
        }

        protected AnswerCache<ANSWER> cache() {
            return answerCache;
        }

        public abstract void resetCacheSource();
    }

    class MatchResolutionState extends BoundConcludableResolutionState<ConceptMap> {

        protected MatchResolutionState(Partial<?> fromUpstream, AnswerCache<ConceptMap> answerCache,
                                       boolean singleAnswerRequired) {
            super(fromUpstream, answerCache, true, singleAnswerRequired);
        }

        @Override
        public ConceptMap answerFromPartial(Partial<?> partial) {
            return partial.conceptMap();
        }

        @Override
        public FunctionalIterator<? extends Partial<?>> toUpstream(ConceptMap conceptMap) {
            return Iterators.single(fromUpstream.asConcludable().asMatch().toUpstreamLookup(
                    conceptMap, context.concludable().isInferredAnswer(conceptMap)));
        }

        @Override
        public void resetCacheSource() {
            answerCache.setSource(() -> traversalIterator(context.concludable().pattern(), bounds));
        }
    }

    static class ExplainResolutionState extends BoundConcludableResolutionState<Partial.Concludable<?>> {

        protected ExplainResolutionState(Partial<?> fromUpstream, AnswerCache<Partial.Concludable<?>> answerCache) {
            super(fromUpstream, answerCache, false, false);
        }

        @Override
        public Partial.Concludable<?> answerFromPartial(Partial<?> partial) {
            return partial.asConcludable();
        }

        @Override
        public FunctionalIterator<? extends Partial<?>> toUpstream(Partial.Concludable<?> partial) {
            return Iterators.single(partial.asExplain().toUpstreamInferred());
        }

        @Override
        public void resetCacheSource() {
            answerCache.setSource(Iterators::empty);
        }
    }

    public static class Exploring extends BoundConcludableResolver<Exploring> {

        private final Map<Partial.Concludable<?>, BoundConcludableResolutionState<?>> resolutionStates;
        private final Map<Partial.Concludable<?>, ExplorationManager> explorationManagers;

        public Exploring(Driver<Exploring> driver, BoundConcludableContext context,
                         ConceptMap bounds, ResolverRegistry registry) {
            super(driver, BoundConcludableResolver.Exploring.class.getSimpleName() + "(pattern: " +
                    context.concludable().pattern() + ", bounds: " + bounds.concepts().toString() + ")", context,
                  bounds, registry);
            this.resolutionStates = new HashMap<>();
            this.explorationManagers = new HashMap<>();
        }

        protected void sendNextMessage(Request.Visit visit, BoundConcludableResolutionState<?> resolutionState,
                                       ExplorationManager explorationManager) {
            Optional<Partial.Compound<?, ?>> upstreamAnswer = resolutionState.upstreamAnswer();
            if (upstreamAnswer.isPresent()) {
                if (resolutionState.singleAnswerRequired()) {
                    /*
                    When we only require 1 answer (eg. when the conjunction is already fully bound), we can short
                    circuit and prevent exploration of further rules.

                    One latency optimisation we could do here, is keep track of how many N repeated requests are
                    received, forward them downstream (to parallelise searching for the single answer), and when the
                    first one finds an answer, we respond for all N ahead of time. Then, when the rules actually
                    return an answer to this concludable, we do nothing.
                     */
                    explorationManager.clear();
                    resolutionState.cache().setComplete();
                }
                answerToUpstream(upstreamAnswer.get(), visit);
            } else if (resolutionState.cache().isComplete()) {
                failToUpstream(visit);
            } else if (explorationManager.hasNextVisit()) {
                visitDownstream(explorationManager.nextVisit(visit.trace()), visit);
            } else if (explorationManager.hasNextRevisit()) {
                revisitDownstream(explorationManager.nextRevisit(visit.trace()), visit);
            } else if (startsHere(explorationManager.cycles())) {
                resolutionState.cache().setComplete();
                failToUpstream(visit);
            } else {
                blockToUpstream(visit, startingElsewhere(explorationManager.cycles()));
            }
        }

        @Override
        public void receiveVisit(Request.Visit fromUpstream) {
            LOG.trace("{}: received Visit: {}", name(), fromUpstream);
            if (isTerminated()) return;
            assert fromUpstream.partialAnswer().isConcludable();
            sendNextMessage(fromUpstream, getOrCreateResolutionState(fromUpstream), getOrCreateExplorationManager(fromUpstream));
        }

        @Override
        protected void receiveRevisit(Request.Revisit fromUpstream) {
            LOG.trace("{}: received Revisit: {}", name(), fromUpstream);
            if (isTerminated()) return;
            assert fromUpstream.visit().partialAnswer().isConcludable();
            BoundConcludableResolutionState<?> resolutionState = getOrCreateResolutionState(fromUpstream.visit());
            ExplorationManager explorationManager = getOrCreateExplorationManager(fromUpstream.visit());
            explorationManager.unblock(fromUpstream.cycles());
            sendNextMessage(fromUpstream.visit(), resolutionState, explorationManager);
        }

        @Override
        protected void receiveAnswer(Response.Answer fromDownstream) {
            LOG.trace("{}: received Answer: {}", name(), fromDownstream);
            if (isTerminated()) return;
            BoundConcludableResolutionState<?> resolutionState = getResolutionState(fromDownstream);
            ExplorationManager explorationManager = getExplorationManager(fromDownstream);
            if (resolutionState.newAnswer(fromDownstream.answer())) {
                Set<Cycle> outdated = outdatedCycles(explorationManager.cycles());
                if (!outdated.isEmpty()) explorationManager.unblock(outdated);
            }
            sendNextMessage(upstreamRequest(fromDownstream).visit(), resolutionState, explorationManager);
        }

        @Override
        protected void receiveFail(Response.Fail fromDownstream) {
            LOG.trace("{}: received Fail: {}", name(), fromDownstream);
            if (isTerminated()) return;
            BoundConcludableResolutionState<?> resolutionState = getResolutionState(fromDownstream);
            ExplorationManager explorationManager = getExplorationManager(fromDownstream);
            explorationManager.remove(fromDownstream.sourceRequest().visit().factory());
            sendNextMessage(upstreamRequest(fromDownstream).visit(), resolutionState, explorationManager);
        }

        @Override
        protected void receiveBlocked(Response.Blocked fromDownstream) {
            LOG.trace("{}: received Blocked: {}", name(), fromDownstream);
            if (isTerminated()) return;
            BoundConcludableResolutionState<?> resolutionState = getResolutionState(fromDownstream);
            ExplorationManager explorationManager = getExplorationManager(fromDownstream);
            Request.Factory blockingDownstream = fromDownstream.sourceRequest().visit().factory();
            if (explorationManager.contains(blockingDownstream)) {
                explorationManager.block(blockingDownstream, fromDownstream.cycles());
                Set<Cycle> outdated = outdatedCycles(explorationManager.cycles());
                if (!outdated.isEmpty()) explorationManager.unblock(outdated);
            }
            sendNextMessage(upstreamRequest(fromDownstream).visit(), resolutionState, explorationManager);
        }

        private Set<Cycle> outdatedCycles(Set<Cycle> cycles) {
            return iterate(cycles).filter(this::isOutdated).toSet();
        }

        private Set<Cycle> startingElsewhere(Set<Cycle> cycles) {
            return iterate(cycles).filter(c -> !startsHere(c)).toSet();
        }

        private boolean isOutdated(Cycle cycle) {
            return startsHere(cycle) && cycle.numAnswersSeen() < matchCache.size();
        }

        private boolean startsHere(Set<Cycle> cycles) {
            return iterate(cycles).filter(c -> !startsHere(c)).first().isEmpty();
        }

        private boolean startsHere(Cycle cycle) {
            return cycle.end().equals(context.concludable());
        }

        private BoundConcludableResolutionState<?> getOrCreateResolutionState(Request.Visit fromUpstream) {
            return resolutionStates.computeIfAbsent(fromUpstream.partialAnswer().asConcludable(),
                                                    partial -> createResolutionState(fromUpstream.partialAnswer()));
        }

        private ExplorationManager getOrCreateExplorationManager(Request.Visit fromUpstream) {
            return explorationManagers.computeIfAbsent(fromUpstream.partialAnswer().asConcludable(),
                                                      partial -> new ExplorationManager(
                                                              ruleDownstreams(fromUpstream.visit().partialAnswer())));
        }

        private BoundConcludableResolutionState<?> getResolutionState(Response response) {
            return resolutionStates.get(partialFromUpstream(response).asConcludable());
        }

        private ExplorationManager getExplorationManager(Response response) {
            return explorationManagers.get(partialFromUpstream(response).asConcludable());
        }

        @Override
        public void terminate(Throwable cause) {
            super.terminate(cause);
            resolutionStates.clear();
            explorationManagers.clear();
        }
    }

    public static class Blocked extends BoundConcludableResolver<Blocked> {

        private final Map<Partial.Concludable<?>, BoundConcludableResolutionState<?>> resolutionStates;

        public Blocked(Driver<Blocked> driver, BoundConcludableContext context, ConceptMap bounds,
                       ResolverRegistry registry) {
            super(driver, BoundConcludableResolver.Blocked.class.getSimpleName() + "(pattern: " +
                          context.concludable().pattern() + ", bounds: " + bounds.concepts().toString() + ")",
                  context, bounds, registry);
            this.resolutionStates = new HashMap<>();
        }

        void sendNextMessage(Request.Visit visit, BoundConcludableResolutionState<?> resolutionState) {
            Optional<Partial.Compound<?, ?>> upstreamAnswer = resolutionState.upstreamAnswer();
            if (upstreamAnswer.isPresent()) {
                if (resolutionState.singleAnswerRequired()) resolutionState.cache().setComplete();
                answerToUpstream(upstreamAnswer.get(), visit);
            } else if (resolutionState.cache().isComplete()) {
                failToUpstream(visit);
            } else {
                blockToUpstream(visit, set(new Cycle(context.concludable(), resolutionState.cache().size())));
            }
        }

        @Override
        public void receiveVisit(Request.Visit fromUpstream) {
            LOG.trace("{}: received Visit: {}", name(), fromUpstream);
            if (isTerminated()) return;
            assert fromUpstream.partialAnswer().isConcludable();
            assert !fromUpstream.partialAnswer().asConcludable().isExplain();
            sendNextMessage(fromUpstream, getOrCreateResolutionState(fromUpstream));
        }

        @Override
        protected void receiveRevisit(Request.Revisit fromUpstream) {
            LOG.trace("{}: received Revisit: {}", name(), fromUpstream);
            if (isTerminated()) return;
            assert fromUpstream.visit().partialAnswer().isConcludable();
            assert !fromUpstream.visit().partialAnswer().asConcludable().isExplain();
            BoundConcludableResolutionState<?> resolutionState = getOrCreateResolutionState(fromUpstream.visit());

            // Similar to sendNextMessage
            Optional<Partial.Compound<?, ?>> upstreamAnswer = resolutionState.upstreamAnswer();
            if (upstreamAnswer.isPresent()) {
                if (resolutionState.singleAnswerRequired()) resolutionState.cache().setComplete();
                answerToUpstream(upstreamAnswer.get(), fromUpstream.visit());
            } else if (resolutionState.cache().isComplete()) {
                failToUpstream(fromUpstream.visit());
            } else {
                resolutionState.resetCacheSource();
                upstreamAnswer = resolutionState.upstreamAnswer();
                if (upstreamAnswer.isPresent()) {
                    if (resolutionState.singleAnswerRequired()) resolutionState.cache().setComplete();
                    answerToUpstream(upstreamAnswer.get(), fromUpstream.visit());
                } else {
                    blockToUpstream(fromUpstream.visit(), set(new Cycle(context.concludable(), resolutionState.cache().size())));
                }
            }
        }

        @Override
        protected void receiveAnswer(Response.Answer fromDownstream) {
            throw TypeDBException.of(ILLEGAL_STATE);
        }

        @Override
        protected void receiveFail(Response.Fail fromDownstream) {
            throw TypeDBException.of(ILLEGAL_STATE);
        }

        @Override
        protected void receiveBlocked(Response.Blocked fromDownstream) {
            throw TypeDBException.of(ILLEGAL_STATE);
        }

        private BoundConcludableResolutionState<?> getOrCreateResolutionState(Request.Visit fromUpstream) {
            return resolutionStates.computeIfAbsent(fromUpstream.partialAnswer().asConcludable(),
                                                    partial -> createResolutionState(fromUpstream.partialAnswer()));
        }

        @Override
        public void terminate(Throwable cause) {
            super.terminate(cause);
            resolutionStates.clear();
        }

    }

    public static class BoundConcludableContext {

        private final Concludable concludable;
        private final Map<Driver<ConclusionResolver>, Set<Unifier>> conclusionUnifiers;

        BoundConcludableContext(Concludable concludable,
                                Map<Driver<ConclusionResolver>, Set<Unifier>> conclusionResolvers) {
            this.concludable = concludable;
            this.conclusionUnifiers = conclusionResolvers;
        }

        public Concludable concludable() {
            return concludable;
        }

        public Map<Driver<ConclusionResolver>, Set<Unifier>> conclusionResolvers() {
            return conclusionUnifiers;
        }
    }

}
