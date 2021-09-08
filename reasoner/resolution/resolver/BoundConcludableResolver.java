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
import com.vaticle.typedb.core.reasoner.resolution.framework.Request;
import com.vaticle.typedb.core.reasoner.resolution.framework.Resolver;
import com.vaticle.typedb.core.reasoner.resolution.framework.Response;
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
    private final Driver<ConcludableResolver> parent;
    private final Map<Request.Visit, ExploringRequestState<?>> requestStates;

    public BoundConcludableResolver(Driver<BoundConcludableResolver> driver, Driver<ConcludableResolver> parent,
                                    ConceptMap bounds, ResolverRegistry registry) {
        super(driver, BoundConcludableResolver.class.getSimpleName() + "(pattern: " +
                parent.actor().concludable().pattern() + ", bounds: " + bounds.concepts().toString() + ")", registry);
        this.parent = parent;
        this.bounds = bounds;
        this.requestStates = new HashMap<>();
    }

    protected Driver<ConcludableResolver> parent() {
        return parent;
    }

    @Override
    public void receiveVisit(Request.Visit fromUpstream) {
        LOG.trace("{}: received Visit: {}", name(), fromUpstream);
        if (isTerminated()) return;
        assert fromUpstream.partialAnswer().isConcludable();
        ExploringRequestState<?> requestState = requestStates.computeIfAbsent(
                fromUpstream, request -> createExploringRequestState(fromUpstream));
        Optional<Partial.Compound<?, ?>> upstreamAnswer = upstreamAnswer(requestState);
        if (upstreamAnswer.isPresent()) {
            answerToUpstream(upstreamAnswer.get(), fromUpstream, requestState);
        } else if (cache().isComplete()) {
            failToUpstream(fromUpstream);
        } else if (isCycle(fromUpstream.partialAnswer()).isPresent()) { // TODO: We can cache this on the requestState
            cycleToUpstream(fromUpstream, cache().size());
        } else if (requestState.downstreamManager().hasNextVisit()) {
            visitDownstream(requestState.downstreamManager().nextVisit(fromUpstream), fromUpstream);
        } else if (requestState.downstreamManager().hasNextRevisit()) {
            revisitDownstream(requestState.downstreamManager().nextRevisit(fromUpstream), fromUpstream);
        } else if (requestState.downstreamManager().allDownstreamsCycleToHereOnly()) {
            cache().setComplete();
            failToUpstream(fromUpstream);
        } else {
            cycleToUpstream(fromUpstream, requestState.downstreamManager().cyclesNotOriginatingHere());
        }
    }

    @Override
    protected void receiveRevisit(Request.Revisit fromUpstream) {
        assert fromUpstream.visit().partialAnswer().isConcludable();
        ExploringRequestState<?> requestState = requestStates.get(fromUpstream.visit());
        Optional<Partial.Compound<?, ?>> upstreamAnswer = upstreamAnswer(requestState);
        requestState.downstreamManager().unblock(fromUpstream.cycles());
        if (upstreamAnswer.isPresent()) {
            answerToUpstream(upstreamAnswer.get(), fromUpstream.visit(), requestState);
        } else if (cache().isComplete()) {
            failToUpstream(fromUpstream.visit());
        } else if (isCycle(fromUpstream.visit().partialAnswer()).isPresent()) {
            cycleToUpstream(fromUpstream.visit(), cache().size());
        } else if (requestState.downstreamManager().hasNextVisit()) {
            visitDownstream(requestState.downstreamManager().nextVisit(fromUpstream.visit()), fromUpstream.visit());
        } else if (requestState.downstreamManager().hasNextRevisit()) {
            revisitDownstream(requestState.downstreamManager().nextRevisit(fromUpstream.visit()), fromUpstream.visit());
        } else if (requestState.downstreamManager().allDownstreamsCycleToHereOnly()) {
            cache().setComplete();
            failToUpstream(fromUpstream.visit());
        } else {
            cycleToUpstream(fromUpstream.visit(), requestState.downstreamManager().cyclesNotOriginatingHere());
        }
    }

    private Optional<Partial.Concludable<?>> isCycle(Partial<?> partialAnswer) {
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

    abstract ExploringRequestState<?> createExploringRequestState(Request.Visit fromUpstream);

    @Override
    protected void receiveAnswer(Response.Answer fromDownstream) {
        if (isTerminated()) return;
        Request.Visit fromUpstream = fromUpstream(fromDownstream.sourceRequest());
        ExploringRequestState<?> requestState = this.requestStates.get(fromUpstream);
        if (requestState.newAnswer(fromDownstream.answer())) requestState.downstreamManager().unblockOutdated(); // TODO: This needs to trigger the creation of a Revisit Request somehow.
        answerUpstreamOrSearchDownstreamOrFail(fromUpstream, requestState);
    }

    @Override
    protected void receiveFail(Response.Fail fromDownstream) {
        LOG.trace("{}: received Fail: {}", name(), fromDownstream);
        if (isTerminated()) return;

        Request.Visit toDownstream = fromDownstream.sourceRequest();
        Request.Visit fromUpstream = fromUpstream(toDownstream);

        ExploringRequestState<?> requestState = this.requestStates.get(fromUpstream);
        requestState.downstreamManager().remove(Downstream.of(fromDownstream.sourceRequest()));
        answerUpstreamOrSearchDownstreamOrFail(fromUpstream, requestState);
    }

    @Override
    protected void receiveCycle(Response.Cycle fromDownstream) {
        LOG.trace("{}: received Cycle: {}", name(), fromDownstream);
        if (isTerminated()) return;

        Downstream cyclingDownstream = Downstream.of(fromDownstream.sourceRequest());
        Request.Visit fromUpstream = fromUpstream(fromDownstream.sourceRequest());

        ExploringRequestState<?> requestState = this.requestStates.get(fromUpstream);
        if (requestState.downstreamManager().contains(cyclingDownstream)) {
            requestState.downstreamManager().block(cyclingDownstream, fromDownstream.origins());
            requestState.downstreamManager().unblockOutdated();
        }
        answerUpstreamOrSearchDownstreamOrFail(fromUpstream, requestState);
    }

    private void answerUpstreamOrSearchDownstreamOrFail(Request.Visit fromUpstream, ExploringRequestState<?> requestState) {
        Optional<Partial.Compound<?, ?>> upstreamAnswer = upstreamAnswer(requestState);
        if (upstreamAnswer.isPresent()) {
            answerToUpstream(upstreamAnswer.get(), fromUpstream, requestState);
        } else if (cache().isComplete()) {
            failToUpstream(fromUpstream);
        } else if (requestState.downstreamManager().hasNextVisit()) {
            visitDownstream(requestState.downstreamManager().nextVisit(fromUpstream), fromUpstream);
        } else if (requestState.downstreamManager().hasNextRevisit()) {
            revisitDownstream(requestState.downstreamManager().nextRevisit(fromUpstream), fromUpstream);
        } else if (requestState.downstreamManager().allDownstreamsCycleToHereOnly()) {
            cache().setComplete();
            failToUpstream(fromUpstream);
        } else {
            cycleToUpstream(fromUpstream, requestState.downstreamManager().cyclesNotOriginatingHere());
        }
    }

    private static Optional<Partial.Compound<?, ?>> upstreamAnswer(ExploringRequestState<?> requestState) {
        return requestState.nextAnswer().map(Partial::asCompound);
    }

    protected void answerToUpstream(AnswerState answer, Request.Visit fromUpstream, ExploringRequestState<?> requestState) {
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
        answerToUpstream(answer, fromUpstream);
    }

    private void requestFromSubsumer(Request.Visit.ToSubsumed fromUpstream) {
        Request.Visit toSubsumer = Request.Visit.ToSubsumer.create(driver(), fromUpstream.subsumer(),
                                                                   fromUpstream, fromUpstream.partialAnswer());
        visitDownstream(toSubsumer, fromUpstream);
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

    protected List<Downstream> ruleDownstreams(Request.Visit fromUpstream) {
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

        protected ExploringRequestState(Request.Visit fromUpstream, AnswerCache<ANSWER> answerCache,
                                        List<Downstream> ruleDownstreams, boolean deduplicate) {
            super(fromUpstream, answerCache, deduplicate);
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

        class ConcludableDownstreamManager extends DownstreamManager {

            public ConcludableDownstreamManager(List<Downstream> ruleDownstreams) {
                super(ruleDownstreams);
            }

            public void unblockOutdated() {
                Set<Response.Cycle.Origin> outdated =
                        iterate(blocked.values()).flatMap(origins -> iterate(origins).filter(this::isOutdated)).toSet();
                if (!outdated.isEmpty()) unblock(outdated);
            }

            private boolean originatedHere(Response.Cycle.Origin cycleOrigin) {
                return cycleOrigin.sender().equals(driver());
            }

            public boolean isOutdated(Response.Cycle.Origin cycleOrigin) {
                return originatedHere(cycleOrigin) && cycleOrigin.numAnswersSeen() < cache().size();
            }

            public Set<Response.Cycle.Origin> cyclesNotOriginatingHere() {
                return iterate(blocked.values()).flatMap(Iterators::iterate).filter(o -> !originatedHere(o)).toSet();
            }

            public boolean allDownstreamsCycleToHereOnly() {
                return iterate(blocked.values())
                        .filter(cycleOrigins -> iterate(cycleOrigins)
                                .filter(o -> !originatedHere(o))
                                .first()
                                .isPresent())
                        .first()
                        .isEmpty();
            }
        }
    }
}
