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
import com.vaticle.typedb.core.logic.resolvable.Negated;
import com.vaticle.typedb.core.pattern.Disjunction;
import com.vaticle.typedb.core.reasoner.resolution.ResolverRegistry;
import com.vaticle.typedb.core.reasoner.resolution.answer.AnswerState.Partial;
import com.vaticle.typedb.core.reasoner.resolution.answer.AnswerState.Partial.Compound;
import com.vaticle.typedb.core.reasoner.resolution.framework.Request;
import com.vaticle.typedb.core.reasoner.resolution.framework.Resolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;

public class NegationResolver extends Resolver<NegationResolver> {

    private static final Logger LOG = LoggerFactory.getLogger(NegationResolver.class);

    private final Negated negated;
    private final Map<ConceptMap, BoundsState> boundsStates;
    private boolean isInitialised;
    private Driver<? extends Resolver<?>> downstream;

    public NegationResolver(Driver<NegationResolver> driver, Negated negated, ResolverRegistry registry) {
        super(driver, NegationResolver.class.getSimpleName() + "(pattern: " + negated.pattern() + ")", registry);
        this.negated = negated;
        this.boundsStates = new HashMap<>();
        this.isInitialised = false;
    }

    @Override
    public void receiveRequest(Request fromUpstream, int iteration) {
        LOG.trace("{}: received Request: {}", name(), fromUpstream);
        if (!isInitialised) initialiseDownstreamResolvers();
        if (isTerminated()) return;

        BoundsState boundsState = this.boundsStates.computeIfAbsent(fromUpstream.partialAnswer().conceptMap(),
                                                                    (cm) -> new BoundsState());
        if (boundsState.status.isEmpty()) {
            boundsState.addAwaiting(fromUpstream, iteration);
            tryAnswer(fromUpstream, boundsState);
        } else if (boundsState.status.isRequested()) {
            boundsState.addAwaiting(fromUpstream, iteration);
        } else if (boundsState.status.isSatisfied()) {
            answerToUpstream(upstreamAnswer(fromUpstream), fromUpstream, iteration);
        } else if (boundsState.status.isFailed()) {
            failToUpstream(fromUpstream, iteration);
        } else {
            throw TypeDBException.of(ILLEGAL_STATE);
        }
    }

    @Override
    protected void initialiseDownstreamResolvers() {
        LOG.debug("{}: initialising downstream resolvers", name());

        Disjunction disjunction = negated.pattern();
        if (disjunction.conjunctions().size() == 1) {
            try {
                downstream = registry.nested(disjunction.conjunctions().get(0));
            } catch (TypeDBException e) {
                terminate(e);
            }
        } else {
            downstream = registry.nested(disjunction);
        }
        isInitialised = true;
    }

    private void tryAnswer(Request fromUpstream, BoundsState boundsState) {
        // TODO: if we wanted to accelerate the searching of a negation counter example, we could send multiple
        //  requests into the sub system at once!

        /*
        NOTE:
           Correctness: concludables that get reused in the negated portion, would conflate recursion/reiteration state from
              the toplevel root with the negation iterations, which we cannot allow. So, we must use THIS resolver
              as a sort of new root!
        */
        assert fromUpstream.partialAnswer().isCompound();
        Compound.Nestable downstreamPartial = fromUpstream.partialAnswer().asCompound().filterToNestable(negated.retrieves());
        Request request = Request.create(driver(), this.downstream, downstreamPartial);
        requestFromDownstream(request, fromUpstream, 0);
        boundsState.setRequested();
    }

    @Override
    protected void receiveAnswer(com.vaticle.typedb.core.reasoner.resolution.framework.Response.Answer fromDownstream, int iteration) {
        LOG.trace("{}: received Answer: {}, therefore is FAILED", name(), fromDownstream);
        if (isTerminated()) return;

        Request toDownstream = fromDownstream.sourceRequest();
        Request fromUpstream = fromUpstream(toDownstream);
        BoundsState boundsState = this.boundsStates.get(fromUpstream.partialAnswer().conceptMap());
        boundsState.setFailed();
        for (BoundsState.Awaiting awaiting : boundsState.awaiting) {
            failToUpstream(awaiting.request, awaiting.iterationRequested);
        }
        boundsState.clearAwaiting();
    }

    @Override
    protected void receiveFail(com.vaticle.typedb.core.reasoner.resolution.framework.Response.Fail fromDownstream, int iteration) {
        LOG.trace("{}: Receiving Failed: {}, therefore is SATISFIED", name(), fromDownstream);
        if (isTerminated()) return;

        Request toDownstream = fromDownstream.sourceRequest();
        Request fromUpstream = fromUpstream(toDownstream);
        BoundsState boundsState = this.boundsStates.get(fromUpstream.partialAnswer().conceptMap());

        boundsState.setSatisfied();
        for (BoundsState.Awaiting awaiting : boundsState.awaiting) {
            answerToUpstream(upstreamAnswer(awaiting.request), awaiting.request, awaiting.iterationRequested);
        }
        boundsState.clearAwaiting();
    }

    private Partial<?> upstreamAnswer(Request fromUpstream) {
        assert fromUpstream.partialAnswer().isCompound() && fromUpstream.partialAnswer().asCompound().isNestable();
        return fromUpstream.partialAnswer().asCompound().asNestable().toUpstream();
    }

    private static class BoundsState {

        final List<Awaiting> awaiting;
        Status status;

        public BoundsState() {
            this.awaiting = new LinkedList<>();
            this.status = Status.EMPTY;
        }

        public void addAwaiting(Request request, int iteration) {
            awaiting.add(new Awaiting(request, iteration));
        }

        public void setRequested() { this.status = Status.REQUESTED; }

        public void setFailed() { this.status = Status.FAILED; }

        public void setSatisfied() { this.status = Status.SATISFIED; }

        public void clearAwaiting() {
            awaiting.clear();
        }

        private static class Awaiting {
            final Request request;
            final int iterationRequested;

            public Awaiting(Request request, int iteration) {
                this.request = request;
                this.iterationRequested = iteration;
            }
        }

        enum Status {
            EMPTY,
            REQUESTED,
            FAILED,
            SATISFIED;

            public boolean isEmpty() { return this == EMPTY; }

            public boolean isRequested() { return this == REQUESTED; }

            public boolean isFailed() { return this == FAILED; }

            public boolean isSatisfied() { return this == SATISFIED; }
        }
    }
}
