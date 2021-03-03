/*
 * Copyright (C) 2021 Grakn Labs
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

package grakn.core.reasoner.resolution.resolver;

import grakn.core.common.exception.GraknException;
import grakn.core.concept.ConceptManager;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.logic.resolvable.Negated;
import grakn.core.pattern.Disjunction;
import grakn.core.reasoner.resolution.ResolutionRecorder;
import grakn.core.reasoner.resolution.ResolverRegistry;
import grakn.core.reasoner.resolution.answer.AnswerState.Partial;
import grakn.core.reasoner.resolution.answer.AnswerState.Partial.Filtered;
import grakn.core.reasoner.resolution.framework.Request;
import grakn.core.reasoner.resolution.framework.Resolver;
import grakn.core.traversal.TraversalEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;

public class NegationResolver extends Resolver<NegationResolver> {

    private static final Logger LOG = LoggerFactory.getLogger(NegationResolver.class);

    private final Driver<ResolutionRecorder> resolutionRecorder;
    private final Negated negated;
    private final Map<ConceptMap, RequestState> requestStates;
    private boolean isInitialised;
    private Driver<? extends Resolver<?>> downstream;

    public NegationResolver(Driver<NegationResolver> driver, Negated negated, ResolverRegistry registry,
                            TraversalEngine traversalEngine, ConceptManager conceptMgr,
                            Driver<ResolutionRecorder> resolutionRecorder, boolean resolutionTracing) {
        super(driver, NegationResolver.class.getSimpleName() + "(pattern: " + negated.pattern() + ")",
              registry, traversalEngine, conceptMgr, resolutionTracing);
        this.negated = negated;
        this.resolutionRecorder = resolutionRecorder;
        this.requestStates = new HashMap<>();
        this.isInitialised = false;
    }

    @Override
    public void receiveRequest(Request fromUpstream, int iteration) {
        LOG.trace("{}: received Request: {}", name(), fromUpstream);
        if (!isInitialised) initialiseDownstreamResolvers();
        if (isTerminated()) return;

        RequestState requestState = requestStates.computeIfAbsent(fromUpstream.partialAnswer().conceptMap(),
                                                                  (cm) -> new RequestState());
        if (requestState.status.isEmpty()) {
            requestState.addAwaiting(fromUpstream, iteration);
            tryAnswer(fromUpstream, requestState);
        } else if (requestState.status.isRequested()) {
            requestState.addAwaiting(fromUpstream, iteration);
        } else if (requestState.status.isSatisfied()) {
            answerToUpstream(upstreamAnswer(fromUpstream), fromUpstream, iteration);
        } else if (requestState.status.isFailed()) {
            failToUpstream(fromUpstream, iteration);
        } else {
            throw GraknException.of(ILLEGAL_STATE);
        }
    }

    @Override
    protected void initialiseDownstreamResolvers() {
        LOG.debug("{}: initialising downstream resolvers", name());

        Disjunction disjunction = negated.pattern();
        if (disjunction.conjunctions().size() == 1) {
            try {
                downstream = registry.nested(disjunction.conjunctions().get(0));
            } catch (GraknException e) {
                terminate(e);
            }
        } else {
            downstream = registry.nested(disjunction);
        }
        isInitialised = true;
    }

    private void tryAnswer(Request fromUpstream, RequestState requestState) {
        // TODO: if we wanted to accelerate the searching of a negation counter example,
        // TODO: we could send multiple requests into the sub system at once!

        /*
        NOTE:
           Correctness: concludables that get reused in the negated portion, would conflate recursion/reiteration state from
              the toplevel root with the negation iterations, which we cannot allow. So, we must use THIS resolver
              as a sort of new root!
        */
        Filtered downstreamPartial = fromUpstream.partialAnswer().filterToDownstream(negated.retrieves(), downstream);
        Request request = Request.create(driver(), this.downstream, downstreamPartial);
        requestFromDownstream(request, fromUpstream, 0);
        requestState.setRequested();
    }

    @Override
    protected void receiveAnswer(grakn.core.reasoner.resolution.framework.Response.Answer fromDownstream, int iteration) {
        LOG.trace("{}: received Answer: {}, therefore is FAILED", name(), fromDownstream);
        if (isTerminated()) return;

        Request toDownstream = fromDownstream.sourceRequest();
        Request fromUpstream = fromUpstream(toDownstream);
        RequestState requestState = this.requestStates.get(fromUpstream.partialAnswer().conceptMap());
        requestState.setFailed();
        for (RequestState.Awaiting awaiting : requestState.awaiting) {
            failToUpstream(awaiting.request, awaiting.iterationRequested);
        }
        requestState.clearAwaiting();
    }

    @Override
    protected void receiveFail(grakn.core.reasoner.resolution.framework.Response.Fail fromDownstream, int iteration) {
        LOG.trace("{}: Receiving Failed: {}, therefore is SATISFIED", name(), fromDownstream);
        if (isTerminated()) return;

        Request toDownstream = fromDownstream.sourceRequest();
        Request fromUpstream = fromUpstream(toDownstream);
        RequestState requestState = this.requestStates.get(fromUpstream.partialAnswer().conceptMap());

        requestState.setSatisfied();
        for (RequestState.Awaiting awaiting : requestState.awaiting) {
            answerToUpstream(upstreamAnswer(awaiting.request), awaiting.request, awaiting.iterationRequested);
        }
        requestState.clearAwaiting();
    }

    private Partial<?> upstreamAnswer(Request fromUpstream) {
        assert fromUpstream.partialAnswer().isFiltered();
        Partial<?> upstreamAnswer = fromUpstream.partialAnswer().asFiltered().toUpstream();

        if (fromUpstream.partialAnswer().recordExplanations()) {
            resolutionRecorder.execute(state -> state.record(fromUpstream.partialAnswer()));
        }
        return upstreamAnswer;
    }

    private static class RequestState {

        List<Awaiting> awaiting;
        Status status;

        public RequestState() {
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
