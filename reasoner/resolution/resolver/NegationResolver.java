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
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concurrent.actor.Actor;
import grakn.core.logic.resolvable.Negated;
import grakn.core.pattern.Conjunction;
import grakn.core.reasoner.resolution.ResolutionRecorder;
import grakn.core.reasoner.resolution.ResolverRegistry;
import grakn.core.reasoner.resolution.answer.AnswerState.Partial;
import grakn.core.reasoner.resolution.answer.AnswerState.Partial.Filtered;
import grakn.core.reasoner.resolution.framework.Request;
import grakn.core.reasoner.resolution.framework.Resolver;
import grakn.core.reasoner.resolution.framework.Response;
import grakn.core.reasoner.resolution.framework.ResponseProducer;
import grakn.core.traversal.TraversalEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.common.exception.ErrorMessage.Internal.UNIMPLEMENTED;

public class NegationResolver extends Resolver<NegationResolver> {

    private static final Logger LOG = LoggerFactory.getLogger(NegationResolver.class);

    private final Actor<ResolutionRecorder> resolutionRecorder;
    private final Negated negated;
    private final Map<ConceptMap, NegationResponse> responses;
    private boolean isInitialised;
    private Actor<? extends Resolver<?>> downstream;

    public NegationResolver(Actor<NegationResolver> self, Negated negated, ResolverRegistry registry,
                            TraversalEngine traversalEngine, Actor<ResolutionRecorder> resolutionRecorder,
                            boolean explanations) {
        super(self, NegationResolver.class.getSimpleName() + "(pattern: " + negated.pattern() + ")",
              registry, traversalEngine, explanations);
        this.negated = negated;
        this.resolutionRecorder = resolutionRecorder;
        this.responses = new HashMap<>();
        this.isInitialised = false;
    }

    @Override
    protected void initialiseDownstreamActors() {
        LOG.debug("{}: initialising downstream actors", name());

        List<Conjunction> disjunction = negated.pattern().conjunctions();
        if (disjunction.size() == 1) {
            downstream = registry.conjunction(disjunction.get(0));
        } else {
            // negations with complex disjunctions not yet working
            throw GraknException.of(UNIMPLEMENTED);
        }
    }

    @Override
    public void receiveRequest(Request fromUpstream, int iteration) {
        LOG.trace("{}: received Request: {}", name(), fromUpstream);

        if (!isInitialised) {
            initialiseDownstreamActors();
            isInitialised = true;
        }

        NegationResponse negationResponse = getOrInitialise(fromUpstream.partialAnswer().conceptMap());
        if (negationResponse.status.isEmpty()) {
            negationResponse.addAwaiting(fromUpstream, iteration);
            tryAnswer(fromUpstream, negationResponse);
        } else if (negationResponse.status.isRequested()) {
            negationResponse.addAwaiting(fromUpstream, iteration);
        } else if (negationResponse.status.isSatisfied()) {
            answerToUpstream(upstreamAnswer(fromUpstream), fromUpstream, iteration);
        } else if (negationResponse.status.isFailed()) {
            failToUpstream(fromUpstream, iteration);
        } else {
            throw GraknException.of(ILLEGAL_STATE);
        }
    }

    private NegationResponse getOrInitialise(ConceptMap conceptMap) {
        return responses.computeIfAbsent(conceptMap, (cm) -> new NegationResponse());
    }

    private void tryAnswer(Request fromUpstream, NegationResponse negationResponse) {
        // TODO: if we wanted to accelerate the searching of a negation counter example,
        // TODO: we could send multiple requests into the sub system at once!

        /*
        NOTE:
           Correctness: concludables that get reused in the negated portion, would conflate recursion/reiteration state from
              the toplevel root with the negation iterations, which we cannot allow. So, we must use THIS resolver
              as a sort of new root! TODO: should NegationResolvers also implement a kind of Root interface??
        */
        Filtered downstream = fromUpstream.partialAnswer().filterToDownstream(negated.variableNames());
        Request request = Request.create(new Request.Path(self(), downstream).append(this.downstream, downstream),
                                         downstream);
        requestFromDownstream(request, fromUpstream, 0);
        negationResponse.setRequested();
    }

    @Override
    protected void receiveAnswer(Response.Answer fromDownstream, int iteration) {
        LOG.trace("{}: received Answer: {}, therefore is FAILED", name(), fromDownstream);

        Request toDownstream = fromDownstream.sourceRequest();
        Request fromUpstream = fromUpstream(toDownstream);
        NegationResponse negationResponse = this.responses.get(fromUpstream.partialAnswer().conceptMap());
        negationResponse.setFailed();
        for (NegationResponse.Awaiting awaiting : negationResponse.awaiting) {
            failToUpstream(awaiting.request, awaiting.iterationRequested);
        }
        negationResponse.clearAwaiting();
    }

    @Override
    protected void receiveExhausted(Response.Fail fromDownstream, int iteration) {
        LOG.trace("{}: Receiving Exhausted: {}, therefore is SATISFIED", name(), fromDownstream);

        Request toDownstream = fromDownstream.sourceRequest();
        Request fromUpstream = fromUpstream(toDownstream);
        NegationResponse negationResponse = this.responses.get(fromUpstream.partialAnswer().conceptMap());

        negationResponse.setSatisfied();
        for (NegationResponse.Awaiting awaiting : negationResponse.awaiting) {
            answerToUpstream(upstreamAnswer(awaiting.request), awaiting.request, awaiting.iterationRequested);
        }
        negationResponse.clearAwaiting();
    }

    private Partial<?> upstreamAnswer(Request fromUpstream) {

        if (fromUpstream.partialAnswer().recordExplanations()) {
            // TODO Check this
            resolutionRecorder.tell(state -> state.record(fromUpstream.partialAnswer()));
        }
        // TODO: decide if we want to use isMapped here? Can Mapped currently act as a filter?
        assert fromUpstream.partialAnswer().isMapped();
        return fromUpstream.partialAnswer().asMapped().toUpstream(self());
    }

    @Override
    protected ResponseProducer responseProducerCreate(Request fromUpstream, int iteration) {
        throw GraknException.of(ILLEGAL_STATE);
    }

    @Override
    protected ResponseProducer responseProducerReiterate(Request fromUpstream, ResponseProducer responseProducer, int newIteration) {
        throw GraknException.of(ILLEGAL_STATE);
    }

    @Override
    protected void exception(Throwable e) {
        LOG.error("Actor exception", e);
        // TODO, once integrated into the larger flow of executing queries, kill the actors and report and exception to root
    }

    private static class NegationResponse {

        List<Awaiting> awaiting;
        Status status;

        public NegationResponse() {
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
