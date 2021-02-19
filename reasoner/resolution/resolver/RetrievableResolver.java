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
 */

package grakn.core.reasoner.resolution.resolver;

import grakn.core.common.exception.GraknException;
import grakn.core.concept.ConceptManager;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concurrent.actor.Actor;
import grakn.core.concurrent.producer.Producer;
import grakn.core.logic.resolvable.Retrievable;
import grakn.core.reasoner.resolution.ResolverRegistry;
import grakn.core.reasoner.resolution.answer.AnswerState.Partial;
import grakn.core.reasoner.resolution.framework.Request;
import grakn.core.reasoner.resolution.framework.Resolver;
import grakn.core.reasoner.resolution.framework.Response;
import grakn.core.reasoner.resolution.framework.Response.Answer;
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
import static grakn.core.concurrent.common.Executors.asyncPool2;

public class RetrievableResolver extends Resolver<RetrievableResolver> {

    private static final int PREFETCH_SIZE = 32;
    private static final int REFETCH_THRESHOLD = PREFETCH_SIZE / 4;

    private static final Logger LOG = LoggerFactory.getLogger(RetrievableResolver.class);
    private final Retrievable retrievable;
    private final Map<Request, Responses> responses;

    public RetrievableResolver(Actor<RetrievableResolver> self, Retrievable retrievable, ResolverRegistry registry,
                               TraversalEngine traversalEngine, ConceptManager conceptMgr, boolean resolutionTracing) {
        super(self, RetrievableResolver.class.getSimpleName() + "(pattern: " + retrievable.pattern() + ")",
              registry, traversalEngine, conceptMgr, resolutionTracing);
        this.retrievable = retrievable;
        this.responses = new HashMap<>();
    }

    @Override
    public void receiveRequest(Request fromUpstream, int iteration) {
        LOG.trace("{}: received Request: {}", name(), fromUpstream);
        Responses responses = mayUpdateAndGetResponses(fromUpstream, iteration);
        if (iteration < responses.iteration()) {
            // short circuit old iteration exhausted messages to upstream
            failToUpstream(fromUpstream, iteration);
        } else {
            assert iteration == responses.iteration();
            responses.nextAnswer();
        }
    }

    @Override
    protected void receiveAnswer(Answer fromDownstream, int iteration) {
        throw GraknException.of(ILLEGAL_STATE);
    }

    @Override
    protected void receiveFail(Response.Fail fromDownstream, int iteration) {
        throw GraknException.of(ILLEGAL_STATE);
    }

    @Override
    protected void initialiseDownstreamResolvers() {
        throw GraknException.of(ILLEGAL_STATE);
    }

    Responses createResponses(Request fromUpstream, int iteration) {
        LOG.debug("{}: Creating a new Responses for request: {}", name(), fromUpstream);
        assert fromUpstream.partialAnswer().isMapped();

        Producer<ConceptMap> traversalAsync = traversalProducer(retrievable.pattern(), fromUpstream.partialAnswer().conceptMap(), 2);
        Producer<Partial<?>> upstreamAnswersAsync = traversalAsync
                .map(conceptMap -> fromUpstream.partialAnswer().asMapped().aggregateToUpstream(conceptMap, self()));

        return new Responses(fromUpstream, upstreamAnswersAsync, iteration);
    }

    Responses reiterateResponses(Request fromUpstream, Responses responsesPrevious, int newIteration) {
        assert newIteration > responsesPrevious.iteration();
        LOG.debug("{}: Updating Responses for iteration '{}'", name(), newIteration);

        assert newIteration > responsesPrevious.iteration();

        assert fromUpstream.partialAnswer().isMapped();
        Producer<ConceptMap> traversalAsync = traversalProducer(retrievable.pattern(), fromUpstream.partialAnswer().conceptMap(), 2);
        Producer<Partial<?>> upstreamAnswersAsync = traversalAsync
                .map(conceptMap -> fromUpstream.partialAnswer().asMapped().aggregateToUpstream(conceptMap, self()));

        return new Responses(fromUpstream, upstreamAnswersAsync, newIteration);
    }

    /*
    TODO clean up the following two when we remove the requirement for every resolver to have ResponseProducers
     */
    @Override
    protected ResponseProducer responseProducerCreate(Request fromUpstream, int iteration) {
        throw GraknException.of(UNIMPLEMENTED);
    }

    @Override
    protected ResponseProducer responseProducerReiterate(Request fromUpstream, ResponseProducer responseProducer, int newIteration) {
        throw GraknException.of(UNIMPLEMENTED);
    }

    private Responses mayUpdateAndGetResponses(Request fromUpstream, int iteration) {
        if (!responses.containsKey(fromUpstream)) {
            responses.put(fromUpstream, createResponses(fromUpstream, iteration));
        } else {
            Responses responses = this.responses.get(fromUpstream);
            assert iteration <= responses.iteration() + 1;

            if (responses.iteration() + 1 == iteration) {
                // when the same request for the next iteration the first time, re-initialise required state
                Responses responseProducerNextIter = reiterateResponses(fromUpstream, responses, iteration);
                this.responses.put(fromUpstream, responseProducerNextIter);
            }
        }
        return responses.get(fromUpstream);
    }

    public void receiveTraversalAnswer(Partial<?> upstreamAnswer, Request fromUpstreamSource) {
        Responses responses = this.responses.get(fromUpstreamSource);
        responses.decrementProcessing();
        if (responses.hasAwaitingRequest()) {
            dispatchAnswer(upstreamAnswer, responses.popAwaitingRequest());
            responses.mayFetch();
        } else {
            responses.addFetched(upstreamAnswer);
        }
    }

    public void receiveTraversalDone(Request fromUpstreamSource) {
        Responses responses = this.responses.get(fromUpstreamSource);
        responses.setTraversalDone();
        if (!responses.hasFetched()) {
            responses.finished();
        }
    }

    private void dispatchAnswer(Partial<?> upstreamAnswer, Request fromUpstreamSource) {
        answerToUpstream(upstreamAnswer, fromUpstreamSource, responses.get(fromUpstreamSource).iteration());
    }

    private void dispatchFail(Request fromUpstreamSource) {
        Responses responses = this.responses.get(fromUpstreamSource);
        failToUpstream(fromUpstreamSource, responses.iteration);
    }

    private class Responses {

        private final Request request;
        private final int iteration;
        private final Producer<Partial<?>> traversalUpstreamAnswers;
        private final List<Partial<?>> fetched;
        private final List<Request> awaitingAnswers;
        private int processing;
        private boolean traversalDone;

        public Responses(Request request, Producer<Partial<?>> traversalUpstreamAnswers, int iteration) {
            this.request = request;
            this.traversalUpstreamAnswers = traversalUpstreamAnswers;
            this.iteration = iteration;
            this.fetched = new LinkedList<>();
            this.awaitingAnswers = new LinkedList<>();
            this.processing = 0;
            this.traversalDone = false;
        }

        public int iteration() {
            return iteration;
        }

        public void setTraversalDone() {
            this.traversalDone = true;
            this.processing = 0;
        }

        public boolean hasAwaitingRequest() {
            return !awaitingAnswers.isEmpty();
        }

        public Request popAwaitingRequest() {
            return awaitingAnswers.remove(0);
        }

        public boolean hasFetched() {
            return !fetched.isEmpty();
        }

        public void addFetched(Partial<?> upstreamAnswer) {
            fetched.add(upstreamAnswer);
        }

        public void nextAnswer() {
            if (traversalDone && fetched.isEmpty()) {
                dispatchFail(request);
            } else {
                if (!fetched.isEmpty()) dispatchAnswer(fetched.remove(0), request);
                else awaitingAnswers.add(request);
                mayFetch();
            }
        }

        private void mayFetch() {
            // when buffered answers is mostly empty, trigger more processing
            if (!traversalDone && fetched.size() + processing < REFETCH_THRESHOLD) {
                int toFetch = PREFETCH_SIZE - fetched.size() - processing;
                this.traversalUpstreamAnswers.produce(new Queue(request), toFetch, asyncPool2());
                processing += toFetch;
            }
        }

        public void decrementProcessing() {
            processing--;
        }

        public void finished() {
            while (hasAwaitingRequest()) {
                dispatchFail(popAwaitingRequest());
            }
            assert traversalDone && awaitingAnswers.isEmpty() && fetched.isEmpty();
        }

        class Queue implements Producer.Queue<Partial<?>> {

            private final Request sourceRequest;

            Queue(Request sourceRequest) {
                this.sourceRequest = sourceRequest;
            }

            @Override
            public void put(Partial<?> upstreamAnswer) {
                self().tell(resolver -> resolver.receiveTraversalAnswer(upstreamAnswer, sourceRequest));
            }

            @Override
            public void done() {
                self().tell(resolver -> resolver.receiveTraversalDone(sourceRequest));
            }

            @Override
            public void done(Throwable e) {
                self().tell(resolver -> exception(e));
            }
        }

    }

}
