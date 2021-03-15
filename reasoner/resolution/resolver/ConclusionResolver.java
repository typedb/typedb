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
import grakn.core.common.iterator.FunctionalIterator;
import grakn.core.concept.Concept;
import grakn.core.concept.ConceptManager;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.logic.Rule;
import grakn.core.reasoner.resolution.ResolverRegistry;
import grakn.core.reasoner.resolution.answer.AnswerState.Partial;
import grakn.core.reasoner.resolution.framework.Request;
import grakn.core.reasoner.resolution.framework.Resolver;
import grakn.core.reasoner.resolution.framework.Response;
import grakn.core.traversal.Traversal;
import grakn.core.traversal.TraversalEngine;
import grakn.core.traversal.common.Identifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.common.iterator.Iterators.iterate;

public class ConclusionResolver extends Resolver<ConclusionResolver> {

    private static final Logger LOG = LoggerFactory.getLogger(ConclusionResolver.class);

    private final Rule.Conclusion conclusion;
    private final Map<Request, RequestState> requestStates;
    private Driver<ConditionResolver> ruleResolver;
    private boolean isInitialised;

    public ConclusionResolver(Driver<ConclusionResolver> driver, Rule.Conclusion conclusion, ResolverRegistry registry,
                              TraversalEngine traversalEngine, ConceptManager conceptMgr, boolean resolutionTracing) {
        super(driver, ConclusionResolver.class.getSimpleName() + "(" + conclusion + ")",
              registry, traversalEngine, conceptMgr, resolutionTracing);
        this.conclusion = conclusion;
        this.requestStates = new HashMap<>();
        this.isInitialised = false;
    }

    @Override
    public void receiveRequest(Request fromUpstream, int iteration) {
        LOG.trace("{}: received Request: {}", name(), fromUpstream);
        if (!isInitialised) initialiseDownstreamResolvers();
        if (isTerminated()) return;

        RequestState requestState = getOrUpdateRequestState(fromUpstream, iteration);

        if (iteration < requestState.iteration()) {
            // short circuit if the request came from a prior iteration
            failToUpstream(fromUpstream, iteration);
        } else {
            assert iteration == requestState.iteration();
            nextAnswer(fromUpstream, requestState, iteration);
        }
    }

    @Override
    protected void receiveAnswer(Response.Answer fromDownstream, int iteration) {
        LOG.trace("{}: received Answer: {}", name(), fromDownstream);
        if (isTerminated()) return;

        Request toDownstream = fromDownstream.sourceRequest();
        Request fromUpstream = fromUpstream(toDownstream);
        RequestState requestState = this.requestStates.get(fromUpstream);

        FunctionalIterator<Map<Identifier.Variable, Concept>> materialisations = conclusion
                .materialise(fromDownstream.answer().conceptMap(), traversalEngine, conceptMgr);
        if (!materialisations.hasNext()) throw GraknException.of(ILLEGAL_STATE);

        FunctionalIterator<Partial.Concludable<?, ?>> materialisedAnswers = materialisations
                .map(concepts -> fromDownstream.answer().asConclusion().aggregateToUpstream(concepts))
                .filter(Optional::isPresent)
                .map(Optional::get);
        requestState.addResponses(materialisedAnswers);
        nextAnswer(fromUpstream, requestState, iteration);
    }

    @Override
    protected void receiveFail(Response.Fail fromDownstream, int iteration) {
        LOG.trace("{}: received Fail: {}", name(), fromDownstream);
        if (isTerminated()) return;

        Request toDownstream = fromDownstream.sourceRequest();
        Request fromUpstream = fromUpstream(toDownstream);
        RequestState requestState = this.requestStates.get(fromUpstream);

        if (iteration < requestState.iteration()) {
            // short circuit old iteration fail messages to upstream
            failToUpstream(fromUpstream, iteration);
            return;
        }

        requestState.removeDownstream(fromDownstream.sourceRequest());
        nextAnswer(fromUpstream, requestState, iteration);
    }

    @Override
    public void terminate(Throwable cause) {
        super.terminate(cause);
        requestStates.clear();
    }

    @Override
    protected void initialiseDownstreamResolvers() {
        LOG.debug("{}: initialising downstream resolvers", name());
        try {
            ruleResolver = registry.registerCondition(conclusion.rule().condition());
            isInitialised = true;
        } catch (GraknException e) {
            terminate(e);
        }
    }

    private void nextAnswer(Request fromUpstream, RequestState requestState, int iteration) {
        Optional<Partial.Concludable<?, ?>> upstreamAnswer = requestState.nextResponse();
        if (upstreamAnswer.isPresent()) {
            answerToUpstream(upstreamAnswer.get(), fromUpstream, iteration);
        } else if (requestState.hasDownstream()) {
            requestFromDownstream(requestState.nextDownstream(), fromUpstream, iteration);
        } else {
            failToUpstream(fromUpstream, iteration);
        }
    }

    private RequestState getOrUpdateRequestState(Request fromUpstream, int iteration) {
        if (!requestStates.containsKey(fromUpstream)) {
            requestStates.put(fromUpstream, createRequestState(fromUpstream, iteration));
        } else {
            RequestState requestState = this.requestStates.get(fromUpstream);
            assert requestState.iteration() == iteration || requestState.iteration() + 1 == iteration;

            if (requestState.iteration() + 1 == iteration) {
                // when the same request for the next iteration the first time, re-initialise required state
                RequestState requestStateNextIter = createRequestState(fromUpstream, iteration);
                this.requestStates.put(fromUpstream, requestStateNextIter);
            }
        }
        return requestStates.get(fromUpstream);
    }

    private RequestState createRequestState(Request fromUpstream, int iteration) {
        LOG.debug("{}: Creating a new ConclusionResponse for request: {}", name(), fromUpstream);

        RequestState requestState = new RequestState(iteration);

        assert fromUpstream.partialAnswer().isConclusion();
        Partial.Conclusion<?, ?> partialAnswer = fromUpstream.partialAnswer().asConclusion();
        // we do a extra traversal to expand the partial answer if we already have the concept that is meant to be generated
        // and if there's extra variables to be populated
        assert conclusion.retrievableIds().containsAll(partialAnswer.conceptMap().concepts().keySet());
        if (conclusion.generating().isPresent() && conclusion.retrievableIds().size() > partialAnswer.conceptMap().concepts().size() &&
                partialAnswer.conceptMap().concepts().containsKey(conclusion.generating().get().id())) {
            FunctionalIterator<Partial.Compound.Condition<?>> completedDownstreamAnswers = candidateAnswers(partialAnswer);
            completedDownstreamAnswers.forEachRemaining(answer -> requestState.addDownstream(Request.create(driver(), ruleResolver,
                                                                                                            answer)));
        } else {
            Set<Identifier.Variable.Retrievable> named = iterate(conclusion.retrievableIds()).filter(Identifier::isName).toSet();
            Partial.Compound.Condition<?> downstreamAnswer = partialAnswer.toDownstream(named);
            requestState.addDownstream(Request.create(driver(), ruleResolver, downstreamAnswer));
        }

        return requestState;
    }

    private FunctionalIterator<Partial.Compound.Condition<?>> candidateAnswers(Partial.Conclusion<?, ?> partialAnswer) {
        Traversal traversal = boundTraversal(conclusion.conjunction().traversal(), partialAnswer.conceptMap());
        FunctionalIterator<ConceptMap> answers = traversalEngine.iterator(traversal).map(conceptMgr::conceptMap);
        Set<Identifier.Variable.Retrievable> named = iterate(conclusion.retrievableIds()).filter(Identifier::isName).toSet();
        return answers.map(ans -> partialAnswer.extend(ans).toDownstream(named));
    }

    @Override
    public String toString() {
        return name();
    }

    private static class RequestState {

        private final List<FunctionalIterator<Partial.Concludable<?, ?>>> materialisedAnswers;
        private final LinkedHashSet<Request> downstreams;
        private Iterator<Request> downstreamProducerSelector;
        private final int iteration;

        public RequestState(int iteration) {
            this.materialisedAnswers = new LinkedList<>();
            this.downstreams = new LinkedHashSet<>();
            this.iteration = iteration;
            downstreamProducerSelector = downstreams.iterator();
        }

        public int iteration() {
            return iteration;
        }

        public void addResponses(FunctionalIterator<Partial.Concludable<?, ?>> materialisations) {
            materialisedAnswers.add(materialisations);
        }

        public Optional<Partial.Concludable<?, ?>> nextResponse() {
            if (hasResponse()) return Optional.of(materialisedAnswers.get(0).next());
            else return Optional.empty();
        }

        private boolean hasResponse() {
            while (!materialisedAnswers.isEmpty() && !materialisedAnswers.get(0).hasNext()) {
                materialisedAnswers.remove(0);
            }
            return !materialisedAnswers.isEmpty();
        }

        public boolean hasDownstream() {
            return !downstreams.isEmpty();
        }

        public Request nextDownstream() {
            if (!downstreamProducerSelector.hasNext()) downstreamProducerSelector = downstreams.iterator();
            return downstreamProducerSelector.next();
        }

        public void removeDownstream(Request request) {
            boolean removed = downstreams.remove(request);
            // only update the iterator when removing an element, to avoid resetting and reusing first request too often
            // note: this is a large performance win when processing large batches of requests
            if (removed) downstreamProducerSelector = downstreams.iterator();
        }

        public void addDownstream(Request request) {
            assert !(downstreams.contains(request)) : "downstream answer producer already contains this request";
            downstreams.add(request);
            downstreamProducerSelector = downstreams.iterator();
        }
    }
}
