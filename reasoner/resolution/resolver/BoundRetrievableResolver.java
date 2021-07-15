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
 */

package com.vaticle.typedb.core.reasoner.resolution.resolver;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.iterator.Iterators;
import com.vaticle.typedb.core.concept.ConceptManager;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concept.type.AttributeType;
import com.vaticle.typedb.core.logic.resolvable.Retrievable;
import com.vaticle.typedb.core.reasoner.resolution.ResolverRegistry;
import com.vaticle.typedb.core.reasoner.resolution.answer.AnswerState;
import com.vaticle.typedb.core.reasoner.resolution.answer.AnswerState.Partial.Compound;
import com.vaticle.typedb.core.reasoner.resolution.framework.AnswerCache;
import com.vaticle.typedb.core.reasoner.resolution.framework.AnswerCache.SubsumptionAnswerCache.ConceptMapCache;
import com.vaticle.typedb.core.reasoner.resolution.framework.Request;
import com.vaticle.typedb.core.reasoner.resolution.framework.RequestState;
import com.vaticle.typedb.core.reasoner.resolution.framework.Resolver;
import com.vaticle.typedb.core.reasoner.resolution.framework.Response;
import com.vaticle.typedb.core.traversal.TraversalEngine;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;

public class BoundRetrievableResolver extends Resolver<BoundRetrievableResolver> {
    private final Retrievable retrievable;
    private final ConceptMap bounds;
    private final ConceptMapCache cache;
    private final Map<Request, BoundRetrievableRequestState> requestStates;
    private boolean traversalInitialised;
    private final Map<Request, Request> subsumptionRequests;

    public BoundRetrievableResolver(Retrievable retrievable, ConceptMap bounds, Driver<BoundRetrievableResolver> driver,
                                    ResolverRegistry registry, TraversalEngine traversalEngine,
                                    ConceptManager conceptMgr, boolean resolutionTracing) {
        super(driver, initName(retrievable, bounds), registry, traversalEngine, conceptMgr, resolutionTracing);
        this.retrievable = retrievable;
        this.bounds = bounds;
        this.cache = new ConceptMapCache(new HashMap<>(), this.bounds);
        this.requestStates = new HashMap<>();
        this.traversalInitialised = false;
        this.subsumptionRequests = new HashMap<>(); // TODO: Remove this map by storing the original request in the request/response sent/received for subsumption
    }

    private static String initName(Retrievable retrievable, ConceptMap bounds) {
        return BoundRetrievableResolver.class.getSimpleName() + "(pattern: " + retrievable.pattern() + " " +
                bounds.toString() + ")";
    }

    @Override
    public void receiveRequest(Request fromUpstream, int iteration) {
        // TODO: Problems:
        //  1. Passing from BoundConcludable to BoundConcludable means they try to aggregateToUpstream twice
        //  2. We need to continue from where the requeststate left off before subsumption was activated
        requestStates.computeIfAbsent(fromUpstream, request -> new BoundRetrievableRequestState(request, cache, iteration)); // TODO: Iteration shouldn't be needed
        if (fromUpstream.subsumer().isPresent() && !cache.isComplete()) {
            // TODO: Once we don't add the traversal into the cache, we can first check for any existing answers not yet
            //  returned and send them first.
            requestFromSubsumer(fromUpstream, iteration);
        } else {
            initTraversal();
            Optional<Compound<?, ?>> upstreamAnswer = requestStates.get(fromUpstream).nextAnswer().map(AnswerState.Partial::asCompound);
            if (upstreamAnswer.isPresent()) {
                answerToUpstream(upstreamAnswer.get(), fromUpstream, iteration);
            } else {
                cache.setComplete();
                failToUpstream(fromUpstream, iteration);
            }
        }
    }

    private void requestFromSubsumer(Request fromUpstream, int iteration) {
        assert fromUpstream.subsumer().isPresent();
        Request subsumptionRequest = Request.create(driver(), fromUpstream.subsumer().get(),
                                                    fromUpstream.partialAnswer());
        subsumptionRequests.put(subsumptionRequest, fromUpstream);
        requestFromDownstream(subsumptionRequest, fromUpstream, iteration);
    }

    private void initTraversal() {
        // TODO: Once we no longer rely on the cache to detect the conditions for reiteration, we can not add the
        //  traversal as a source and instead add answers from traversal one-by-one as needed, so that we can kill the
        //  traversal as soon as subsumption kicks in.
        if (!traversalInitialised) cache.addSource(traversalIterator(retrievable.pattern(), bounds));
        traversalInitialised = true;
    }

    @Override
    protected void receiveAnswer(Response.Answer fromDownstream, int iteration) {
        Request fromUpstream = subsumptionRequests.get(fromDownstream.sourceRequest());
        ConceptMap subsumerAnswer = fromDownstream.answer().conceptMap();
        if (cache.subsumes(subsumerAnswer, bounds)) {
            cache.add(fromDownstream.answer().conceptMap());
            BoundRetrievableRequestState requestState = requestStates.get(fromUpstream);
            Optional<Compound<?, ?>> upstreamAnswer = requestState.nextAnswer().map(AnswerState.Partial::asCompound);
            if (upstreamAnswer.isPresent()) {
                answerToUpstream(upstreamAnswer.get(), fromUpstream, iteration);
            } else {
                requestFromSubsumer(fromUpstream, iteration);
            }
        } else {
            requestFromSubsumer(fromUpstream, iteration);
        }

    }

    @Override
    protected void receiveFail(Response.Fail fromDownstream, int iteration) {
        // Check the cache for answers that could have arrived in async
        // If none then fail
        Request fromUpstream = subsumptionRequests.get(fromDownstream.sourceRequest());
        BoundRetrievableRequestState requestState = requestStates.get(fromUpstream);
        Optional<Compound<?, ?>> upstreamAnswer = requestState.nextAnswer().map(AnswerState.Partial::asCompound);
        if (upstreamAnswer.isPresent()) {
            answerToUpstream(upstreamAnswer.get(), fromUpstream, iteration);
        } else {
            cache.setComplete();
            failToUpstream(fromUpstream, iteration);
        }
    }

    @Override
    protected void initialiseDownstreamResolvers() {
        throw TypeDBException.of(ILLEGAL_STATE);
    }

    private static class BoundRetrievableRequestState extends RequestState.CachingRequestState<ConceptMap, ConceptMap> {

        public BoundRetrievableRequestState(Request fromUpstream, AnswerCache<ConceptMap, ConceptMap> answerCache, int iteration) {
            super(fromUpstream, answerCache, iteration, false, false);
        }

        @Override
        protected FunctionalIterator<? extends AnswerState.Partial<?>> toUpstream(ConceptMap answer) {
            return Iterators.single(fromUpstream.partialAnswer().asRetrievable().aggregateToUpstream(answer));
        }
    }
}
