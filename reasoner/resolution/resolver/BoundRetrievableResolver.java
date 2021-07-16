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
    private final Map<Request, RequestState> requestStates;
    private boolean traversalInitialised;

    public BoundRetrievableResolver(Retrievable retrievable, ConceptMap bounds, Driver<BoundRetrievableResolver> driver,
                                    ResolverRegistry registry, TraversalEngine traversalEngine,
                                    ConceptManager conceptMgr, boolean resolutionTracing) {
        super(driver, initName(retrievable, bounds), registry, traversalEngine, conceptMgr, resolutionTracing);
        this.retrievable = retrievable;
        this.bounds = bounds;
        this.cache = new ConceptMapCache(new HashMap<>(), this.bounds);
        this.requestStates = new HashMap<>();
        this.traversalInitialised = false;
    }

    private static String initName(Retrievable retrievable, ConceptMap bounds) {
        return BoundRetrievableResolver.class.getSimpleName() + "(pattern: " + retrievable.pattern() + " " +
                bounds.toString() + ")";
    }

    @Override
    public void receiveRequest(Request fromUpstream, int iteration) {
        if (fromUpstream.isToSubsumed()) {
            receiveSubsumedRequest(fromUpstream.asToSubsumed(), iteration);
        } else if (fromUpstream.isToSubsumer()) {
            receiveSubsumerRequest(fromUpstream.asToSubsumer(), iteration);
        } else {
            receiveDirectRequest(fromUpstream, iteration);
        }
    }

    private void receiveSubsumedRequest(Request.ToSubsumed fromUpstream, int iteration) {
        RequestState requestState = requestStates.computeIfAbsent(
                fromUpstream, request -> new BoundRetrievableRequestState(request, cache, iteration));
        if (cache.isComplete()) {
            // We need to continue from where the RequestState left off before subsumption was activated, so we use
            // toRequest() to convert to a vanilla request. TODO: Create a more elegant solution to track this state
            sendAnswerOrFail(fromUpstream, iteration, requestState);
        } else {
            // TODO: Once we don't add the traversal into the cache, we will be able to first check for any existing
            //  answers not yet returned and send them first.
            requestFromSubsumer(fromUpstream, iteration);
        }
    }

    private void receiveSubsumerRequest(Request.ToSubsumer fromUpstream, int iteration) {
        sendAnswerOrFail(fromUpstream, iteration, requestStates.computeIfAbsent(
                fromUpstream, request -> new BoundRetrievableRequestState(request, cache, iteration)));
    }

    private void receiveDirectRequest(Request fromUpstream, int iteration) {
        initTraversal();
        sendAnswerOrFail(fromUpstream, iteration, requestStates.computeIfAbsent(
                fromUpstream, request -> new BoundRetrievableRequestState(request, cache, iteration)));
    }

    @Override
    protected void receiveAnswer(Response.Answer fromDownstream, int iteration) {
        Request.ToSubsumed fromUpstream = fromDownstream.sourceRequest().asToSubsumer().toSubsumed();
        if (cache.isComplete()) sendAnswerOrFail(fromUpstream, iteration, requestStates.get(fromUpstream));
        else {
            ConceptMap subsumerAnswer = fromDownstream.answer().conceptMap();
            if (cache.subsumes(subsumerAnswer, bounds)) {
                cache.add(subsumerAnswer.filter(retrievable.retrieves()));
                Optional<Compound<?, ?>> upstreamAnswer = requestStates.get(fromUpstream).nextAnswer().map(AnswerState.Partial::asCompound);
                if (upstreamAnswer.isPresent()) {
                    answerToUpstream(upstreamAnswer.get(), fromUpstream, iteration);
                } else {
                    requestFromSubsumer(fromUpstream, iteration);
                }
            } else {
                requestFromSubsumer(fromUpstream, iteration);
            }
        }
    }

    @Override
    protected void receiveFail(Response.Fail fromDownstream, int iteration) {
        Request fromUpstream = fromDownstream.sourceRequest().asToSubsumer().toSubsumed();
        sendAnswerOrFail(fromUpstream, iteration, requestStates.get(fromUpstream));
    }

    private void sendAnswerOrFail(Request fromUpstream, int iteration, RequestState requestState) {
        Optional<Compound<?, ?>> upstreamAnswer = requestState.nextAnswer().map(AnswerState.Partial::asCompound);
        if (upstreamAnswer.isPresent()) {
            answerToUpstream(upstreamAnswer.get(), fromUpstream, iteration);
        } else {
            cache.setComplete();
            failToUpstream(fromUpstream, iteration);
        }
    }

    private void requestFromSubsumer(Request.ToSubsumed fromUpstream, int iteration) {
        Request toSubsumer = Request.ToSubsumer.create(driver(), fromUpstream.subsumer(),
                                                       fromUpstream, fromUpstream.partialAnswer());
        requestFromDownstream(toSubsumer, fromUpstream, iteration);
    }

    private void initTraversal() {
        // TODO: Once we no longer rely on the cache to detect the conditions for reiteration, we can not add the
        //  traversal as a source and instead add answers from traversal one-by-one as needed, so that we can kill the
        //  traversal as soon as subsumption kicks in.
        if (!traversalInitialised) cache.addSource(traversalIterator(retrievable.pattern(), bounds));
        traversalInitialised = true;
    }

    @Override
    protected void initialiseDownstreamResolvers() {
        throw TypeDBException.of(ILLEGAL_STATE);
    }

    private static class BoundRetrievableRequestState extends RequestState.CachingRequestState<ConceptMap, ConceptMap> {

        public BoundRetrievableRequestState(Request fromUpstream, AnswerCache<ConceptMap, ConceptMap> answerCache, int iteration) {  // TODO: Iteration shouldn't be needed
            super(fromUpstream, answerCache, iteration, false, false);
        }

        @Override
        protected FunctionalIterator<? extends AnswerState.Partial<?>> toUpstream(ConceptMap answer) {
            return Iterators.single(fromUpstream.partialAnswer().asRetrievable().aggregateToUpstream(answer));
        }
    }
}
