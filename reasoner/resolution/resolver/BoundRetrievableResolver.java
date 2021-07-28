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
    private final ConceptMapCache cache;
    private final Map<Request, RequestState> requestStates;
    private ConceptMap bounds;

    public BoundRetrievableResolver(Driver<BoundRetrievableResolver> driver, Retrievable retrievable, ConceptMap bounds,
                                    ResolverRegistry registry, TraversalEngine traversalEngine,
                                    ConceptManager conceptMgr, boolean resolutionTracing) {
        super(driver, initName(retrievable, bounds), registry, traversalEngine, conceptMgr, resolutionTracing);
        this.bounds = bounds;
        this.cache = new ConceptMapCache(new HashMap<>(), bounds, () -> traversalIterator(retrievable.pattern(), bounds));
        this.requestStates = new HashMap<>();
    }

    private static String initName(Retrievable retrievable, ConceptMap bounds) {
        return BoundRetrievableResolver.class.getSimpleName() + "(pattern: " + retrievable.pattern() + " bounds: " +
                bounds.toString() + ")";
    }

    @Override
    public void receiveRequest(Request fromUpstream, int iteration) {
        if (fromUpstream.isToSubsumed()) {
            assert fromUpstream.partialAnswer().conceptMap().equals(bounds);
            receiveSubsumedRequest(fromUpstream.asToSubsumed(), iteration);
        } else if (fromUpstream.isToSubsumer()) {
            receiveSubsumerRequest(fromUpstream.asToSubsumer(), iteration);
        } else {
            assert fromUpstream.partialAnswer().conceptMap().equals(bounds);
            receiveDirectRequest(fromUpstream, iteration);
        }
    }

    private void receiveSubsumedRequest(Request.ToSubsumed fromUpstream, int iteration) {
        RequestState requestState = requestStates.computeIfAbsent(
                fromUpstream, request -> new BoundRequestState(request, cache, iteration));
        if (cache.sourceExhausted()) {
            sendAnswerOrFail(fromUpstream, iteration, requestState);
        } else {
            cache.clearSource();
            Optional<? extends AnswerState.Partial<?>> upstreamAnswer;
            upstreamAnswer = requestState.nextAnswer();
            if (upstreamAnswer.isPresent()) {
                answerToUpstream(upstreamAnswer.get(), fromUpstream, iteration);
            } else {
                requestFromSubsumer(fromUpstream, iteration);
            }
        }
    }

    private void receiveSubsumerRequest(Request.ToSubsumer fromUpstream, int iteration) {
        sendAnswerOrFail(fromUpstream, iteration, requestStates.computeIfAbsent(
                fromUpstream, request -> new SubsumerRequestState(request, cache, iteration)));
    }

    private void receiveDirectRequest(Request fromUpstream, int iteration) {
        sendAnswerOrFail(fromUpstream, iteration, requestStates.computeIfAbsent(
                fromUpstream, request -> new BoundRequestState(request, cache, iteration)));
    }

    @Override
    protected void receiveAnswer(Response.Answer fromDownstream, int iteration) {
        Request.ToSubsumed fromUpstream = fromDownstream.sourceRequest().asToSubsumer().toSubsumed();
        if (cache.sourceExhausted()) sendAnswerOrFail(fromUpstream, iteration, requestStates.get(fromUpstream));
        else {
            cache.add(fromDownstream.answer().conceptMap());
            Optional<? extends AnswerState.Partial<?>> upstreamAnswer = requestStates.get(fromUpstream).nextAnswer();
            if (upstreamAnswer.isPresent()) {
                answerToUpstream(upstreamAnswer.get(), fromUpstream, iteration);
            } else {
                requestFromSubsumer(fromUpstream, iteration);
            }
        }
    }

    @Override
    protected void receiveFail(Response.Fail fromDownstream, int iteration) {
        cache.setComplete();
        Request fromUpstream = fromDownstream.sourceRequest().asToSubsumer().toSubsumed();
        sendAnswerOrFail(fromUpstream, iteration, requestStates.get(fromUpstream));
    }

    private void sendAnswerOrFail(Request fromUpstream, int iteration, RequestState requestState) {
        Optional<? extends AnswerState.Partial<?>> upstreamAnswer = requestState.nextAnswer();
        if (upstreamAnswer.isPresent()) {
            answerToUpstream(upstreamAnswer.get(), fromUpstream, iteration);
        } else {
            failToUpstream(fromUpstream, iteration);
        }
    }

    private void requestFromSubsumer(Request.ToSubsumed fromUpstream, int iteration) {
        Request toSubsumer = Request.ToSubsumer.create(driver(), fromUpstream.subsumer(),
                                                       fromUpstream, fromUpstream.partialAnswer());
        requestFromDownstream(toSubsumer, fromUpstream, iteration);
    }

    @Override
    protected void initialiseDownstreamResolvers() {
        throw TypeDBException.of(ILLEGAL_STATE);
    }

    private static class BoundRequestState extends RequestState.CachingRequestState<ConceptMap, ConceptMap> {

        public BoundRequestState(Request fromUpstream, AnswerCache<ConceptMap, ConceptMap> answerCache, int iteration) {  // TODO: Iteration shouldn't be needed
            super(fromUpstream, answerCache, iteration, false, false);
        }

        @Override
        protected FunctionalIterator<? extends AnswerState.Partial<?>> toUpstream(ConceptMap answer) {
            return Iterators.single(fromUpstream.partialAnswer().asRetrievable().aggregateToUpstream(answer));
        }
    }

    private class SubsumerRequestState extends RequestState.CachingRequestState<ConceptMap, ConceptMap> {

        public SubsumerRequestState(Request fromUpstream, AnswerCache<ConceptMap, ConceptMap> answerCache, int iteration) {  // TODO: Iteration shouldn't be needed
            super(fromUpstream, answerCache, iteration, false, false);
        }

        @Override
        protected FunctionalIterator<? extends AnswerState.Partial<?>> toUpstream(ConceptMap answer) {
            if (subsumesBounds(answer)) {
                return Iterators.single(fromUpstream.partialAnswer().asRetrievable().with(answer));
            } else {
                return Iterators.empty();
            }
        }

        private boolean subsumesBounds(ConceptMap subsumer) {
            return subsumer.concepts().entrySet().containsAll(bounds.concepts().entrySet());
        }
    }
}
