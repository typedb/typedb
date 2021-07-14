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
    private final Retrievable retrievable;
    private final ConceptMap bounds;
    private final ConceptMapCache cache;
    private final Map<Request, RetrievableRequestState> requestStates;

    public BoundRetrievableResolver(Retrievable retrievable, ConceptMap bounds, Driver<BoundRetrievableResolver> driver,
                                    ResolverRegistry registry, TraversalEngine traversalEngine,
                                    ConceptManager conceptMgr, boolean resolutionTracing) {
        super(driver, initName(retrievable, bounds), registry, traversalEngine, conceptMgr, resolutionTracing);
        this.retrievable = retrievable;
        this.bounds = bounds;
        this.cache = new ConceptMapCache(new HashMap<>(), this.bounds);
        // TODO: Figure out how to fetch answers from completed subsumers
        if (!this.cache.completeIfSubsumerComplete()) this.cache.addSource(traversalIterator(this.retrievable.pattern(), this.bounds));
        this.requestStates = new HashMap<>();
    }

    private static String initName(Retrievable retrievable, ConceptMap bounds) {
        return BoundRetrievableResolver.class.getSimpleName() + "(pattern: " + retrievable.pattern() + " " +
                bounds.toString() + ")";
    }

    @Override
    public void receiveRequest(Request fromUpstream, int iteration) {
        this.requestStates.computeIfAbsent(fromUpstream, request -> new RetrievableRequestState(request, cache, iteration)); // TODO: Iteration shouldn't be needed
        RetrievableRequestState requestState = this.requestStates.get(fromUpstream);

        Optional<AnswerState.Partial.Compound<?, ?>> upstreamAnswer = requestState.nextAnswer().map(AnswerState.Partial::asCompound);
        if (upstreamAnswer.isPresent()) {
            answerToUpstream(upstreamAnswer.get(), fromUpstream, iteration);
        } else {
            cache.setComplete();
            failToUpstream(fromUpstream, iteration);
        }
    }

    @Override
    protected void receiveAnswer(Response.Answer fromDownstream, int iteration) {
        throw TypeDBException.of(ILLEGAL_STATE);
    }

    @Override
    protected void receiveFail(Response.Fail fromDownstream, int iteration) {
        throw TypeDBException.of(ILLEGAL_STATE);
    }

    @Override
    protected void initialiseDownstreamResolvers() {
        throw TypeDBException.of(ILLEGAL_STATE);
    }

    private static class RetrievableRequestState extends RequestState.CachingRequestState<ConceptMap, ConceptMap> {

        public RetrievableRequestState(Request fromUpstream, AnswerCache<ConceptMap, ConceptMap> answerCache, int iteration) {
            super(fromUpstream, answerCache, iteration, false, false);
        }

        @Override
        protected FunctionalIterator<? extends AnswerState.Partial<?>> toUpstream(ConceptMap answer) {
            return Iterators.single(fromUpstream.partialAnswer().asRetrievable()
                                            .aggregateToUpstream(answer));
        }
    }
}
