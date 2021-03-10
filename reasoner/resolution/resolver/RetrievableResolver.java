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

import com.vaticle.typedb.common.collection.Pair;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.iterator.Iterators;
import com.vaticle.typedb.core.concept.ConceptManager;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.logic.resolvable.Retrievable;
import com.vaticle.typedb.core.reasoner.resolution.ResolverRegistry;
import com.vaticle.typedb.core.reasoner.resolution.answer.AnswerState.Partial;
import com.vaticle.typedb.core.reasoner.resolution.framework.AnswerCache;
import com.vaticle.typedb.core.reasoner.resolution.framework.AnswerCache.SubsumptionAnswerCache.ConceptMapCache;
import com.vaticle.typedb.core.reasoner.resolution.framework.Request;
import com.vaticle.typedb.core.reasoner.resolution.framework.RequestState.CachingRequestState;
import com.vaticle.typedb.core.reasoner.resolution.framework.Resolver;
import com.vaticle.typedb.core.reasoner.resolution.framework.Response;
import com.vaticle.typedb.core.reasoner.resolution.framework.Response.Answer;
import com.vaticle.typedb.core.traversal.TraversalEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;

public class RetrievableResolver extends Resolver<RetrievableResolver> {

    private static final Logger LOG = LoggerFactory.getLogger(RetrievableResolver.class);

    private final Retrievable retrievable;
    private final Map<Request, RetrievableRequestState> requestStates;
    protected final Map<Driver<? extends Resolver<?>>, Pair<Map<ConceptMap, AnswerCache<ConceptMap, ConceptMap>>, Integer>> cacheRegistersByRoot;

    public RetrievableResolver(Driver<RetrievableResolver> driver, Retrievable retrievable, ResolverRegistry registry,
                               TraversalEngine traversalEngine, ConceptManager conceptMgr, boolean resolutionTracing) {
        super(driver, RetrievableResolver.class.getSimpleName() + "(pattern: " + retrievable.pattern() + ")",
              registry, traversalEngine, conceptMgr, resolutionTracing);
        this.retrievable = retrievable;
        this.requestStates = new HashMap<>();
        this.cacheRegistersByRoot = new HashMap<>();
    }

    @Override
    public void receiveRequest(Request fromUpstream, int iteration) {
        LOG.trace("{}: received Request: {}", name(), fromUpstream);
        if (isTerminated()) return;

        RetrievableRequestState requestState = getOrReplaceRequestState(fromUpstream, iteration);
        if (iteration < requestState.iteration()) {
            // short circuit old iteration failed messages to upstream
            failToUpstream(fromUpstream, iteration);
        } else {
            assert iteration == requestState.iteration();
            nextAnswer(fromUpstream, requestState, iteration);
        }
    }

    @Override
    protected void receiveAnswer(Answer fromDownstream, int iteration) {
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

    private RetrievableRequestState getOrReplaceRequestState(Request fromUpstream, int iteration) {
        if (!requestStates.containsKey(fromUpstream)) {
            requestStates.put(fromUpstream, createRequestState(fromUpstream, iteration));
        } else {
            RetrievableRequestState requestState = this.requestStates.get(fromUpstream);

            if (requestState.iteration() < iteration) {
                // when the same request for the next iteration the first time, re-initialise required state
                RetrievableRequestState requestStateNextIter = createRequestState(fromUpstream, iteration);
                this.requestStates.put(fromUpstream, requestStateNextIter);
            }
        }
        return requestStates.get(fromUpstream);
    }

    protected RetrievableRequestState createRequestState(Request fromUpstream, int iteration) {
        LOG.debug("{}: Creating a new RequestState for iteration:{}, request: {}", name(), iteration, fromUpstream);
        assert fromUpstream.partialAnswer().isRetrievable();
        Map<ConceptMap, AnswerCache<ConceptMap, ConceptMap>> cacheRegister = cacheRegisterForRoot(
                fromUpstream.partialAnswer().root(), iteration);
        AnswerCache<ConceptMap, ConceptMap> answerCache = cacheRegister.computeIfAbsent(
                fromUpstream.partialAnswer().conceptMap(), upstreamAns -> {
                    AnswerCache<ConceptMap, ConceptMap> newCache = new ConceptMapCache(cacheRegister, upstreamAns);
                    if (!newCache.isComplete()) newCache.addSource(traversalIterator(retrievable.pattern(), upstreamAns));
                    return newCache;
                });
        return new RetrievableRequestState(fromUpstream, answerCache, iteration);
    }

    private Map<ConceptMap, AnswerCache<ConceptMap, ConceptMap>> cacheRegisterForRoot(Driver<? extends Resolver<?>> root, int iteration) {
        if (cacheRegistersByRoot.containsKey(root) && cacheRegistersByRoot.get(root).second() < iteration) {
            cacheRegistersByRoot.remove(root);
        }
        cacheRegistersByRoot.putIfAbsent(root, new Pair<>(new HashMap<>(), iteration));
        return cacheRegistersByRoot.get(root).first();
    }

    private void nextAnswer(Request fromUpstream, RetrievableRequestState requestState, int iteration) {
        Optional<Partial.Compound<?, ?>> upstreamAnswer = requestState.nextAnswer().map(Partial::asCompound);
        if (upstreamAnswer.isPresent()) {
            answerToUpstream(upstreamAnswer.get(), fromUpstream, iteration);
        } else {
            requestStates.get(fromUpstream).answerCache().setComplete();
            failToUpstream(fromUpstream, iteration);
        }
    }

    private static class RetrievableRequestState extends CachingRequestState<ConceptMap, ConceptMap> {

        public RetrievableRequestState(Request fromUpstream, AnswerCache<ConceptMap, ConceptMap> answerCache, int iteration) {
            super(fromUpstream, answerCache, iteration, false, true);
        }

        @Override
        protected FunctionalIterator<? extends Partial<?>> toUpstream(ConceptMap answer) {
            return Iterators.single(fromUpstream.partialAnswer().asRetrievable()
                                            .aggregateToUpstream(answer, answerCache.requiresReexploration()));
        }
    }
}
