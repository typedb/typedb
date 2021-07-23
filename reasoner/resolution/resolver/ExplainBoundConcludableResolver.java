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
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.iterator.Iterators;
import com.vaticle.typedb.core.concept.ConceptManager;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.logic.Rule;
import com.vaticle.typedb.core.logic.resolvable.Concludable;
import com.vaticle.typedb.core.logic.resolvable.Unifier;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.reasoner.resolution.ResolverRegistry;
import com.vaticle.typedb.core.reasoner.resolution.answer.AnswerState;
import com.vaticle.typedb.core.reasoner.resolution.framework.AnswerCache;
import com.vaticle.typedb.core.reasoner.resolution.framework.AnswerCache.ConcludableAnswerCache;
import com.vaticle.typedb.core.reasoner.resolution.framework.Request;
import com.vaticle.typedb.core.reasoner.resolution.framework.RequestState;
import com.vaticle.typedb.core.traversal.TraversalEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;

public class ExplainBoundConcludableResolver extends BoundConcludableResolver {

    private static final Logger LOG = LoggerFactory.getLogger(ExplainBoundConcludableResolver.class);

    public ExplainBoundConcludableResolver(Driver<BoundConcludableResolver> driver, Concludable concludable, ConceptMap bounds, Map<Driver<ConclusionResolver>, Rule> resolverRules, LinkedHashMap<Driver<ConclusionResolver>, Set<Unifier>> applicableRules, ResolverRegistry registry, TraversalEngine traversalEngine, ConceptManager conceptMgr, boolean resolutionTracing) {
        super(driver, concludable, bounds, resolverRules, applicableRules, registry, traversalEngine, conceptMgr,
              resolutionTracing);
    }

    @Override
    ConcludableAnswerCache createCache(Conjunction conjunction, ConceptMap bounds) {
        return new ConcludableAnswerCache(new HashMap<>(), bounds); // TODO How is this working without doing traversal?
    }


    @Override
    protected RequestState.CachingRequestState<?, ConceptMap> createRequestState(Request fromUpstream, int iteration) {
        LOG.debug("{}: Creating new Responses for iteration{}, request: {}", name(), iteration, fromUpstream);
        RequestState.CachingRequestState<?, ConceptMap> requestState;
        if (exploringRequestState != null) {
            if (cache.isConceptMapCache()) {
                // We have a cache already which we must evict to use a cache suitable for explaining
                cache = new ConcludableAnswerCache(new HashMap<>(), bounds);
                requestState = new Explain(fromUpstream, cache.asConcludableAnswerCache(), iteration);
                requestState.asExploration().downstreamManager().addDownstreams(ruleDownstreams(fromUpstream));
            } else if (cache.isConcludableAnswerCache()) {
                requestState = new FollowingExplain(fromUpstream, cache.asConcludableAnswerCache(), iteration);
            } else {
                throw TypeDBException.of(ILLEGAL_STATE);
            }
        } else {
            cache = new ConcludableAnswerCache(new HashMap<>(), bounds);
            requestState = new Explain(fromUpstream, cache.asConcludableAnswerCache(), iteration);
            requestState.asExploration().downstreamManager().addDownstreams(ruleDownstreams(fromUpstream));
        }
        return requestState;
    }

    private static class FollowingExplain extends RequestState.CachingRequestState<AnswerState.Partial.Concludable<?>, ConceptMap> {

        public FollowingExplain(Request fromUpstream, AnswerCache<AnswerState.Partial.Concludable<?>, ConceptMap> answerCache,
                                int iteration) {
            super(fromUpstream, answerCache, iteration, true, true);
        }

        @Override
        protected FunctionalIterator<? extends AnswerState.Partial<?>> toUpstream(AnswerState.Partial.Concludable<?> partial) {
            return Iterators.single(partial.asExplain().toUpstreamInferred());
        }
    }

    private static class Explain extends RequestState.CachingRequestState<AnswerState.Partial.Concludable<?>, ConceptMap> implements RequestState.Exploration {

        private final DownstreamManager downstreamManager;

        public Explain(Request fromUpstream, AnswerCache<AnswerState.Partial.Concludable<?>, ConceptMap> answerCache,
                       int iteration) {
            super(fromUpstream, answerCache, iteration, false, false);
            this.downstreamManager = new DownstreamManager();
        }

        @Override
        public boolean isExploration() {
            return true;
        }

        @Override
        public Exploration asExploration() {
            return this;
        }

        @Override
        public DownstreamManager downstreamManager() {
            return downstreamManager;
        }

        @Override
        public void newAnswer(AnswerState.Partial<?> partial) {
            if (!answerCache.isComplete()) answerCache.add(partial.asConcludable());
        }

        @Override
        public boolean singleAnswerRequired() {
            return false;
        }

        @Override
        protected FunctionalIterator<? extends AnswerState.Partial<?>> toUpstream(AnswerState.Partial.Concludable<?> partial) {
            return Iterators.single(partial.asExplain().toUpstreamInferred());
        }

    }

}
