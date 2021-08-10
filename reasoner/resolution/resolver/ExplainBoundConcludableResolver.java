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

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.iterator.Iterators;
import com.vaticle.typedb.core.concept.ConceptManager;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.logic.resolvable.Concludable;
import com.vaticle.typedb.core.reasoner.resolution.ResolverRegistry;
import com.vaticle.typedb.core.reasoner.resolution.answer.AnswerState;
import com.vaticle.typedb.core.reasoner.resolution.framework.AnswerCache;
import com.vaticle.typedb.core.reasoner.resolution.framework.Request;
import com.vaticle.typedb.core.reasoner.resolution.framework.RequestState.CachingRequestState;
import com.vaticle.typedb.core.reasoner.resolution.framework.RequestState.Exploration;
import com.vaticle.typedb.core.traversal.TraversalEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ExplainBoundConcludableResolver extends BoundConcludableResolver {

    private static final Logger LOG = LoggerFactory.getLogger(ExplainBoundConcludableResolver.class);

    private final AnswerCache<AnswerState.Partial.Concludable<?>> cache;

    public ExplainBoundConcludableResolver(Driver<BoundConcludableResolver> driver, Concludable concludable,
                                           ConceptMap bounds, ResolverRegistry registry,
                                           TraversalEngine traversalEngine, ConceptManager conceptMgr,
                                           boolean resolutionTracing) {
        super(driver, concludable, bounds, registry, traversalEngine, conceptMgr, resolutionTracing);
        this.cache = new AnswerCache<>(Iterators::empty); // TODO How is this working without doing traversal?
    }

    @Override
    protected CachingRequestState<?> createRequestState(Request fromUpstream, int iteration) {
        LOG.debug("{}: Creating new request state for iteration{}, request: {}", name(), iteration, fromUpstream);
        return new RequestState(fromUpstream, cache, iteration, true, true);
    }

    @Override
    CachingRequestState<?> createExploringRequestState(Request fromUpstream, int iteration) {
        LOG.debug("{}: Creating new exploring request state for iteration{}, request: {}", name(), iteration,
                  fromUpstream);
        return new ExploringRequestState(fromUpstream, cache, iteration, ruleDownstreams(fromUpstream));
    }

    @Override
    protected AnswerCache<AnswerState.Partial.Concludable<?>> cache() {
        return cache;
    }

    private static class RequestState extends CachingRequestState<AnswerState.Partial.Concludable<?>> {

        public RequestState(Request fromUpstream,
                            AnswerCache<AnswerState.Partial.Concludable<?>> answerCache,
                            int iteration, boolean deduplicate, boolean isSubscriber) {
            super(fromUpstream, answerCache, iteration, deduplicate, isSubscriber);
        }

        @Override
        protected FunctionalIterator<? extends AnswerState.Partial<?>> toUpstream(
                AnswerState.Partial.Concludable<?> partial) {
            return Iterators.single(partial.asExplain().toUpstreamInferred());
        }
    }

    private static class ExploringRequestState extends RequestState implements Exploration {

        private final DownstreamManager downstreamManager;

        public ExploringRequestState(Request fromUpstream,
                                     AnswerCache<AnswerState.Partial.Concludable<?>> answerCache,
                                     int iteration, List<Request> ruleDownstreams) {
            super(fromUpstream, answerCache, iteration, false, false);
            this.downstreamManager = new DownstreamManager(ruleDownstreams);
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

    }

}
