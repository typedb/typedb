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
import com.vaticle.typedb.core.logic.Rule;
import com.vaticle.typedb.core.logic.resolvable.Concludable;
import com.vaticle.typedb.core.logic.resolvable.Unifier;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.reasoner.resolution.ResolverRegistry;
import com.vaticle.typedb.core.reasoner.resolution.answer.AnswerState;
import com.vaticle.typedb.core.reasoner.resolution.framework.AnswerCache;
import com.vaticle.typedb.core.reasoner.resolution.framework.AnswerCache.SubsumptionAnswerCache.ConceptMapCache;
import com.vaticle.typedb.core.reasoner.resolution.framework.Request;
import com.vaticle.typedb.core.reasoner.resolution.framework.RequestState.CachingRequestState;
import com.vaticle.typedb.core.reasoner.resolution.framework.RequestState.Exploration;
import com.vaticle.typedb.core.traversal.TraversalEngine;
import com.vaticle.typedb.core.traversal.common.Identifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public class MatchBoundConcludableResolver extends BoundConcludableResolver {

    private static final Logger LOG = LoggerFactory.getLogger(MatchBoundConcludableResolver.class);
    private final boolean singleAnswerRequired;

    public MatchBoundConcludableResolver(Driver<BoundConcludableResolver> driver, Concludable concludable,
                                         ConceptMap bounds, Map<Driver<ConclusionResolver>, Rule> resolverRules,
                                         LinkedHashMap<Driver<ConclusionResolver>, Set<Unifier>> applicableRules,
                                         ResolverRegistry registry, TraversalEngine traversalEngine,
                                         ConceptManager conceptMgr, boolean resolutionTracing) {
        super(driver, concludable, bounds, resolverRules, applicableRules, registry, traversalEngine, conceptMgr,
              resolutionTracing);
        this.singleAnswerRequired = bounds.concepts().keySet().containsAll(unboundVars());
    }

    @Override
    ConceptMapCache createCache(Conjunction conjunction, ConceptMap bounds) {
        return new ConceptMapCache(new HashMap<>(), bounds, () -> traversalIterator(conjunction, bounds));
    }

    @Override
    protected CachingRequestState<?, ConceptMap> createRequestState(Request fromUpstream, int iteration) {
        LOG.debug("{}: Creating new Responses for iteration{}, request: {}", name(), iteration, fromUpstream);
        CachingRequestState<?, ConceptMap> requestState;
        assert cache.isConceptMapCache();
        if (exploringRequestState == null) {
            requestState = new ExploringRequestState(fromUpstream, cache.asConceptMapCache(), iteration
            );
            requestState.asExploration().downstreamManager().addDownstreams(ruleDownstreams(fromUpstream));
        } else {
            requestState = new RequestState(fromUpstream, cache.asConceptMapCache(), iteration, true);
        }
        return requestState;
    }

    private Set<Identifier.Variable.Retrievable> unboundVars() {
        Set<Identifier.Variable.Retrievable> missingBounds = new HashSet<>();
        iterate(concludable.pattern().variables()).filter(var -> var.id().isRetrievable()).forEachRemaining(var -> {
            if (var.isType() && !var.asType().label().isPresent()) missingBounds.add(var.asType().id().asRetrievable());
            else if (var.isThing() && !var.asThing().iid().isPresent())
                missingBounds.add(var.asThing().id().asRetrievable());
        });
        return missingBounds;
    }

    private class RequestState extends CachingRequestState<ConceptMap, ConceptMap> {

        public RequestState(Request fromUpstream, AnswerCache<ConceptMap, ConceptMap> answerCache, int iteration,
                            boolean isSubscriber) {
            super(fromUpstream, answerCache, iteration, true, isSubscriber);
        }

        @Override
        protected FunctionalIterator<? extends AnswerState.Partial<?>> toUpstream(ConceptMap conceptMap) {
            return Iterators.single(fromUpstream.partialAnswer().asConcludable().asMatch().toUpstreamLookup(
                    conceptMap, concludable.isInferredAnswer(conceptMap)));
        }

    }

    private class ExploringRequestState extends RequestState implements Exploration {

        private final DownstreamManager downstreamManager;

        public ExploringRequestState(Request fromUpstream, AnswerCache<ConceptMap, ConceptMap> answerCache,
                                     int iteration) {
            super(fromUpstream, answerCache, iteration, false);
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
            if (!answerCache.isComplete()) answerCache.add(partial.conceptMap());
        }

        @Override
        public boolean singleAnswerRequired() {
            return singleAnswerRequired;
        }
    }
}
