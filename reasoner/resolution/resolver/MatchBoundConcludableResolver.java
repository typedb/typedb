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
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.reasoner.resolution.ResolverRegistry;
import com.vaticle.typedb.core.reasoner.resolution.answer.AnswerState;
import com.vaticle.typedb.core.reasoner.resolution.framework.AnswerCache;
import com.vaticle.typedb.core.reasoner.resolution.framework.Downstream;
import com.vaticle.typedb.core.reasoner.resolution.framework.Request;
import com.vaticle.typedb.core.traversal.common.Identifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public class MatchBoundConcludableResolver extends BoundConcludableResolver {

    private static final Logger LOG = LoggerFactory.getLogger(MatchBoundConcludableResolver.class);
    private final boolean singleAnswerRequired;
    private final AnswerCache<ConceptMap> cache;

    public MatchBoundConcludableResolver(Driver<BoundConcludableResolver> driver, Driver<ConcludableResolver> parent,
                                         ConceptMap bounds, ResolverRegistry registry) {
        super(driver, parent, bounds, registry);
        this.singleAnswerRequired = bounds.concepts().keySet().containsAll(unboundVars());
        this.cache = new AnswerCache<>(() -> traversalIterator(parent.actor().concludable().pattern(), bounds));
    }

    @Override
    BoundConcludableResolver.ExploringRequestState<?> createExploringRequestState(Request.Visit fromUpstream) {
        LOG.debug("{}: Creating new exploring request state for request: {}", name(), fromUpstream);
        return new MatchRequestState(fromUpstream, cache, ruleDownstreams(fromUpstream));
    }

    @Override
    protected AnswerCache<ConceptMap> cache() {
        return cache;
    }

    private Set<Identifier.Variable.Retrievable> unboundVars() {
        Set<Identifier.Variable.Retrievable> missingBounds = new HashSet<>();
        iterate(parent().actor().concludable().pattern().variables())
                .filter(var -> var.id().isRetrievable()).forEachRemaining(var -> {
            if (var.isType() && !var.asType().label().isPresent()) {
                missingBounds.add(var.asType().id().asRetrievable());
            } else if (var.isThing() && !var.asThing().iid().isPresent()) {
                missingBounds.add(var.asThing().id().asRetrievable());
            }
        });
        return missingBounds;
    }

    private class MatchRequestState extends ExploringRequestState<ConceptMap> implements RequestState.Exploration {

        private MatchRequestState(Request.Visit fromUpstream, AnswerCache<ConceptMap> answerCache,
                                  List<Downstream> ruleDownstreams) {
            super(fromUpstream, answerCache, ruleDownstreams, true);
        }

        @Override
        ConceptMap answerFromPartial(AnswerState.Partial<?> partial) {
            return partial.conceptMap();
        }

        @Override
        public boolean singleAnswerRequired() {
            return singleAnswerRequired;
        }

        @Override
        protected FunctionalIterator<? extends AnswerState.Partial<?>> toUpstream(ConceptMap conceptMap) {
            return Iterators.single(fromUpstream.partialAnswer().asConcludable().asMatch().toUpstreamLookup(
                    conceptMap, parent().actor().concludable().isInferredAnswer(conceptMap)));
        }
    }
}
