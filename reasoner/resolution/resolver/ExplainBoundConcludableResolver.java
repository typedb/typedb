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
import com.vaticle.typedb.core.reasoner.resolution.answer.AnswerState.Partial;
import com.vaticle.typedb.core.reasoner.resolution.framework.AnswerCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExplainBoundConcludableResolver extends BoundConcludableResolver {

    private static final Logger LOG = LoggerFactory.getLogger(ExplainBoundConcludableResolver.class);

    private final AnswerCache<Partial.Concludable<?>> cache;

    public ExplainBoundConcludableResolver(Driver<BoundConcludableResolver> driver, BoundConcludableContext context,
                                           ConceptMap bounds, ResolverRegistry registry) {
        super(driver, context, bounds, registry);
        this.cache = new AnswerCache<>(Iterators::empty);
    }

    @Override
    ExploringResolutionState<?> createExploringResolutionState(Partial<?> fromUpstream) {
        LOG.debug("{}: Creating new exploring request state for request: {}", name(), fromUpstream);
        return new ExploringResolutionState<>(fromUpstream, cache(), ruleDownstreams(fromUpstream), false,
                                              new ExplainUpstream(), false);
    }

    @Override
    BlockedResolutionState<?> createBlockedResolutionState(Partial<?> fromUpstream) {
        LOG.debug("{}: Creating new blocked request state for request: {}", name(), fromUpstream);
        return new BlockedResolutionState<>(fromUpstream, cache(), false, new ExplainUpstream(), false);
    }

    @Override
    protected AnswerCache<Partial.Concludable<?>> cache() {
        return cache;
    }

    private static class ExplainUpstream implements UpstreamBehaviour<Partial.Concludable<?>> {

        @Override
        public Partial.Concludable<?> answerFromPartial(Partial<?> partial) {
            return partial.asConcludable();
        }

        @Override
        public FunctionalIterator<? extends Partial<?>> toUpstream(Partial<?> fromUpstream,
                                                                   Partial.Concludable<?> partial) {
            return Iterators.single(partial.asExplain().toUpstreamInferred());
        }
    }

}
