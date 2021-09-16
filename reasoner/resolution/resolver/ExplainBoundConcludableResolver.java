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
import com.vaticle.typedb.core.reasoner.resolution.framework.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExplainBoundConcludableResolver extends BoundConcludableResolver {

    private static final Logger LOG = LoggerFactory.getLogger(ExplainBoundConcludableResolver.class);

    private final AnswerCache<AnswerState.Partial.Concludable<?>> cache;

    public ExplainBoundConcludableResolver(Driver<BoundConcludableResolver> driver, Driver<ConcludableResolver> parent,
                                           ConceptMap bounds, ResolverRegistry registry) {
        super(driver, parent, bounds, registry);
        this.cache = new AnswerCache<>(Iterators::empty); // TODO How is this working without doing traversal?
    }

    @Override
    ExploringRequestState<?> createExploringRequestState(Request.Template fromUpstream) {
        LOG.debug("{}: Creating new exploring request state for request: {}", name(), fromUpstream);
        return new ExploringRequestState<>(fromUpstream, cache(), ruleDownstreams(fromUpstream), false,
                                           new ExplainUpstream(), false);
    }

    @Override
    BlockedRequestState<?> createBlockedRequestState(Request.Template fromUpstream) {
        LOG.debug("{}: Creating new blocked request state for request: {}", name(), fromUpstream);
        return new BlockedRequestState<>(fromUpstream, cache(), false, new ExplainUpstream(), false);
    }

    @Override
    protected AnswerCache<AnswerState.Partial.Concludable<?>> cache() {
        return cache;
    }

    private static class ExplainUpstream extends UpstreamBehaviour<AnswerState.Partial.Concludable<?>> {

        @Override
        AnswerState.Partial.Concludable<?> answerFromPartial(AnswerState.Partial<?> partial) {
            return partial.asConcludable();
        }

        @Override
        FunctionalIterator<? extends AnswerState.Partial<?>> toUpstream(Request.Template fromUpstream, AnswerState.Partial.Concludable<?> partial) {
            return Iterators.single(partial.asExplain().toUpstreamInferred());
        }
    }

}
