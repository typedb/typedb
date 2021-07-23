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
import com.vaticle.typedb.core.concept.ConceptManager;
import com.vaticle.typedb.core.logic.Rule;
import com.vaticle.typedb.core.reasoner.resolution.ResolverRegistry;
import com.vaticle.typedb.core.reasoner.resolution.answer.AnswerState.Partial;
import com.vaticle.typedb.core.reasoner.resolution.framework.Resolver;
import com.vaticle.typedb.core.traversal.TraversalEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public class ConclusionResolver extends Coordinator<ConclusionResolver, BoundConclusionResolver> {

    private static final Logger LOG = LoggerFactory.getLogger(ConclusionResolver.class);

    private final Rule.Conclusion conclusion;
    private Driver<ConditionResolver> conditionResolver;

    public ConclusionResolver(Driver<ConclusionResolver> driver, Rule.Conclusion conclusion, ResolverRegistry registry,
                              TraversalEngine traversalEngine, ConceptManager conceptMgr, boolean resolutionTracing) {
        super(driver, ConclusionResolver.class.getSimpleName() + "(" + conclusion + ")",
              registry, traversalEngine, conceptMgr, resolutionTracing);
        this.conclusion = conclusion;
        this.isInitialised = false;
    }

    @Override
    protected void initialiseDownstreamResolvers() {
        LOG.debug("{}: initialising downstream resolvers", name());
        try {
            conditionResolver = registry.registerCondition(conclusion.rule().condition());
            isInitialised = true;
        } catch (TypeDBException e) {
            terminate(e);
        }
    }

    @Override
    Driver<BoundConclusionResolver> getOrReplaceWorker(Driver<? extends Resolver<?>> root, Partial<?> partial) {
        return workersByRoot.computeIfAbsent(root, r -> new HashMap<>()).computeIfAbsent(partial.conceptMap(), p -> {
            LOG.debug("{}: Creating a new BoundConclusionResolver for bounds: {}", name(), partial);
            return registry.registerBoundConclusion(conclusion, partial.conceptMap(), conditionResolver,
                                                    partial.asConclusion().isExplain());
        });
    }

    @Override
    public String toString() {
        return name();
    }
}
