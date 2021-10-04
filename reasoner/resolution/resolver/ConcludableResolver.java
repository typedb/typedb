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
import com.vaticle.typedb.core.logic.resolvable.Concludable;
import com.vaticle.typedb.core.logic.resolvable.Unifier;
import com.vaticle.typedb.core.reasoner.resolution.ResolverRegistry;
import com.vaticle.typedb.core.reasoner.resolution.answer.AnswerState;
import com.vaticle.typedb.core.reasoner.resolution.resolver.BoundConcludableResolver.BoundConcludableContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Set;

public class ConcludableResolver extends SubsumptiveCoordinator<ConcludableResolver, BoundConcludableResolver> {

    private static final Logger LOG = LoggerFactory.getLogger(ConcludableResolver.class);

    private final LinkedHashMap<Driver<ConclusionResolver>, Set<Unifier>> conclusionResolvers;
    private final Concludable concludable;

    public ConcludableResolver(Driver<ConcludableResolver> driver, Concludable concludable, ResolverRegistry registry) {
        super(driver, ConcludableResolver.class.getSimpleName() + "(pattern: " + concludable.pattern() + ")",
                registry);
        this.concludable = concludable;
        this.conclusionResolvers = new LinkedHashMap<>();
        this.isInitialised = false;
    }

    public Concludable concludable() {
        return concludable;
    }

    @Override
    Driver<BoundConcludableResolver> getOrCreateBoundResolver(AnswerState.Partial<?> partial) {
        return workers.computeIfAbsent(partial.conceptMap(), p -> {
            LOG.debug("{}: Creating a new BoundConcludableResolver for bounds: {}", name(), partial);
            // TODO: We could use the bounds to prune the applicable rules further
            BoundConcludableContext context = new BoundConcludableContext(driver(), concludable, conclusionResolvers);
            return registry.registerBoundConcludable(partial.conceptMap(), context, partial.asConcludable().isExplain());
        });
    }

    @Override
    protected void initialiseDownstreamResolvers() {
        LOG.debug("{}: initialising downstream resolvers", name());
        concludable.getApplicableRules(registry.conceptManager(), registry.logicManager())
                .forEachRemaining(rule -> concludable.getUnifiers(rule).forEachRemaining(unifier -> {
                    if (isTerminated()) return;
                    try {
                        Driver<ConclusionResolver> conclusionResolver = registry.registerConclusion(rule.conclusion());
                        conclusionResolvers.computeIfAbsent(conclusionResolver, r -> new HashSet<>()).add(unifier);
                    } catch (TypeDBException e) {
                        terminate(e);
                    }
                }));
        if (!isTerminated()) isInitialised = true;
    }

}
