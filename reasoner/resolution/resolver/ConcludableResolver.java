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
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.logic.resolvable.Concludable;
import com.vaticle.typedb.core.logic.resolvable.Unifier;
import com.vaticle.typedb.core.reasoner.resolution.ResolverRegistry;
import com.vaticle.typedb.core.reasoner.resolution.answer.AnswerState;
import com.vaticle.typedb.core.reasoner.resolution.answer.Mapping;
import com.vaticle.typedb.core.reasoner.resolution.resolver.BoundConcludableResolver.BoundConcludableContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public class ConcludableResolver extends SubsumptiveCoordinator<ConcludableResolver> {

    private static final Logger LOG = LoggerFactory.getLogger(ConcludableResolver.class);

    private final LinkedHashMap<Driver<ConclusionResolver>, Set<Unifier>> conclusionResolvers;
    private final Concludable concludable;
    private final Map<AnswerState.Partial.Concludable<?>, Boolean> isCycle;
    private final Map<ConceptMap, Driver<BoundConcludableResolver.Blocked>> blockedBoundResolvers;
    private final Map<ConceptMap, Driver<BoundConcludableResolver.Exploring>> exploringBoundResolvers;

    public ConcludableResolver(Driver<ConcludableResolver> driver, Concludable concludable, ResolverRegistry registry) {
        super(driver, ConcludableResolver.class.getSimpleName() + "(pattern: " + concludable.pattern() + ")",
              createEquivalentMappings(concludable), registry);
        this.concludable = concludable;
        this.conclusionResolvers = new LinkedHashMap<>();
        this.isInitialised = false;
        this.isCycle = new HashMap<>();
        this.blockedBoundResolvers = new HashMap<>();
        this.exploringBoundResolvers = new HashMap<>();
    }

    public Concludable concludable() {
        return concludable;
    }

    private static Set<Mapping> createEquivalentMappings(Concludable concludable) {
        return concludable.alphaEquals(concludable).map(am -> Mapping.of(am.retrievableMapping())).toSet();
    }

    @Override
    protected Driver<? extends BoundConcludableResolver<?>> getOrCreateBoundResolver(AnswerState.Partial<?> partial, ConceptMap mapped) {
        // TODO: partial answer only needed for cycle detection
        // Note: `mapped` is a different ConceptMap to that of the partial answer, an important optimisation.
        boolean cycle = isCycle.computeIfAbsent(partial.asConcludable(), p -> isCycle(p, mapped));
        if (cycle) {
            return blockedBoundResolvers.computeIfAbsent(mapped, conceptMap -> {
                LOG.debug("{}: Creating a new BoundConcludableResolver.Blocked for bounds: {}", name(), partial);
                BoundConcludableContext context = new BoundConcludableContext(concludable, conclusionResolvers);
                return registry.registerBlocked(conceptMap, context);
            });
        } else {
            return exploringBoundResolvers.computeIfAbsent(mapped, conceptMap -> {
                LOG.debug("{}: Creating a new BoundConcludableResolver.Exploring for bounds: {}", name(), partial);
                BoundConcludableContext context = new BoundConcludableContext(concludable, conclusionResolvers);
                return registry.registerExploring(conceptMap, context);
            });
        }
    }

    protected AnswerState.Partial<?> applyRemapping(AnswerState.Partial<?> partial, Mapping mapping) {
        return partial.asConcludable().remap(mapping);
    }

    private boolean isCycle(AnswerState.Partial<?> partialAnswer, ConceptMap bounds) {
        AnswerState.Partial<?> ans = partialAnswer;
        while (ans.parent().isPartial()) {
            ans = ans.parent().asPartial();
            if (ans.isConcludable()
                    && registry.concludables(driver()).contains(ans.asConcludable().concludable())
                    && ans.conceptMap().equals(bounds)) {
                return true;
            }
        }
        return false;
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
