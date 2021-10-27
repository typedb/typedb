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
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.logic.Rule;
import com.vaticle.typedb.core.reasoner.resolution.ResolverRegistry;
import com.vaticle.typedb.core.reasoner.resolution.answer.AnswerState.Partial;
import com.vaticle.typedb.core.reasoner.resolution.answer.Mapping;
import com.vaticle.typedb.core.reasoner.resolution.framework.Resolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static com.vaticle.typedb.common.collection.Collections.set;

public class ConclusionResolver extends SubsumptiveCoordinator<ConclusionResolver> {

    private static final Logger LOG = LoggerFactory.getLogger(ConclusionResolver.class);

    private final Rule.Conclusion conclusion;
    protected final Map<ConceptMap, Driver<BoundConclusionResolver>> boundResolvers;

    public ConclusionResolver(Driver<ConclusionResolver> driver, Rule.Conclusion conclusion, ResolverRegistry registry) {
        super(driver, ConclusionResolver.class.getSimpleName() + "(" + conclusion + ")", createEquivalentMappings(conclusion),
              registry);
        this.conclusion = conclusion;
        this.isInitialised = false;
        this.boundResolvers = new HashMap<>();
    }

    @Override
    protected void initialiseDownstreamResolvers() {
        LOG.debug("{}: initialising downstream resolvers", name());
        try {
            registry.registerCondition(conclusion.rule().condition());
            isInitialised = true;
        } catch (TypeDBException e) {
            terminate(e);
        }
    }

    private static Set<Mapping> createEquivalentMappings(Rule.Conclusion conclusion) {
        // TODO: compute all possible reflexive mappings. Requires computation over the rule's `when`, requiring
        //  conjunction equality. For now we use an identity mapping.
        return set(Mapping.identity(conclusion.retrievableIds()));
    }

    @Override
    protected Driver<? extends Resolver<?>> getOrCreateBoundResolver(Partial<?> partial, ConceptMap mapped) {
        return boundResolvers.computeIfAbsent(mapped, p -> {
            LOG.debug("{}: Creating a new BoundConclusionResolver for bounds: {}", name(), mapped);
            return registry.registerBoundConclusion(conclusion, mapped);
        });
    }

    @Override
    public String toString() {
        return name();
    }

    public Rule.Conclusion conclusion() {
        return conclusion;
    }
}
