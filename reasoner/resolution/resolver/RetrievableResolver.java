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

import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.logic.resolvable.Retrievable;
import com.vaticle.typedb.core.reasoner.resolution.ResolverRegistry;
import com.vaticle.typedb.core.reasoner.resolution.answer.AnswerState;
import com.vaticle.typedb.core.reasoner.resolution.answer.Mapping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static com.vaticle.typedb.common.collection.Collections.set;

public class RetrievableResolver extends SubsumptiveCoordinator<RetrievableResolver> {

    private static final Logger LOG = LoggerFactory.getLogger(RetrievableResolver.class);

    private final Retrievable retrievable;
    protected final Map<ConceptMap, Driver<BoundRetrievableResolver>> boundResolvers;

    public RetrievableResolver(Driver<RetrievableResolver> driver, Retrievable retrievable, ResolverRegistry registry) {
        super(driver, RetrievableResolver.class.getSimpleName() + "(pattern: " + retrievable.pattern() + ")",
              createEquivalentMappings(retrievable), registry);
        this.retrievable = retrievable;
        this.boundResolvers = new HashMap<>();
    }

    @Override
    protected Driver<BoundRetrievableResolver> getOrCreateBoundResolver(AnswerState.Partial<?> partial, ConceptMap mapped) {
        return boundResolvers.computeIfAbsent(mapped, p -> {
            LOG.debug("{}: Creating a new BoundRetrievableResolver for bounds: {}", name(), mapped);
            return registry.registerBoundRetrievable(retrievable, mapped);
        });
    }

    private static Set<Mapping> createEquivalentMappings(Retrievable retrievable) {
        // TODO: compute all possible reflexive mappings. Requires conjunction equality. For now we use an identity mapping.
        return set(Mapping.identity(retrievable.retrieves()));
    }

    @Override
    protected void initialiseDownstreamResolvers() {}

}
