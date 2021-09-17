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

import com.vaticle.typedb.core.logic.resolvable.Retrievable;
import com.vaticle.typedb.core.reasoner.resolution.ResolverRegistry;
import com.vaticle.typedb.core.reasoner.resolution.answer.AnswerState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RetrievableResolver extends SubsumptiveCoordinator<RetrievableResolver, BoundRetrievableResolver> {

    private static final Logger LOG = LoggerFactory.getLogger(RetrievableResolver.class);

    private final Retrievable retrievable;

    public RetrievableResolver(Driver<RetrievableResolver> driver, Retrievable retrievable, ResolverRegistry registry) {
        super(driver, RetrievableResolver.class.getSimpleName() + "(pattern: " + retrievable.pattern() + ")", registry);
        this.retrievable = retrievable;
    }

    @Override
    Driver<BoundRetrievableResolver> getOrCreateWorker(AnswerState.Partial<?> partial) {
        return workers.computeIfAbsent(partial.conceptMap(), p -> {
            LOG.debug("{}: Creating a new BoundRetrievableResolver for bounds: {}", name(), partial);
            return registry.registerBoundRetrievable(retrievable, partial.conceptMap());
        });
    }

    @Override
    protected void initialiseDownstreamResolvers() {}

}
