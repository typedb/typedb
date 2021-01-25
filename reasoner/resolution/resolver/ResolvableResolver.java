/*
 * Copyright (C) 2021 Grakn Labs
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

package grakn.core.reasoner.resolution.resolver;

import grakn.core.concurrent.actor.Actor;
import grakn.core.reasoner.resolution.ResolverRegistry;
import grakn.core.reasoner.resolution.framework.Resolver;
import grakn.core.traversal.TraversalEngine;

public abstract class ResolvableResolver<T extends ResolvableResolver<T>> extends Resolver<T> {
    public ResolvableResolver(Actor<T> self, String name, ResolverRegistry registry, TraversalEngine traversalEngine, boolean explanations) {
        super(self, name, registry, traversalEngine, explanations);
    }
}
