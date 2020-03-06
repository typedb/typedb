/*
 * Copyright (C) 2020 Grakn Labs
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

package grakn.core.graql.reasoner;

import grakn.core.graql.reasoner.cache.MultilevelSemanticCache;
import grakn.core.graql.reasoner.cache.RuleCacheImpl;
import grakn.core.kb.graql.reasoner.ReasonerException;
import grakn.core.kb.graql.reasoner.cache.QueryCache;
import grakn.core.kb.graql.reasoner.cache.RuleCache;

/**
 * Downcasting interfaces that are injected into reasoner into more specific instances
 * THat reasoner is already allowed to access and instantiate
 */
public class CacheCasting {

    // ---------- TODO these casts should be removed when the architecture is improved ---------

    public static MultilevelSemanticCache queryCacheCast(QueryCache cache) {
        if (cache instanceof MultilevelSemanticCache) {
            return (MultilevelSemanticCache) cache;
        } else {
            throw ReasonerException.invalidCast(cache.getClass(), MultilevelSemanticCache.class);
        }
    }

    public static RuleCacheImpl ruleCacheCast(RuleCache cache) {
        if (cache instanceof RuleCacheImpl) {
            return (RuleCacheImpl) cache;
        } else {
            throw ReasonerException.invalidCast(cache.getClass(), RuleCacheImpl.class);
        }
    }
}
