/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2019 Grakn Labs Ltd
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

package grakn.core.server.session.cache;

import grakn.core.graql.reasoner.cache.MultilevelSemanticCache;

/**
 * Implemented CacheProvider as a provider to have a idempotent `get()` methods.
 * This ensures that if the provider is shared, everyone receives the same instances of the Caches
 */
public class CacheProvider {

    private final RuleCache ruleCache;
    private final MultilevelSemanticCache queryCache;
    private final TransactionCache transactionCache;

    public CacheProvider(KeyspaceSchemaCache keyspaceSchemaCache) {
        this.ruleCache = new RuleCache();
        this.queryCache = new MultilevelSemanticCache();
        this.transactionCache = new TransactionCache(keyspaceSchemaCache);
    }

    public RuleCache getRuleCache() {
        return ruleCache;
    }

    public MultilevelSemanticCache getQueryCache() {
        return queryCache;
    }

    public TransactionCache getTransactionCache() {
        return transactionCache;
    }
}
