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

package grakn.core.kb.keyspace;

import grakn.core.kb.concept.api.LabelId;
import graql.lang.statement.Label;

import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Keyspace cache contains:
 * - Label Cache - Map labels to IDs for fast lookups
 * <p>
 * This cache is shared across sessions and transactions to the same keyspace, and kept in sync
 * on commit.
 */
public class KeyspaceSchemaCache {
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final LabelCache labelCache;

    public KeyspaceSchemaCache() {
        labelCache = new LabelCache();
    }

    public ReentrantReadWriteLock concurrentUpdateLock() {
        return lock;
    }

    /**
     * Caches a label so we can map type labels to type ids. This is necessary so we can make fast
     * indexed lookups.
     *
     * @param label The label of the type to cache
     * @param id    The id of the type to cache
     */
    public void cacheLabel(Label label, LabelId id) {
        labelCache.cacheCompleteLabel(label, id);
    }


    /**
     * Reads the SchemaConcept labels currently in the transaction cache
     * into the keyspace cache. This happens when a commit occurs and allows us to track schema
     * mutations without having to read the graph.
     */
    public void overwriteCache(LabelCache modifiedLabelCache) {
        try {
            lock.writeLock().lock();
            labelCache.clear();
            labelCache.absorb(modifiedLabelCache);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * A copy of the cached labels. This is used when creating a new transaction.
     *
     * @return an immutable copy of the cached labels.
     */
    public LabelCache labelCacheCopy() {
        return new LabelCache(labelCache);
    }


    public boolean isEmpty(){
        return labelCache.isEmpty();
    }

    public boolean cacheMatches(LabelCache cache) {
        return labelCache.equals(cache);
    }
}
