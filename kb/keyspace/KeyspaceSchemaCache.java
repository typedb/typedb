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

import com.google.common.collect.ImmutableMap;
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.concept.api.LabelId;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
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
    private final Map<Label, LabelId> cachedLabels;

    public KeyspaceSchemaCache() {
        cachedLabels = new ConcurrentHashMap<>();
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
        cachedLabels.put(label, id);
    }


    /**
     * Reads the SchemaConcept labels currently in the transaction cache
     * into the keyspace cache. This happens when a commit occurs and allows us to track schema
     * mutations without having to read the graph.
     */
    public void overwriteCache(Map<Label, LabelId> modifiedLabelCache) {
        try {
            lock.writeLock().lock();
            cachedLabels.clear();
            cachedLabels.putAll(modifiedLabelCache);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * A copy of the cached labels. This is used when creating a new transaction.
     *
     * @return an immutable copy of the cached labels.
     */
    public Map<Label, LabelId> labelCacheCopy() {
        return ImmutableMap.copyOf(cachedLabels);
    }


    public boolean isEmpty(){
        return cachedLabels.isEmpty();
    }

    public boolean cacheMatches(Map<Label, LabelId> cache) {
        return cachedLabels.equals(cache);
    }
}
