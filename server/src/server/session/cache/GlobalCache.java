/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;
import grakn.core.common.config.Config;
import grakn.core.common.config.ConfigKey;
import grakn.core.concept.Label;
import grakn.core.concept.LabelId;
import grakn.core.concept.type.SchemaConcept;
import grakn.core.server.kb.concept.SchemaConceptImpl;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * <p>
 *     Tracks Knowledge Base Specific Variables
 * </p>
 *
 * <p>
 *     Caches Knowledge Base or Session specific data which is shared across transactions:
 *     <ol>
 *         <li>Schema Cache - All the types which make up the schema. This cache expires</li>
 *         <li>
 *             Label Cache - All the labels which make up the schema. This can never expire and is needed in order
 *             to perform fast lookups. Essentially it is used for mapping labels to ids.
 *         </li>
 *     <ol/>
 * </p>
 *
 *
 */
public class GlobalCache {
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    //Caches
    private final Cache<Label, SchemaConcept> cachedTypes;
    private final Map<Label, LabelId> cachedLabels;

    public GlobalCache(Config config) {
        cachedLabels = new ConcurrentHashMap<>();

        int cacheTimeout = config.getProperty(ConfigKey.SESSION_CACHE_TIMEOUT_MS);
        cachedTypes = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .expireAfterAccess(cacheTimeout, TimeUnit.MILLISECONDS)
                .build();
    }

    void populateSchemaTxCache(TransactionCache transactionCache){
        try {
            lock.writeLock().lock();

            Map<Label, SchemaConcept> cachedSchemaSnapshot = getCachedTypes();
            Map<Label, LabelId> cachedLabelsSnapshot = getCachedLabels();

            //Read central cache into transactionCache cloning only base concepts. Sets clones later
            for (SchemaConcept type : cachedSchemaSnapshot.values()) {
                transactionCache.cacheConcept(type);
            }

            //Load Labels Separately. We do this because the TypeCache may have expired.
            cachedLabelsSnapshot.forEach(transactionCache::cacheLabel);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Caches a type so that we can retrieve ontological concepts without making a DB read.
     *
     * @param label The label of the type to cache
     * @param type The type to cache
     */
    public void cacheType(Label label, SchemaConcept type) {
        cachedTypes.put(label, type);
    }

    /**
     * Caches a label so we can map type labels to type ids. This is necesssary so we can make fast
     * indexed lookups.
     *
     * @param label The label of the type to cache
     * @param id The id of the type to cache
     */
    public void cacheLabel(Label label, LabelId id) {
        cachedLabels.put(label, id);
    }

    /**
     * Reads the {@link SchemaConcept} and their {@link Label} currently in the transaction cache
     * into the graph cache. This usually happens when a commit occurs and allows us to track schema
     * mutations without having to read the graph.
     *
     * @param transactionCache The transaction cache
     */
    void readTxCache(TransactionCache transactionCache) {
        //Check if the ontology has been changed and should be flushed into this cache
        if(!cachedLabels.equals(transactionCache.getLabelCache())) {
            try {
                lock.readLock().lock();

                //Clear the cache
                cachedLabels.clear();
                cachedTypes.invalidateAll();

                //Add a new one
                cachedLabels.putAll(transactionCache.getLabelCache());
                cachedTypes.putAll(transactionCache.getSchemaConceptCache());
            } finally {
                lock.readLock().unlock();
            }
        }

        //Flush All The Internal Transaction Caches
        transactionCache.getSchemaConceptCache().values().forEach(schemaConcept
                -> SchemaConceptImpl.from(schemaConcept).txCacheFlush());
    }

    /**
     * A copy of the cached labels. This is used when creating a new transaction.
     *
     * @return an immutable copy of the cached labels.
     */
    private Map<Label, LabelId> getCachedLabels() {
        return ImmutableMap.copyOf(cachedLabels);
    }

    /**
     * A copy of the cached schema. This is used when creating a new transaction.
     *
     * @return an immutable copy of the cached schema.
     */
    public Map<Label, SchemaConcept> getCachedTypes() {
        return ImmutableMap.copyOf(cachedTypes.asMap());
    }
}
