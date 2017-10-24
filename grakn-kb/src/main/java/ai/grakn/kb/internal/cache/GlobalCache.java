/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.kb.internal.cache;

import ai.grakn.concept.Label;
import ai.grakn.concept.LabelId;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.kb.internal.GraknTxAbstract;
import ai.grakn.kb.internal.concept.SchemaConceptImpl;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Properties;
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
 * @author fppt
 *
 */
public class GlobalCache {
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private static final int DEFAULT_CACHE_TIMEOUT_MS = 600_000;

    //Caches
    private final Cache<Label, SchemaConcept> cachedTypes;
    private final Map<Label, LabelId> cachedLabels;

    public GlobalCache(Properties properties) {
        cachedLabels = new ConcurrentHashMap<>();

        int cacheTimeout = Integer.parseInt(properties
                .getProperty(GraknTxAbstract.NORMAL_CACHE_TIMEOUT_MS,
                        String.valueOf(DEFAULT_CACHE_TIMEOUT_MS)));
        cachedTypes = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .expireAfterAccess(cacheTimeout, TimeUnit.MILLISECONDS)
                .build();
    }

    void populateSchemaTxCache(TxCache txCache){
        try {
            lock.writeLock().lock();

            Map<Label, SchemaConcept> cachedSchemaSnapshot = getCachedTypes();
            Map<Label, LabelId> cachedLabelsSnapshot = getCachedLabels();

            //Read central cache into txCache cloning only base concepts. Sets clones later
            for (SchemaConcept type : cachedSchemaSnapshot.values()) {
                txCache.cacheConcept(type);
            }

            //Load Labels Separately. We do this because the TypeCache may have expired.
            cachedLabelsSnapshot.forEach(txCache::cacheLabel);
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
     * @param txCache The transaction cache
     */
    void readTxCache(TxCache txCache) {
        //Check if the ontology has been changed and should be flushed into this cache
        if(!cachedLabels.equals(txCache.getLabelCache())) {
            try {
                lock.readLock().lock();

                //Clear the cache
                cachedLabels.clear();
                cachedTypes.invalidateAll();

                //Add a new one
                cachedLabels.putAll(txCache.getLabelCache());
                cachedTypes.putAll(txCache.getSchemaConceptCache());
            } finally {
                lock.readLock().unlock();
            }
        }

        //Flush All The Internal Transaction Caches
        txCache.getSchemaConceptCache().values().forEach(schemaConcept
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
