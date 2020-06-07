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
 */

package grakn.core.graph.diskstorage.keycolumnvalue.cache;

import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import grakn.core.graph.core.JanusGraphException;
import grakn.core.graph.diskstorage.BackendException;
import grakn.core.graph.diskstorage.EntryList;
import grakn.core.graph.diskstorage.StaticBuffer;
import grakn.core.graph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import grakn.core.graph.diskstorage.keycolumnvalue.KeySliceQuery;
import grakn.core.graph.diskstorage.keycolumnvalue.SliceQuery;
import grakn.core.graph.diskstorage.keycolumnvalue.StoreTransaction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class KCVSExpirationCache extends KCVSCache {

    private static final int GUAVA_CACHE_ENTRY_SIZE = 104;
    private static final int OBJECT_HEADER = 12;
    private static final int OBJECT_REFERENCE = 8;
    private static final int STATICARRAYBUFFER_RAW_SIZE = OBJECT_HEADER + 2 * 4 + 6 + (OBJECT_REFERENCE + OBJECT_HEADER + 8); // 6 = overhead & padding, (byte[] array)

    //Weight estimation
    private static final int STATIC_ARRAY_BUFFER_SIZE = STATICARRAYBUFFER_RAW_SIZE + 10; // 10 = last number is average length
    private static final int KEY_QUERY_SIZE = OBJECT_HEADER + 4 + 1 + 3 * (OBJECT_REFERENCE + STATIC_ARRAY_BUFFER_SIZE); // object_size + int + boolean + 3 static buffers

    private static final int INVALIDATE_KEY_FRACTION_PENALTY = 1000;
    private static final int PENALTY_THRESHOLD = 5;

    private volatile CountDownLatch penaltyCountdown;

    private final Cache<KeySliceQuery, EntryList> cache;
    private final ConcurrentHashMap<StaticBuffer, Long> expiredKeys;

    private final long cacheTimeMS;
    private final long invalidationGracePeriodMS;
    private final CleanupThread cleanupThread;

    public KCVSExpirationCache(KeyColumnValueStore store, long cacheTimeMS, long invalidationGracePeriodMS, long maximumByteSize) {
        super(store);
        Preconditions.checkArgument(System.currentTimeMillis() + 1000L * 3600 * 24 * 365 * 100 + cacheTimeMS > 0, "Cache expiration time too large, overflow may occur: %s", cacheTimeMS);
        this.cacheTimeMS = cacheTimeMS;
        int concurrencyLevel = Runtime.getRuntime().availableProcessors();
        Preconditions.checkArgument(invalidationGracePeriodMS >= 0, "Invalid expiration grace period: %s", invalidationGracePeriodMS);
        this.invalidationGracePeriodMS = invalidationGracePeriodMS;
        CacheBuilder<KeySliceQuery, EntryList> cachebuilder = CacheBuilder.newBuilder()
                .maximumWeight(maximumByteSize)
                .concurrencyLevel(concurrencyLevel)
                .initialCapacity(1000)
                .expireAfterWrite(cacheTimeMS, TimeUnit.MILLISECONDS)
                .weigher((keySliceQuery, entries) -> GUAVA_CACHE_ENTRY_SIZE + KEY_QUERY_SIZE + entries.getByteSize());

        cache = cachebuilder.build();
        expiredKeys = new ConcurrentHashMap<>(50, 0.75f, concurrencyLevel);
        penaltyCountdown = new CountDownLatch(PENALTY_THRESHOLD);

        cleanupThread = new CleanupThread();
        cleanupThread.start();
    }

    @Override
    public EntryList getSlice(KeySliceQuery query, StoreTransaction txh) throws BackendException {
        if (isExpired(query)) {
            return store.getSlice(query, unwrapTx(txh));
        }

        try {
            return cache.get(query, () -> store.getSlice(query, unwrapTx(txh)));
        } catch (Exception e) {
            if (e instanceof JanusGraphException) {
                throw (JanusGraphException) e;
            } else if (e.getCause() instanceof JanusGraphException) {
                throw (JanusGraphException) e.getCause();
            } else {
                throw new JanusGraphException(e);
            }
        }
    }

    @Override
    public Map<StaticBuffer, EntryList> getSlice(List<StaticBuffer> keys, SliceQuery query, StoreTransaction txh) throws BackendException {
        Map<StaticBuffer, EntryList> results = new HashMap<>(keys.size());
        List<StaticBuffer> remainingKeys = new ArrayList<>(keys.size());
        KeySliceQuery[] ksqs = new KeySliceQuery[keys.size()];
        //Find all cached queries
        for (int i = 0; i < keys.size(); i++) {
            StaticBuffer key = keys.get(i);
            ksqs[i] = new KeySliceQuery(key, query);
            EntryList result = null;
            if (!isExpired(ksqs[i])) result = cache.getIfPresent(ksqs[i]);
            else ksqs[i] = null;
            if (result != null) results.put(key, result);
            else remainingKeys.add(key);
        }
        //Request remaining ones from backend
        if (!remainingKeys.isEmpty()) {
            Map<StaticBuffer, EntryList> subresults = store.getSlice(remainingKeys, query, unwrapTx(txh));
            for (int i = 0; i < keys.size(); i++) {
                StaticBuffer key = keys.get(i);
                EntryList subresult = subresults.get(key);
                if (subresult != null) {
                    results.put(key, subresult);
                    if (ksqs[i] != null) cache.put(ksqs[i], subresult);
                }
            }
        }
        return results;
    }

    // Invalidate workflow:
    // - a key gets invalidated by Cache Transaction
    // - we move the key in expiredKeys and we generate a "valid period" -> this period becomes the value
    // - randomly countdown on the penalty if something something
    @Override
    public void invalidate(StaticBuffer key, List<StaticBuffer> entries) {
        Preconditions.checkArgument(!hasValidateKeysOnly() || entries.isEmpty());
        expiredKeys.put(key, getExpirationTime());
        if (Math.random() < (1.0 / INVALIDATE_KEY_FRACTION_PENALTY)) penaltyCountdown.countDown();
    }

    @Override
    public void close() throws BackendException {
        cleanupThread.stopThread();
        super.close();
    }

    private boolean isExpired(KeySliceQuery query) {
        Long until = expiredKeys.get(query.getKey());
        if (until == null) return false;
        if (isBeyondExpirationTime(until)) {
            expiredKeys.remove(query.getKey(), until);
            return false;
        }
        //We suffer a cache miss, hence decrease the count down
        penaltyCountdown.countDown();
        return true;
    }

    private long getExpirationTime() {
        return System.currentTimeMillis() + cacheTimeMS;
    }

    private boolean isBeyondExpirationTime(long until) {
        return until < System.currentTimeMillis();
    }

    // return time expressing how long a key has been cached for
    private long getAge(long until) {
        return System.currentTimeMillis() - (until - cacheTimeMS);
    }

    // Fancy daemon thread used to read from expiredKeys and decide whether the keys in there should be actually be removed from the real cache.
    // This is because when a key gets invalidated it's not immediately removed from the cache!
    // It's possible to keep expired keys in cache for a invalidationGracePeriodMS
    // Does all of this really lead to a smarter caching? 🤔
    private class CleanupThread extends Thread {

        private boolean stop = false;

        CleanupThread() {
            this.setDaemon(true);
            this.setName("ExpirationStoreCache-" + getId());
        }

        @Override
        public void run() {
            while (true) {
                if (stop) {
                    return;
                }
                try {
                    penaltyCountdown.await();
                } catch (InterruptedException e) {
                    if (stop) {
                        return;
                    } else {
                        throw new RuntimeException("Cleanup thread got interrupted", e);
                    }
                }
                //Do clean up work by invalidating all entries for expired keys
                Map<StaticBuffer, Long> keysToInvalidate = new HashMap<>(expiredKeys.size());
                for (Map.Entry<StaticBuffer, Long> expKey : expiredKeys.entrySet()) {
                    //if it's already beyond the main cache's expiration time, then the cache will have already invalidated the key by now,
                    // so just get rid of it also in expiredKeys
                    if (isBeyondExpirationTime(expKey.getValue())) {
                        expiredKeys.remove(expKey.getKey(), expKey.getValue());
                    }
                    // If we get here it's because the key is not old enough for the main cache to delete it, so let's check if we need to speed up the invalidation:
                    // if the key has been cached for a time between invalidationGracePeriodMS and cacheTimeMS, we force the deletion from the main cache
                    else if (getAge(expKey.getValue()) >= invalidationGracePeriodMS) {
                        keysToInvalidate.put(expKey.getKey(), expKey.getValue());
                    }
                }
                for (KeySliceQuery ksq : cache.asMap().keySet()) {
                    if (keysToInvalidate.containsKey(ksq.getKey())) cache.invalidate(ksq);
                }
                penaltyCountdown = new CountDownLatch(PENALTY_THRESHOLD);
                for (Map.Entry<StaticBuffer, Long> expKey : keysToInvalidate.entrySet()) {
                    expiredKeys.remove(expKey.getKey(), expKey.getValue());
                }
            }
        }

        void stopThread() {
            stop = true;
            this.interrupt();
        }
    }


}
