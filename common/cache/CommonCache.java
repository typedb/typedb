/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.common.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import java.util.function.Function;

import static java.util.concurrent.TimeUnit.MINUTES;

public class CommonCache<KEY, VALUE> {

    private static final int CACHE_SIZE = 10_000; // TODO: parameterise this through typedb.properties
    private static final int CACHE_TIMEOUT_MINUTES = 1_440;
    private final Cache<KEY, VALUE> cache;

    public CommonCache() {
        this(CACHE_SIZE, CACHE_TIMEOUT_MINUTES);
    }

    public CommonCache(int size) {
        this(size, CACHE_TIMEOUT_MINUTES);
    }

    public CommonCache(int size, int timeoutMinutes) {
        cache = Caffeine.newBuilder().maximumSize(size).expireAfterAccess(timeoutMinutes, MINUTES).build();
    }

    public VALUE get(KEY key, Function<KEY, VALUE> function) {
        return cache.get(key, function);
    }

    public void invalidate(KEY key) { cache.invalidate(key); }

    public void put(KEY key, VALUE value) {
        cache.put(key, value);
    }

    public VALUE getIfPresent(KEY key) { return cache.getIfPresent(key); }

    public void clear() {
        cache.invalidateAll();
    }
}
