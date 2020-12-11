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

package grakn.core.common.cache;

import com.github.benmanes.caffeine.cache.Caffeine;

import java.util.function.Function;

import static java.util.concurrent.TimeUnit.MINUTES;

public class CommonCache<KEY, VALUE> {

    private static final int CACHE_SIZE = 10_000; // TODO: parameterise this through grakn.properties
    private static final int CACHE_TIMEOUT_MINUTES = 1_440;
    private final com.github.benmanes.caffeine.cache.Cache<KEY, VALUE> cache;

    public CommonCache() {
        this(CACHE_SIZE, CACHE_TIMEOUT_MINUTES);
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
