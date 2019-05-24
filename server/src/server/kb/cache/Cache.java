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

package grakn.core.server.kb.cache;

import javax.annotation.Nullable;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * An internal cached object which hits the database only when it needs to.
 * This is used to cache the components of ontological concepts. i.e. the fields of Type,
 * RelationType, and Role.
 * The Cache object is transaction bound as it's associate to a Concept.
 *
 * @param <V> The object it is caching
 */
public class Cache<V> {
    //If no cache can produce the data then the database is read
    private final Supplier<V> databaseReader;
    private V cacheValue = null;
    private boolean valueRetrieved;

    public Cache(Supplier<V> databaseReader) {
        this.databaseReader = databaseReader;
        this.valueRetrieved = false;
    }

    /**
     * Retrieves the object in the cache. If nothing is cached the database is read.
     *
     * @return The cached object.
     */
    @Nullable
    public V get() {
        if (!valueRetrieved) {
            cacheValue = databaseReader.get();
            valueRetrieved = true;
        }
        return cacheValue;
    }

    /**
     * Explicitly set the cache to a provided value.
     *
     * @param value the value to be cached
     */
    public void set(@Nullable V value) {
        cacheValue = value;
        valueRetrieved = true;
    }

    /**
     * @return true if a value has been retrieve and save into the current cache
     */
    public boolean isPresent() {
        return valueRetrieved;
    }

    /**
     * Mutates the cached value if something is cached. Otherwise does nothing.
     *
     * @param modifier the mutator function.
     */
    public void ifPresent(Consumer<V> modifier) {
        if (isPresent()) {
            modifier.accept(get());
        }
    }

}
