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
    //Flag indicating if this Cache can be cleared.
    // If this is false then the owner object must be deleted and garbage collected for the cache to die
    private final boolean isClearable;
    private boolean valueRetrieved;

    private Cache(CacheOwner owner, boolean isClearable, Supplier<V> databaseReader) {
        this.isClearable = isClearable;
        this.databaseReader = databaseReader;
        this.valueRetrieved = false;
        owner.registerCache(this);
    }

    /**
     * Creates a Cache that will only exist within the context of a TransactionOLTP
     */
    public static Cache create(CacheOwner owner, Supplier databaseReader) {
        return new Cache(owner, true, databaseReader);
    }

    /**
     * Creates a session level Cache which cannot be cleared.
     * When creating these types of Caches the only way to get rid of them is to remove the owner ConceptImpl
     */
    public static Cache createPersistentCache(CacheOwner owner, Supplier databaseReader) {
        return new Cache(owner, false, databaseReader);
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
     * Clears the cache.
     */
    public void clear() {
        if (isClearable) {
            cacheValue = null;
            valueRetrieved = false;
        }
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
