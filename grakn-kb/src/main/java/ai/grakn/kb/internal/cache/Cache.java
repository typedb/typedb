/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.kb.internal.cache;

import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.kb.internal.concept.ConceptImpl;

import javax.annotation.Nullable;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * <p>
 *     An internal cached element
 * </p>
 *
 * <p>
 *     An internal cached object which hits the database only when it needs to.
 *     This is used to cache the components of ontological concepts. i.e. the fields of {@link ai.grakn.concept.Type},
 *     {@link RelationshipType}, and {@link Role}.
 * </p>
 *
 * @param <V> The object it is caching
 *
 * @author fppt
 *
 */
public class Cache<V> {
    //If no cache can produce the data then the database is read
    private final Supplier<V> databaseReader;

    //Use to copy the cached value safely
    private final Cacheable<V> cacheable;

    //Transaction bound. If this is not set it does not yet exist in the scope of the transaction.
    private final ThreadLocal<V> valueTx = new ThreadLocal<>();

    //Globally bound value which has already been persisted and acts as a shared component cache
    private Optional<V> valueGlobal = Optional.empty();

    //Flag indicating if this Cache should be flushed into the Session cache upon being disposed of
    private final boolean isSessionCache;

    //Flag indicating if this Cache can be cleared.
    // If this is false then the owner object must be deleted and garabe collected for the cache to die
    private final boolean isClearable;

    private Cache(CacheOwner owner, Cacheable<V> cacheable, boolean isSessionCache, boolean isClearable, Supplier<V> databaseReader){
        this.isSessionCache = isSessionCache;
        this.isClearable = isClearable;
        this.cacheable = cacheable;
        this.databaseReader = databaseReader;
        owner.registerCache(this);
    }

    /**
     * Creates a {@link Cache} that will only exist within the context of a Transaction
     */
    public static Cache createTxCache(CacheOwner owner, Cacheable cacheable, Supplier databaseReader){
        return new Cache(owner, cacheable, false, true, databaseReader);
    }

    /**
     * Creates a {@link Cache} that will only flush to a central shared cache then the Transaction is disposed off
     */
    public static Cache createSessionCache(CacheOwner owner, Cacheable cacheable, Supplier databaseReader){
        return new Cache(owner, cacheable, true, true, databaseReader);
    }

    /**
     * Creates a session level {@link Cache} which cannot be cleared.
     * When creating these types of {@link Cache}s the only way to get rid of them is to remove the owner {@link ConceptImpl}
     */
    public static Cache createPersistentCache(CacheOwner owner, Cacheable cacheable, Supplier databaseReader) {
        return new Cache(owner, cacheable, true, false, databaseReader);
    }

    /**
     * Retrieves the object in the cache. If nothing is cached the database is read.
     *
     * @return The cached object.
     */
    @Nullable
    public V get(){
        V value = valueTx.get();

        if(value != null) return value;
        if(valueGlobal.isPresent()) value = cacheable.copy(valueGlobal.get());
        if(value == null) value = databaseReader.get();
        if(value == null) return null;

        valueTx.set(value);

        return valueTx.get();
    }

    /**
     * Clears the cache.
     */
    public void clear(){
        if(isClearable) {
            valueTx.remove();
        }
    }

    /**
     * Explicitly set the cache to a provided value
     *
     * @param value the value to be cached
     */
    public void set(@Nullable V value){
        valueTx.set(value);
    }

    /**
     *
     * @return true if there is anything stored in the cache
     */
    public boolean isPresent(){
        return valueTx.get() != null || valueGlobal.isPresent();
    }

    /**
     * Mutates the cached value if something is cached. Otherwise does nothing.
     *
     * @param modifier the mutator function.
     */
    public void ifPresent(Consumer<V> modifier){
        if(isPresent()){
            modifier.accept(get());
        }
    }

    /**
     * Takes the current value in the transaction cache if it is present and puts it in the valueGlobal reference so
     * that it can be accessed via all transactions.
     */
    public void flush(){
        if(isSessionCache && isPresent()) {
            V newValue = get();
            if(!valueGlobal.isPresent() || !valueGlobal.get().equals(newValue)) valueGlobal = Optional.of(get());
        }
    }
}
