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

package ai.grakn.graph.internal;

import ai.grakn.concept.Role;

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
 *     {@link ai.grakn.concept.RelationType}, and {@link Role}.
 * </p>
 *
 * @param <V> The object it is caching
 *
 * @author fppt
 *
 */
class Cache<V> {
    //If no cache can produce the data then the database is read
    private final Supplier<V> databaseReader;

    //Transaction bound. If this is not set it does not yet exist in the scope of the transaction.
    private ThreadLocal<V> valueTx = new ThreadLocal<>();

    //Graph bound value which has already been persisted and acts as a shared component cache
    private Optional<V> valueGlobal = Optional.empty();

    Cache(Supplier<V> databaseReader){
        this.databaseReader = databaseReader;
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
        if(valueGlobal.isPresent()) value = copyGlobal();
        if(value == null) value = databaseReader.get();
        if(value == null) return null;

        valueTx.set(value);

        return valueTx.get();
    }

    /**
     * Copies the global value so it can be safely used in another variable.
     * This method is overridden by {@link CacheSet}
     *
     * @return The new value to store in the transaction bound cached value.
     */
    V copyGlobal(){
        return valueGlobal.get();
    }

    /**
     * Clears the cache.
     */
    public void clear(){
        valueTx.remove();
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
    void ifPresent(Consumer<V> modifier){
        if(isPresent()){
            modifier.accept(get());
        }
    }

    /**
     * Takes the current value in the transaction cache if it is present and puts it in the valueGlobal reference so
     * that it can be accessed via all transactions.
     */
    void flush(){
        if(isPresent() && !valueGlobal.get().equals(get())) {
            this.valueGlobal = Optional.of(get());
        }
    }

}
