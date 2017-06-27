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

package ai.grakn.engine.lock;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.function.BiFunction;

/**
 *
 * <p>
 *     Provide the correct locking functionality based on how the {@link GenericLockProvider} was created.
 * </p>
 *
 * <p>
 *     Instantiate a {@link GenericLockProvider} with a function that will create a lock to lock on the provided key. The provided
 *     function is a {@link BiFunction}. THe first argument to the function is the name of the lock the user asked for,
 *     the second argument is the existing lock from the {@link GenericLockProvider} map.
 * </p>
 *
 * @author alexandraorth
 */
public class GenericLockProvider implements LockProvider {

    public static final BiFunction<String, Lock, Lock> LOCAL_LOCK_FUNCTION = (lockName, existingLock) ->
            existingLock != null ? existingLock : new NonReentrantLock();
    private BiFunction<String, Lock, Lock> lockProvider;

    //TODO THIS IS A POTENTIAL MASSIVE MEMORY LEAK MAP IS NEVER EMPTIED
    private static Map<String, Lock> locks = new ConcurrentHashMap<>();

    public GenericLockProvider(){
        this(LOCAL_LOCK_FUNCTION);
    }

    public GenericLockProvider(BiFunction<String, Lock, Lock> provider){
        lockProvider = provider;
    }

    /**
     * Uses the named lock function to retrieve the correct lock
     *
     * @param lockToObtain Name of the lock to obtain from the supplier
     * @return An initialized lock
     */
    public Lock getLock(String lockToObtain){
        return locks.compute(lockToObtain, (existingLockName, existingLock) -> lockProvider.apply(lockToObtain, existingLock));
    }

    public void clear(){
        locks.clear();
        lockProvider = null;
    }
}
