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

import ai.grakn.util.ErrorMessage;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.function.Function;

/**
 *
 * <p>
 *     Provide the correct locking functionality based on how the {@link LockProvider} was created.
 * </p>
 *
 * <p>
 *     Instantiate a {@link LockProvider} with a function that will create a lock to lock on the provided key.
 * </p>
 *
 * @author alexandraorth
 */
public class LockProvider {

    private static Function<String, Lock> lockProvider;
    private static Map<String, Lock> locks = new ConcurrentHashMap<>();

    private LockProvider(){}

    public static void instantiate(Function<String, Lock> provider){
        if(lockProvider == null){
            lockProvider = provider;
            return;
        }

        throw new IllegalArgumentException(ErrorMessage.LOCK_ALREADY_INSTANTIATED.getMessage());
    }

    /**
     * Uses the named lock supplier to retrieve the correct lock
     *
     * @param lockToObtain Name of the lock to obtain from the supplier
     * @return An initialized lock
     */
    public static Lock getLock(String lockToObtain){
        return locks.computeIfAbsent(lockToObtain, (lockName) -> lockProvider.apply(lockName));
    }

    public static void clear(){
        locks.clear();
        lockProvider = null;
    }
}
