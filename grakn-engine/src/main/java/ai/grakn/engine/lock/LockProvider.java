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
import java.util.function.Supplier;

/**
 * <p>
 *     This class provides the correct {@link Lock} based the supplier that was provided when it was
 *     initialized.
 * </p>
 *
 * @author alexandraorth
 */
public class LockProvider {

    private static final Map<String, Supplier<Lock>> locks = new ConcurrentHashMap<>();

    private LockProvider(){}

    /**
     * Instantiates a lock supplier with a name.
     *
     * @param lockName Name of the lock to supply
     * @param instantiateLock Supplier of the lock
     */
    public static void add(String lockName, Supplier<Lock> instantiateLock){
        locks.put(lockName, instantiateLock);
    }

    /**
     * Uses the named lock supplier to retrieve the correct lock
     *
     * @param lockToObtain Name of the lock to obtain from the supplier
     * @return An initialized lock
     */
    public static Lock getLock(String lockToObtain){
        if(!locks.containsKey(lockToObtain)){
            throw new RuntimeException("Lock is not available with name " + lockToObtain);
        }

        // Instantiate a new lock
        return locks.get(lockToObtain).get();
    }

    public static void clear(){
        locks.clear();
    }
}
