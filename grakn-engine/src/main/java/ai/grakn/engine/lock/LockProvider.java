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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;

/**
 * <p>
 *     This class provides the correct {@link Lock} based on how it was initialised.
 * </p>
 *
 * @author alexandraorth
 */
public class LockProvider {

    private static final Map<String, Lock> locks = new HashMap<>();

    private LockProvider(){}

    public static void add(String lockName, Lock providedLock){
        if(locks.containsKey(lockName) && !locks.get(lockName).getClass().equals(providedLock.getClass())){
            throw new RuntimeException("Lock class has already been initialised with another lock.");
        }

        locks.put(lockName, providedLock);
    }

    public static Lock getLock(String lockToObtain){
        if(!locks.containsKey(lockToObtain)){
            throw new RuntimeException("Lock is not available with name " + lockToObtain);
        }
        return locks.get(lockToObtain);
    }

    public static void clear(){
        locks.clear();
    }
}
