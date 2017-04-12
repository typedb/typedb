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

import ai.grakn.engine.tasks.manager.ZookeeperConnection;
import ai.grakn.exception.EngineStorageException;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * <p>
 *     A fully distributed lock that is globally synchronous. No two engines or tasks will think they hold the same lock.
 * </p>
 *
 * @author alexandraorth
 */
public class ZookeeperLock implements Lock {

    private final String lockPath;
    private final InterProcessSemaphoreMutex mutex;

    public ZookeeperLock(ZookeeperConnection zookeeper, String lockPath){
        this.lockPath = lockPath;
        this.mutex = new InterProcessSemaphoreMutex(zookeeper.connection(), lockPath);
    }

    /**
     * Acquires the lock. If the lock is not available, stalls the current thread until it is available.
     * See {@link InterProcessSemaphoreMutex#acquire()} for more information.
     *
     * @throws EngineStorageException when there are Zookeeper connection issues.
     */
    @Override
    public void lock() {
        try {
            mutex.acquire();
        } catch (Exception e) {
            throw new EngineStorageException(e);
        }
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
        throw new UnsupportedOperationException();
    }

    /**
     * Acquire the lock if it is available within 1 second of calling.
     * Note: Checking if the lock is available and acquiring the lock IS NOT atomic.
     *
     * @return {@code true} if the lock was acquired and
     *         {@code false} otherwise
     */
    @Override
    public boolean tryLock() {
        try {
            if (mutex.acquire(100, MILLISECONDS)) {
                return true;
            }
        } catch (Exception e) {
            throw new EngineStorageException(e);
        }

        return false;
    }

    /**
     * Acquire the lock if it is available within {@param time} using {@param unit}.
     *
     * @param time amount of time to wait for the lock to be available
     * @param unit unit of time that qualifies amount of time to wait for lock
     * @return @return {@code true} if the lock was acquired and
     *         {@code false} otherwise
     * @throws InterruptedException
     */
    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        try {
            if (mutex.acquire(time, unit)) {
                return true;
            }
        } catch (Exception e) {
            throw new EngineStorageException(e);
        }

        return false;
    }

    /**
     *
     *
     * @throws EngineStorageException when there are Zookeeper connection issues.
     */
    @Override
    public void unlock() {
        try {
            mutex.release();
        } catch (Exception e) {
            throw new EngineStorageException(e);
        }
    }

    @Override
    public Condition newCondition() {
        throw new UnsupportedOperationException();
    }

    public String getLockPath() {
        return lockPath;
    }
}
