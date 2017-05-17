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
import java.util.regex.Pattern;
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

    private final String[] illegalZKCharacters = {
            "\u0000",
            "[\u0001-\u0019]",
            "[\u007F-\u009F]",
            "[\ud800-\uF8FF]",
            "[\uFFF0-\uFFFF]",
            "\\.",
            "\\..",
            Pattern.quote("zookeeper")}; //remove these characters

    private final String lockPath;
    private final InterProcessSemaphoreMutex mutex;

    public ZookeeperLock(ZookeeperConnection zookeeper, String lockPath){
        this.lockPath = sanitizePath(lockPath);
        this.mutex = new InterProcessSemaphoreMutex(zookeeper.connection(), sanitizePath(lockPath));
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

    /**
     * Sanitize the provided path such that it is a valid Zookeeper path.
     *
     * According to the ZK docs (https://zookeeper.apache.org/doc/r3.1.2/zookeeperProgrammers.html), the following in paths is invalid:
     *
     * 1. The null character (\u0000) cannot be part of a path name. (This causes problems with the C binding.)
     * 2. The following characters can't be used because they don't display well, or render in confusing ways: \u0001 - \u0019 and \u007F - \u009F.
     * 3. The following characters are not allowed: \ud800 -uF8FFF, \uFFF0 - uFFFF.
     * 4. The "." character can be used as part of another name, but "." and ".." cannot alone be used to indicate a node along a path, because ZooKeeper doesn't use relative paths. The following would be invalid: "/a/b/./c" or "/a/b/../c".
     * 5. The token "zookeeper" is reserved.
     *
     *
     * @param lockPath Path potentially containing characters ZK considers illegal
     * @return Path will all illegal characters replaced
     */
    private String sanitizePath(String lockPath){
        for(String illegalCharRange:illegalZKCharacters){
            lockPath = lockPath.replaceAll(illegalCharRange, "*");
        }

        lockPath = lockPath.replaceAll("//", "/");

        return lockPath;
    }
}
