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
 *
 */

package ai.grakn.engine.lock;

import com.google.common.base.Preconditions;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import redis.clients.jedis.Jedis;
import redis.clients.util.Pool;

/**
 * Wrapper for Jedis lock
 *
 * @author Domenico Corapi
 */
public class JedisLock implements Lock {

    private static final long TIMEOUT_MS = 10 * 1000;
    /**
     * Lock key path.
     */
    private String lockName;

    /**
     * Lock expiration in miliseconds.
     */
    private int expireMsecs = 60 * 1000;

    /**
     * Acquire timeout in miliseconds.
     */
    private boolean locked = false;
    private Pool<Jedis> jedis;
    private Lock lock = new ReentrantLock();

    public JedisLock(Pool<Jedis> jedis, String lockName) {
        Preconditions.checkNotNull(jedis,"JedisPool used in lock cannot be null");
        Preconditions.checkArgument(lockName != null && !lockName.isEmpty(),"Lock name not valid");
        this.jedis = jedis;
        this.lockName = lockName;
    }

    @Override
    public void lock() {
        try {
            lockInterruptibly();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
        try(Jedis r = jedis.getResource()) {
            acquire(r, TIMEOUT_MS);
        }
    }

    @Override
    public boolean tryLock() {
        try(Jedis r = jedis.getResource()) {
            return tryOnce(r);
        }
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        Preconditions.checkNotNull(unit);
        long timeout = unit.convert(time, TimeUnit.MILLISECONDS);
        try(Jedis r = jedis.getResource()) {
            return acquire(r, timeout);
        }
    }

    @Override
    public void unlock() {
        try(Jedis r = jedis.getResource()) {
            release(r);
        }
    }

    @Override
    public Condition newCondition() {
        // TODO
        throw new RuntimeException("Not implemented");
    }

    /**
     * Acquire lock.
     *
     * @return true if lock is acquired, false acquire timeouted
     * @throws InterruptedException in case of thread interruption
     */
    private boolean acquire(Jedis jedis, long timeout) throws InterruptedException {
            while (timeout >= 0) {
                lock.lock();
                try{
                    if (tryOnce(jedis)) {
                        return true;
                    }
                } finally {
                    lock.unlock();
                }
                timeout -= 100;
                Thread.sleep(100);
            }
            return false;
    }

    private boolean tryOnce(Jedis jedis) {
        long expires = System.currentTimeMillis() + expireMsecs + 1;
        String expiresStr = String.valueOf(expires);

        lock.lock();
        try{
            if (jedis.setnx(lockName, expiresStr) == 1) {
                // lock acquired
                locked = true;
                return true;
            }
        } finally {
            lock.unlock();
        }

        String currentValueStr = jedis.get(lockName);
        if (currentValueStr != null && Long.parseLong(currentValueStr) < System
                .currentTimeMillis()) {
            // lock is expired
            lock.lock();
            try{
                String oldValueStr = jedis.getSet(lockName, expiresStr);
                if (oldValueStr != null && oldValueStr.equals(currentValueStr)) {
                    // lock acquired
                    locked = true;
                    return true;
                }
            } finally {
                lock.unlock();
            }
        }
        return false;
    }

    /**
     * Acqurired lock release.
     */
    private  void release(Jedis jedis) {
        lock.lock();
        try {
            if (locked) {
                jedis.del(lockName);
                locked = false;
            }
        } finally {
            lock.unlock();
        }
    }

    public String getLockName() {
        return lockName;
    }
}
