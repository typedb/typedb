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

package ai.grakn.engine.lock;

import ai.grakn.test.rule.InMemoryRedisContext;
import com.google.common.base.Stopwatch;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import redis.clients.jedis.JedisPool;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

public class JedisLockTest {

    public static final String LOCK_NAME = "LOCK_NAME";
    private static JedisPool jedisPool;

    @ClassRule
    public static InMemoryRedisContext inMemoryRedisContext = InMemoryRedisContext.create();

    @BeforeClass
    public static void setupClass() {
        jedisPool = inMemoryRedisContext.jedisPool();
    }

    @AfterClass
    public static void tearDownClass() throws InterruptedException {
        jedisPool.close();
    }

    @Test
    public void whenOtherThreadHasLock_TryLockFails() throws Exception {
        Lock lock = new JedisLock(jedisPool, LOCK_NAME);
        lock.lock();

        Thread t = new Thread(() -> {
            Lock lock1 = new JedisLock(jedisPool, LOCK_NAME);
            Assert.assertFalse(lock1.tryLock());
        });

        t.start();
        t.join();
        lock.unlock();

        Thread t2 = new Thread(() -> {
            Lock lock1 = new JedisLock(jedisPool, LOCK_NAME);
            Assert.assertTrue(lock1.tryLock());
        });

        t2.start();
        t2.join();
    }

    @Test
    public void whenOtherThreadHasLock_TryLockTimesOut() throws Exception {
        Lock lock = new JedisLock(jedisPool, LOCK_NAME);
        lock.lock();

        Thread t = new Thread(() -> {
            Lock lock1 = new JedisLock(jedisPool, LOCK_NAME);
            try {
                Stopwatch stopwatch = Stopwatch.createStarted();
                int time = 5000;
                Assert.assertFalse(lock1.tryLock(time, TimeUnit.MILLISECONDS));
                Assert.assertTrue(stopwatch.stop().elapsed(TimeUnit.MILLISECONDS) > time/2 /*Leaving some margin*/);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        t.start();
        t.join();
        lock.unlock();
    }
}