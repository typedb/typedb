/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package ai.grakn.engine.lock;

import ai.grakn.test.rule.InMemoryRedisContext;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@RunWith(Theories.class)
public class LockTest {

    private static final String LOCK_NAME = "/lock";

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @ClassRule
    public static InMemoryRedisContext inMemoryRedisContext = InMemoryRedisContext.create();

    @DataPoints
    public static Locks[] configValues = Locks.values();

    private enum Locks {
        REDIS, NONREENTRANT;
    }

    private Lock getLock(Locks lock, String lockName){
        switch (lock){
            case REDIS:
                return new JedisLock(inMemoryRedisContext.jedisPool(), lockName);
            case NONREENTRANT:
                return new NonReentrantLock();
        }
        throw new RuntimeException("Invalid lock [" + lock + "]");
    }

    private Lock copy(Lock lock){
        if(lock instanceof JedisLock){
            return new JedisLock(inMemoryRedisContext.jedisPool(), ((JedisLock) lock).getLockName());
        } else if(lock instanceof NonReentrantLock){
            return lock;
        }
        throw new RuntimeException("Invalid lock [" + lock + "]");
    }

    @Theory
    public void whenLockReleased_ItCanBeAcquiredAgain(Locks locks){
        Lock lock = getLock(locks, LOCK_NAME);

        lock.lock();
        lock.unlock();

        assertThat(lock.tryLock(), is(true));

        lock.unlock();
    }

    @Theory
    public void whenMultipleOfSameLock_OnlyOneAtATimeCanBeAcquired(Locks locks)
            throws ExecutionException, InterruptedException {
        Lock lock1 = getLock(locks, LOCK_NAME);
        Lock lock2 = copy(lock1);
        Callable<Boolean> r = lock2::tryLock;
        ExecutorService execSvc = Executors.newSingleThreadExecutor();
        lock1.lock();
        Boolean acquired = execSvc.submit(r).get();
        assertThat(acquired, is(false));
        lock1.unlock();
        assertThat(lock2.tryLock(), is(true));
        lock2.unlock();
    }

    @Theory
    public void whenMultipleLocks_TryLockSucceedsWhenFirstLockReleased(Locks locks) throws InterruptedException {
        Lock lock1 = getLock(locks, LOCK_NAME);
        Lock lock2 = copy(lock1);

        lock1.lock();

        Thread thread = new Thread(() -> {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            lock1.unlock();
        });
        thread.run();

        assertThat(lock2.tryLock(100, TimeUnit.MILLISECONDS), is(true));
        lock2.unlock();
    }

    @Theory
    public void whenTwoLocksCreated_TheyCanBothBeAcquired(Locks locks){
        Lock lock1 = getLock(locks, LOCK_NAME + UUID.randomUUID());
        Lock lock2 = getLock(locks, LOCK_NAME + UUID.randomUUID());

        assertThat(lock1.tryLock(), is(true));
        assertThat(lock2.tryLock(), is(true));

        lock1.unlock();
        lock2.unlock();
    }

    @Theory
    public void whenGettingLockWithNullInPath_LockIsAcquired(Locks locks){
        String lockName = "/\u0000";

        Lock lock = getLock(locks, lockName);

        assertThat(lock.tryLock(), is(true));

        lock.unlock();
    }

    @Theory
    public void whenGettingLockWithIllegalCharactersInPath_LockIsAcquired(Locks locks){
        String lockName = "/\ud800";

        Lock lock = getLock(locks, lockName);

        assertThat(lock.tryLock(), is(true));

        lock.unlock();
    }
    @Theory
    public void whenGettingLockWithManyIllegalCharactersInPath_LockIsAcquired(Locks locks){
        String lockName = "/ATTRIBUTE-url-http://dbpedia.org/resource/Jorhat_College";

        Lock lock = getLock(locks, lockName);

        assertThat(lock.tryLock(), is(true));

        lock.unlock();
    }
}
