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

package ai.grakn.test.engine.lock;

import ai.grakn.engine.lock.ZookeeperReentrantLock;
import ai.grakn.engine.tasks.manager.ZookeeperConnection;
import ai.grakn.exception.EngineStorageException;
import ai.grakn.test.EngineContext;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class ZookeeperReentrantLockTest {

    private static final String LOCK_PATH = "/lock";
    private static ZookeeperConnection zookeeperConnection;

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @ClassRule
    public static EngineContext kafka = EngineContext.startKafkaServer();

    @BeforeClass
    public static void setupZK(){
        zookeeperConnection = new ZookeeperConnection();
    }

    @AfterClass
    public static void shutdownZK(){
        zookeeperConnection.close();
    }

    // this is allowed in a Reentrant lock
    @Test
    public void whenLockAcquired_ItCanBeAcquiredAgain(){
        Lock lock = new ZookeeperReentrantLock(zookeeperConnection, LOCK_PATH);

        lock.lock();

        assertThat(lock.tryLock(), is(true));

        lock.unlock();
        lock.unlock();
    }

    @Test
    public void whenLockReleased_ItCanBeAcquiredAgain(){
        Lock lock = new ZookeeperReentrantLock(zookeeperConnection, LOCK_PATH);

        lock.lock();
        lock.unlock();

        assertThat(lock.tryLock(), is(true));

        lock.unlock();
    }

    @Test
    public void whenUnownedLockIsReleased_IllegalMonitorStateExceptionThrown(){
        exception.expect(EngineStorageException.class);

        Lock lock = new ZookeeperReentrantLock(zookeeperConnection, LOCK_PATH);
        lock.unlock();
    }

    @Test
    public void whenMultipleLocks_OnlyOneAtATimeCanBeAcquired(){
        Lock lock1 = new ZookeeperReentrantLock(zookeeperConnection, LOCK_PATH);
        Lock lock2 = new ZookeeperReentrantLock(zookeeperConnection, LOCK_PATH);

        lock1.lock();

        assertThat(lock2.tryLock(), is(false));

        lock1.unlock();

        assertThat(lock2.tryLock(), is(true));
        assertThat(lock1.tryLock(), is(false));

        lock2.unlock();
    }

    @Test
    public void whenMultipleLocks_TryLockSucceedsWhenFirstLockReleased() throws InterruptedException {
        Lock lock1 = new ZookeeperReentrantLock(zookeeperConnection, LOCK_PATH);
        Lock lock2 = new ZookeeperReentrantLock(zookeeperConnection, LOCK_PATH);

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

    @Test
    public void whenLockAcquiredOnPath_AnotherCanBeAcquiredOnDifferentPath(){
        Lock lock1 = new ZookeeperReentrantLock(zookeeperConnection, LOCK_PATH);
        Lock lock2 = new ZookeeperReentrantLock(zookeeperConnection, LOCK_PATH + "2");

        assertThat(lock1.tryLock(), is(true));
        assertThat(lock2.tryLock(), is(true));

        lock1.unlock();
        lock2.unlock();
    }

    @Test
    public void whenLockInterruptiblyCalled_UnsupportedOperationThrown() throws InterruptedException {
        exception.expect(UnsupportedOperationException.class);

        Lock lock = new ZookeeperReentrantLock(zookeeperConnection, LOCK_PATH);
        lock.lockInterruptibly();
    }

    @Test
    public void whenNewConditionCalled_UnsupportedOperationThrown(){
        exception.expect(UnsupportedOperationException.class);

        Lock lock = new ZookeeperReentrantLock(zookeeperConnection, LOCK_PATH);
        lock.newCondition();
    }

}
