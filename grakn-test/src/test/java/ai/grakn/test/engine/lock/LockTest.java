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

import ai.grakn.engine.lock.NonReentrantLock;
import ai.grakn.engine.lock.ZookeeperLock;
import ai.grakn.engine.tasks.manager.ZookeeperConnection;
import ai.grakn.test.EngineContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@RunWith(Theories.class)
public class LockTest {

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

    @DataPoints
    public static Locks[] configValues = Locks.values();

    private static enum Locks {
        ZOOKEEPER, NONREENTRANT;
    }

    private Lock getLock(Locks lock){
        switch (lock){
            case ZOOKEEPER:
                return new ZookeeperLock(zookeeperConnection, LOCK_PATH + UUID.randomUUID());
            case NONREENTRANT:
                return new NonReentrantLock();
        }
        throw new RuntimeException("Invalid lock [" + lock + "]");
    }

    private Lock copy(Lock lock){
        if(lock instanceof ZookeeperLock){
            return new ZookeeperLock(zookeeperConnection, ((ZookeeperLock) lock).getLockPath());
        } else if(lock instanceof NonReentrantLock){
            return lock;
        }
        throw new RuntimeException("Invalid lock [" + lock + "]");
    }

    // this is allowed in a Reentrant lock
    @Theory
    public void whenLockAcquired_ItCannotBeAcquiredAgain(Locks locks){
        Lock lock = getLock(locks);

        lock.lock();

        assertThat(lock.tryLock(), is(false));

        lock.unlock();
    }

    @Theory
    public void whenLockReleased_ItCanBeAcquiredAgain(Locks locks){
        Lock lock = getLock(locks);

        lock.lock();
        lock.unlock();

        assertThat(lock.tryLock(), is(true));

        lock.unlock();
    }

    @Theory
    public void whenMultipleOfSameLock_OnlyOneAtATimeCanBeAcquired(Locks locks){
        Lock lock1 = getLock(locks);
        Lock lock2 = copy(lock1);

        lock1.lock();

        assertThat(lock2.tryLock(), is(false));

        lock1.unlock();

        assertThat(lock2.tryLock(), is(true));
        assertThat(lock1.tryLock(), is(false));

        lock2.unlock();
    }

    @Theory
    public void whenMultipleLocks_TryLockSucceedsWhenFirstLockReleased(Locks locks) throws InterruptedException {
        Lock lock1 = getLock(locks);
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
        Lock lock1 = getLock(locks);
        Lock lock2 = getLock(locks);

        assertThat(lock1.tryLock(), is(true));
        assertThat(lock2.tryLock(), is(true));

        lock1.unlock();
        lock2.unlock();
    }

    @Test
    public void whenNewConditionCalled_UnsupportedOperationThrown(){
        exception.expect(UnsupportedOperationException.class);

        Lock lock = new ZookeeperLock(zookeeperConnection, LOCK_PATH);
        lock.newCondition();
    }

}
