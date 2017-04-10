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

import ai.grakn.engine.lock.LockProvider;
import ai.grakn.engine.lock.NonReentrantLock;
import ai.grakn.engine.lock.ZookeeperLock;
import ai.grakn.engine.tasks.manager.ZookeeperConnection;
import java.util.concurrent.locks.Lock;
import org.apache.curator.framework.CuratorFramework;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LockProviderTest {

    private final String LOCK_NAME = "lock";

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @After
    public void clearLock(){
        LockProvider.clear();
    }

    @Test
    public void whenGivenReentrantLock_ReturnsReentrantLock(){
        Lock lock = new NonReentrantLock();

        LockProvider.add(LOCK_NAME, lock);

        assertThat(LockProvider.getLock(LOCK_NAME), equalTo(lock));
    }

    @Test
    public void whenGivenZKReentrantLock_ReturnsZKReentrantLock(){
        ZookeeperConnection zookeeperConnection = mock(ZookeeperConnection.class);
        when(zookeeperConnection.connection()).thenReturn(mock(CuratorFramework.class));

        Lock lock = new ZookeeperLock(zookeeperConnection, "/lock");

        LockProvider.add(LOCK_NAME, lock);

        assertThat(LockProvider.getLock(LOCK_NAME), equalTo(lock));
    }

    @Test
    public void whenGivenLockAfterInitialization_ThrowsRunttimeException(){
        Lock lock1 = new NonReentrantLock();
        Lock lock2 = new ZookeeperLock(mock(ZookeeperConnection.class), "/testlock");

        exception.expect(RuntimeException.class);
        exception.expectMessage("Lock class has already been initialised with another lock.");

        LockProvider.add(LOCK_NAME, lock1);
        LockProvider.add(LOCK_NAME, lock2);
    }
}
