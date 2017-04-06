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
import ai.grakn.engine.lock.ZookeeperLock;
import ai.grakn.engine.tasks.manager.ZookeeperConnection;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
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

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @After
    public void clearLock(){
        LockProvider.clearLock();
    }

    @Test
    public void whenGivenReentrantLock_ReturnsReentrantLock(){
        Lock lock = new ReentrantLock();

        LockProvider.init(lock);

        assertThat(LockProvider.getLock(), equalTo(lock));
    }

    @Test
    public void whenGivenZKReentrantLock_ReturnsZKReentrantLock(){
        ZookeeperConnection zookeeperConnection = mock(ZookeeperConnection.class);
        when(zookeeperConnection.connection()).thenReturn(mock(CuratorFramework.class));

        Lock lock = new ZookeeperLock(zookeeperConnection, "/lock");

        LockProvider.init(lock);

        assertThat(LockProvider.getLock(), equalTo(lock));
    }

    @Test
    public void whenGivenLockAfterInitialization_ThrowsRunttimeException(){
        Lock lock1 = new ReentrantLock();
        Lock lock2 = new ReentrantLock();

        exception.expect(RuntimeException.class);
        exception.expectMessage("Lock class has already been initialised with another lock.");

        LockProvider.init(lock1);
        LockProvider.init(lock2);
    }
}
