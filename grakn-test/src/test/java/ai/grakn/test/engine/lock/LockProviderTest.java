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
import ai.grakn.util.ErrorMessage;
import java.util.concurrent.locks.Lock;
import java.util.function.Function;
import org.apache.curator.framework.CuratorFramework;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.SystemOutRule;
import org.junit.rules.ExpectedException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class LockProviderTest {

    private final String LOCK_NAME = "lock";

    @Rule
    public final SystemOutRule systemOut = new SystemOutRule().enableLog();

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @After
    public void clearLock(){
        LockProvider.clear();
    }

    @Test
    public void whenGivenLock_ReturnsLockWithSameClass(){
        LockProvider.instantiate((string) -> new NonReentrantLock());

        assertThat(LockProvider.getLock(LOCK_NAME).getClass(), equalTo(NonReentrantLock.class));
    }

    @Test
    public void whenGivenZKReentrantLock_ReturnsLockWithZkReentrantClass(){
        ZookeeperConnection zookeeperConnection = mock(ZookeeperConnection.class);
        when(zookeeperConnection.connection()).thenReturn(mock(CuratorFramework.class));

        LockProvider.instantiate((lockPath) -> new ZookeeperLock(zookeeperConnection, lockPath));

        assertThat(LockProvider.getLock("/" + LOCK_NAME).getClass(), equalTo(ZookeeperLock.class));
    }

    @Test
    public void whenInstantiatedTwice_ThrowsRuntimeException(){
        LockProvider.instantiate((string) -> new NonReentrantLock());

        LockProvider.instantiate((string) -> new NonReentrantLock());

        assertThat(systemOut.getLog(), containsString(ErrorMessage.LOCK_ALREADY_INSTANTIATED.getMessage()));
    }

    @Test
    public void whenInstantiatedTwiceWithClear_ReturnsLock(){
        LockProvider.instantiate((String string) -> new NonReentrantLock());

        LockProvider.clear();

        LockProvider.instantiate((string) -> new NonReentrantLock());

        assertNotNull(LockProvider.getLock(LOCK_NAME));
    }

    @Test
    public void whenGettingLockWithDifferentKeys_LockProviderFunctionCalledTwice(){
        Function<String, Lock> lockProviderFunction = mock(Function.class);
        LockProvider.instantiate((string) -> lockProviderFunction.apply(string));
        when(lockProviderFunction.apply(any())).thenReturn(new NonReentrantLock());

        LockProvider.getLock(LOCK_NAME + "1");
        LockProvider.getLock(LOCK_NAME + "2");

        verify(lockProviderFunction, times(2)).apply(any());
    }

    @Test
    public void whenGettingLockWithSameKey_LockProviderFunctionCalledOnce(){
        Function<String, Lock> lockProviderFunction = mock(Function.class);
        when(lockProviderFunction.apply(any())).thenReturn(new NonReentrantLock());

        LockProvider.instantiate((string) -> lockProviderFunction.apply(string));

        LockProvider.getLock(LOCK_NAME + "1");
        LockProvider.getLock(LOCK_NAME + "1");

        verify(lockProviderFunction, times(1)).apply(any());
    }
}
