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

import ai.grakn.engine.lock.GenericLockProvider;
import ai.grakn.engine.lock.NonReentrantLock;
import java.util.concurrent.locks.Lock;
import java.util.function.Function;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.SystemOutRule;
import org.junit.rules.ExpectedException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class GenericLockProviderTest {

    private final String LOCK_NAME = "lock";

    @Rule
    public final SystemOutRule systemOut = new SystemOutRule().enableLog();

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void whenGivenLock_ReturnsLockWithSameClass(){
        assertThat(new GenericLockProvider().getLock(LOCK_NAME).getClass(), equalTo(NonReentrantLock.class));
    }

    @Test
    public void whenGettingLockWithDifferentKeys_LockProviderFunctionCalledTwice(){
        Function<String, Lock> lockProviderFunction = mock(Function.class);
        GenericLockProvider l = new GenericLockProvider(
                (string, existingLock) -> lockProviderFunction.apply(string));
        when(lockProviderFunction.apply(any())).thenReturn(new NonReentrantLock());

        l.getLock(LOCK_NAME + "1");
        l.getLock(LOCK_NAME + "2");

        verify(lockProviderFunction, times(2)).apply(any());
    }

    @Test
    public void whenGettingLockWithSameKey_LockProviderFunctionCalledTwice(){
        Function<String, Lock> lockProviderFunction = mock(Function.class);
        when(lockProviderFunction.apply(any())).thenReturn(new NonReentrantLock());

        GenericLockProvider l = new GenericLockProvider(
                (string, existingLock) -> lockProviderFunction.apply(string));

        l.getLock(LOCK_NAME + "1");
        l.getLock(LOCK_NAME + "1");

        verify(lockProviderFunction, times(2)).apply(any());
    }

    @Test
    public void whenGivenFunctionToReturnDifferentLocks_LocksAreDifferent(){
        GenericLockProvider l = new GenericLockProvider((string, existingLock) -> new NonReentrantLock());

        Lock lock1 = l.getLock(LOCK_NAME);
        Lock lock2 = l.getLock(LOCK_NAME);

        assertNotEquals(lock1, lock2);
    }

    @Test
    public void whenGivenFunctionToReturnSameLocks_LocksAreTheSame(){
        GenericLockProvider l = new GenericLockProvider((string, existingLock) -> {
            if(existingLock != null){
                return existingLock;
            }
            return new NonReentrantLock();
        });

        Lock lock1 = l.getLock(LOCK_NAME);
        Lock lock2 = l.getLock(LOCK_NAME);

        assertEquals(lock1, lock2);
    }
}
