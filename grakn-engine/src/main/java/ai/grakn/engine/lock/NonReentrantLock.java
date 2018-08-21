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

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * A non-reentrant lock
 *
 * @author alexandraorth
 */
public class NonReentrantLock implements Lock {

    private final Semaphore semaphore;

    public NonReentrantLock(){
        semaphore = new Semaphore(1);
    }

    @Override
    public void lock() {
        semaphore.acquireUninterruptibly(1);
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
        semaphore.acquire(1);
    }

    @Override
    public boolean tryLock() {
        return semaphore.tryAcquire(1);
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        return semaphore.tryAcquire(1, time, unit);
    }

    @Override
    public void unlock() {
        semaphore.release(1);
    }

    @Override
    public Condition newCondition() {
        throw new UnsupportedOperationException();
    }
}
