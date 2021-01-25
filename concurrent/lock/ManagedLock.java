/*
 * Copyright (C) 2021 Grakn Labs
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
 *
 */

package grakn.core.concurrent.lock;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Semaphore;

/**
 * A {@code Semaphore}-based locking mechanism that wrapped in a {@code ManagedBlocker}.
 * It is **NOT** re-entrant by design.
 *
 * When a thread is blocked while waiting to acquire a lock, this class will
 * possibly arrange for a spare thread to be activated if necessary, to ensure
 * sufficient parallelism while the current thread is blocked.
 */
public class ManagedLock {

    private final Semaphore semaphore;
    private final ThreadLocal<ManagedLock.Locker> locker;

    public ManagedLock() {
        semaphore = new Semaphore(1);
        locker = ThreadLocal.withInitial(ManagedLock.Locker::new);
    }

    public void lock() throws InterruptedException {
        ForkJoinPool.managedBlock(locker.get());
    }

    public void unlock() {
        locker.get().unlock();
    }

    private class Locker implements ForkJoinPool.ManagedBlocker {

        @Override
        public boolean block() throws InterruptedException {
            semaphore.acquire();
            return true;
        }

        @Override
        public boolean isReleasable() {
            return false; // blocking is always necessary as this lock is not re-entrant
        }

        void unlock() {
            semaphore.release();
        }
    }
}
