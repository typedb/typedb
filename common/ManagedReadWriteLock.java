/*
 * Copyright (C) 2020 Grakn Labs
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

package hypergraph.common;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ManagedReadWriteLock {
    private final ReentrantReadWriteLock reentrantLock = new ReentrantReadWriteLock();
    private ThreadLocal<ManagedReadBlocker> readBlocker = ThreadLocal.withInitial(ManagedReadBlocker::new);
    private ThreadLocal<ManagedWriteBlocker> writeBlocker = ThreadLocal.withInitial(ManagedWriteBlocker::new);

    public void lockRead() throws InterruptedException {
        ForkJoinPool.managedBlock(readBlocker.get());
    }

    public void lockWrite() throws InterruptedException {
        ForkJoinPool.managedBlock(writeBlocker.get());
    }

    public void unlockRead() {
        readBlocker.get().unlock();
    }

    public void unlockWrite() {
        writeBlocker.get().unlock();
    }

    class ManagedReadBlocker implements ForkJoinPool.ManagedBlocker {
        boolean hasLock = false;

        public boolean block() {
            if (!hasLock) reentrantLock.readLock().lock();
            return true;
        }

        public boolean isReleasable() {
            return hasLock || (hasLock = reentrantLock.readLock().tryLock());
        }

        void unlock() {
            reentrantLock.readLock().unlock();
            hasLock = false;
        }
    }

    class ManagedWriteBlocker implements ForkJoinPool.ManagedBlocker {
        boolean hasLock = false;

        public boolean block() {
            if (!hasLock) reentrantLock.writeLock().lock();
            return true;
        }

        public boolean isReleasable() {
            return hasLock || (hasLock = reentrantLock.writeLock().tryLock());
        }

        void unlock() {
            reentrantLock.writeLock().unlock();
            hasLock = false;
        }
    }
}
