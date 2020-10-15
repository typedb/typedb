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

package grakn.core.common.concurrent;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.LinkedBlockingQueue;

public class ManagedBlockingQueue<E> {

    private final LinkedBlockingQueue<E> queue;
    private final ThreadLocal<QueueTaker> queueTaker;
    private final ThreadLocal<QueuePutter> queuePutter;

    public ManagedBlockingQueue(final int capacity) {
        queue = new LinkedBlockingQueue<>(capacity);
        queueTaker = ThreadLocal.withInitial(QueueTaker::new);
        queuePutter = ThreadLocal.withInitial(QueuePutter::new);
    }

    public E poll() {
        return queue.poll();
    }

    public E take() throws InterruptedException {
        ForkJoinPool.managedBlock(queueTaker.get());
        return queueTaker.get().getItem();
    }

    public void put(final E element) throws InterruptedException {
        queuePutter.get().setItem(element);
        ForkJoinPool.managedBlock(queuePutter.get());
    }

    public void drainTo(final ManagedBlockingQueue<E> otherQueue) {
        queue.drainTo(otherQueue.queue);
    }

    public int remainingCapacity() {
        return queue.remainingCapacity();
    }

    class QueueTaker implements ForkJoinPool.ManagedBlocker {

        private E item = null;

        @Override
        public boolean block() throws InterruptedException {
            if (item == null) item = queue.take();
            return true;
        }

        @Override
        public boolean isReleasable() {
            return item != null || (item = queue.poll()) != null;
        }

        public E getItem() {
            return item;
        }
    }

    class QueuePutter implements ForkJoinPool.ManagedBlocker {

        private E item;

        @Override
        public boolean block() throws InterruptedException {
            if (item != null) {
                queue.put(item);
                item = null;
            }
            return true;
        }

        @Override
        public boolean isReleasable() {
            return false;
        }

        public void setItem(E item) {
            this.item = item;
        }
    }
}
