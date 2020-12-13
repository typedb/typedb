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
import java.util.concurrent.TimeUnit;

/**
 * TODO: implement our own LinkedBlockingQueue that has the features of:
 * 1) ResizingBlockingQueue
 * 2) ManagedBlocker
 * 3) Cancellable via {@code Condition} signalling
 */
public class ManagedBlockingQueue<E> {

    private static final int BLOCKING_TIMEOUT_SECONDS = 8;
    private final LinkedBlockingQueue<E> queue;
    private final ThreadLocal<QueueTaker> queueTaker;
    private final ThreadLocal<QueuePutter> queuePutter;
    private volatile boolean cancelled;

    public ManagedBlockingQueue() {
        this(new LinkedBlockingQueue<>());
    }

    public ManagedBlockingQueue(int capacity) {
        this(new LinkedBlockingQueue<>(capacity));
    }

    private ManagedBlockingQueue(LinkedBlockingQueue<E> queue) {
        this.queue = queue;
        queueTaker = ThreadLocal.withInitial(QueueTaker::new);
        queuePutter = ThreadLocal.withInitial(QueuePutter::new);
        cancelled = false;
    }

    public E poll() {
        return queue.poll();
    }

    public E take() throws InterruptedException {
        ForkJoinPool.managedBlock(queueTaker.get());
        return queueTaker.get().getItem();
    }

    public void put(E element) throws InterruptedException {
        queuePutter.get().setItem(element);
        ForkJoinPool.managedBlock(queuePutter.get());
    }

    public void drainTo(ManagedBlockingQueue<E> otherQueue) {
        queue.drainTo(otherQueue.queue);
    }

    public int remainingCapacity() {
        return queue.remainingCapacity();
    }

    public void cancel() {
        cancelled = true;
    }

    public boolean isCancelled() {
        return cancelled;
    }

    public void clear() {
        queue.clear();
    }

    public int size() {
        return queue.size();
    }

    private class QueueTaker implements ForkJoinPool.ManagedBlocker {

        private E item = null;

        @Override
        public boolean block() throws InterruptedException {
            if (item == null) item = queue.poll(BLOCKING_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            return item != null || cancelled;
        }

        @Override
        public boolean isReleasable() {
            return item != null || (item = queue.poll()) != null;
        }

        public E getItem() {
            E newItem = item;
            item = null;
            return newItem;
        }
    }

    private class QueuePutter implements ForkJoinPool.ManagedBlocker {

        private E item;

        @Override
        public boolean block() throws InterruptedException {
            boolean noLock = true;
            if (item != null && (noLock = queue.offer(item, BLOCKING_TIMEOUT_SECONDS, TimeUnit.SECONDS))) {
                item = null;
            }
            return noLock || cancelled;
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
