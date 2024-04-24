/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.concurrent.executor;

import com.vaticle.typedb.common.collection.Either;
import com.vaticle.typedb.common.concurrent.NamedThreadFactory;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.StampedLock;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.UNEXPECTED_INTERRUPTION;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.SERVER_SHUTDOWN;

public abstract class EventLoopExecutor<E> implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(EventLoopExecutor.class);
    private static final Shutdown SHUTDOWN_SIGNAL = new Shutdown();

    private final ArrayList<EventLoop> executors;
    private final AtomicInteger executorIndex;
    private final ReadWriteLock accessLock;
    private volatile boolean isOpen;

    protected EventLoopExecutor(int executors, NamedThreadFactory threadFactory) {
        this.executors = new ArrayList<>(executors);
        this.executorIndex = new AtomicInteger(0);
        this.accessLock = new StampedLock().asReadWriteLock();
        this.isOpen = true;
        for (int i = 0; i < executors; i++) this.executors.add(new EventLoop(threadFactory));
    }

    private EventLoop next() {
        return executors.get(executorIndex.getAndUpdate(i -> {
            i++;
            if (i % executors.size() == 0) i = 0;
            return i;
        }));
    }

    public abstract void onEvent(E event);

    public abstract void onException(E event, Throwable exception);

    public void submit(E event) {
        try {
            accessLock.readLock().lock();
            if (!isOpen) throw TypeDBException.of(SERVER_SHUTDOWN);
            next().submit(event);
        } finally {
            accessLock.readLock().unlock();
        }
    }

    @Override
    public void close() {
        try {
            accessLock.writeLock().lock();
            if (isOpen) {
                isOpen = false;
                executors.forEach(executor -> executor.queue.clear());
                executors.forEach(EventLoop::shutdown);
            }
        } finally {
            accessLock.writeLock().unlock();
        }
    }

    private static class Shutdown {}

    private static class Event<T> {

        @Nullable
        private final T value;

        private Event(@Nullable T value) {
            this.value = value;
        }

        @Nullable
        private T value() {
            return value;
        }
    }

    private class EventLoop {

        private final BlockingQueue<Either<Event<E>, Shutdown>> queue;

        private EventLoop(NamedThreadFactory threadFactory) {
            this.queue = new LinkedBlockingQueue<>();
            threadFactory.newThread(this::run).start();
        }

        private void submit(E event) {
            try {
                queue.put(Either.first(new Event<>(event)));
            } catch (InterruptedException e) {
                throw TypeDBException.of(UNEXPECTED_INTERRUPTION);
            }
        }

        public void shutdown() {
            try {
                queue.put(Either.second(SHUTDOWN_SIGNAL));
            } catch (InterruptedException e) {
                throw TypeDBException.of(UNEXPECTED_INTERRUPTION);
            }
        }

        private void run() {
            while (true) {
                try {
                    Either<Event<E>, Shutdown> event = queue.take();
                    if (event.isFirst()) {
                        try {
                            onEvent(event.first().value());
                        } catch (Throwable e) {
                            onException(event.first().value(), e);
                        }
                    } else break;
                } catch (InterruptedException e) {
                    throw TypeDBException.of(UNEXPECTED_INTERRUPTION);
                }
            }
        }
    }
}
