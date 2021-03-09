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

package grakn.core.concurrent.executor;

import grakn.common.collection.Either;
import grakn.common.concurrent.NamedThreadFactory;
import grakn.core.common.exception.GraknException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.StampedLock;

import static grakn.core.common.exception.ErrorMessage.Internal.UNEXPECTED_INTERRUPTION;
import static grakn.core.common.exception.ErrorMessage.Server.SERVER_SHUTDOWN;

public abstract class EventLoopExecutor<E> implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(EventLoopExecutor.class);
    private static final Shutdown SHUTDOWN_SIGNAL = new Shutdown();

    private final ArrayList<EventLoop> executors;
    private final AtomicInteger executorIndex;
    private final ReadWriteLock accessLock;
    private volatile boolean isOpen;

    protected EventLoopExecutor(int executors, int queuePerExecutor, NamedThreadFactory threadFactory) {
        this.executors = new ArrayList<>(executors);
        this.executorIndex = new AtomicInteger(0);
        this.accessLock = new StampedLock().asReadWriteLock();
        this.isOpen = true;
        for (int i = 0; i < executors; i++) this.executors.add(new EventLoop(queuePerExecutor, threadFactory));
    }

    private EventLoop next() {
        return executors.get(executorIndex.getAndUpdate(i -> {
            i++; if (i % executors.size() == 0) i = 0; return i;
        }));
    }

    public abstract void onEvent(E event);

    public abstract void onException(E event, Throwable exception);

    public void submit(E event) {
        try {
            accessLock.readLock().lock();
            if (!isOpen) throw GraknException.of(SERVER_SHUTDOWN);
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

        private EventLoop(int queueSize, NamedThreadFactory threadFactory) {
            this.queue = new LinkedBlockingQueue<>(queueSize);
            threadFactory.newThread(this::run).start();
        }

        private void submit(E event) {
            try {
                queue.put(Either.first(new Event<>(event)));
            } catch (InterruptedException e) {
                throw GraknException.of(UNEXPECTED_INTERRUPTION);
            }
        }

        public void shutdown() {
            try {
                queue.put(Either.second(SHUTDOWN_SIGNAL));
            } catch (InterruptedException e) {
                throw GraknException.of(UNEXPECTED_INTERRUPTION);
            }
        }

        private void run() {
            while (true) {
                Either<Event<E>, Shutdown> event;
                try {
                    event = queue.take();
                } catch (InterruptedException e) {
                    throw GraknException.of(UNEXPECTED_INTERRUPTION);
                }
                if (event.isFirst()) {
                    try {
                        onEvent(event.first().value());
                    } catch (Throwable e) {
                        onException(event.first().value(), e);
                    }
                } else break;
            }
        }
    }
}
