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

package grakn.core.concurrent.eventloop;

import grakn.common.collection.Either;
import grakn.common.concurrent.NamedThreadFactory;
import grakn.core.common.exception.GraknException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.StampedLock;

import static grakn.core.common.exception.ErrorMessage.Internal.UNEXPECTED_INTERRUPTION;
import static grakn.core.common.exception.ErrorMessage.Server.SERVER_SHUTDOWN;

public abstract class EventLoopExecutor<E> implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(EventLoopExecutor.class);
    private static final Shutdown SHUTDOWN_SIGNAL = new Shutdown();

    private final ArrayList<EventLoop> executors;
    private final BlockingQueue<Either<Event<E>, Shutdown>> queue;
    private final ReadWriteLock accessLock;
    private volatile boolean isOpen;

    protected EventLoopExecutor(int parallelisation, int queueSize, NamedThreadFactory threadFactory) {
        this.executors = new ArrayList<>(parallelisation);
        this.queue = new LinkedBlockingQueue<>(queueSize);
        this.accessLock = new StampedLock().asReadWriteLock();
        this.isOpen = true;
        for (int i = 0; i < parallelisation; i++) executors.add(new EventLoop(threadFactory));
    }

    public abstract void onEvent(E event);

    public abstract void onException(E event, Throwable exception);

    public void submit(E event) {
        try {
            accessLock.readLock().lock();
            if (!isOpen) throw GraknException.of(SERVER_SHUTDOWN);
            queue.put(Either.first(new Event<>(event)));
        } catch (InterruptedException e) {
            throw GraknException.of(UNEXPECTED_INTERRUPTION);
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
                queue.clear();
                for (int i = 0; i < executors.size(); i++) {
                    queue.put(Either.second(SHUTDOWN_SIGNAL));
                }
            }
        } catch (InterruptedException e) {
            throw GraknException.of(UNEXPECTED_INTERRUPTION);
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

        private final Thread thread;

        private EventLoop(NamedThreadFactory threadFactory) {
            this.thread = threadFactory.newThread(this::run);
            thread.start();
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
