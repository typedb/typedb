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

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.StampedLock;

import static grakn.core.common.exception.ErrorMessage.Internal.UNEXPECTED_INTERRUPTION;
import static grakn.core.common.exception.ErrorMessage.Server.SERVER_SHUTDOWN;

public class ParallelThreadPoolExecutor implements Executor, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ParallelThreadPoolExecutor.class);
    private static final Shutdown SHUTDOWN_SIGNAL = new Shutdown();

    private final ArrayList<RunnableExecutor> executors;
    private final AtomicInteger executorIndex;
    private final ReadWriteLock accessLock;
    private volatile boolean isOpen;

    public ParallelThreadPoolExecutor(int executors, NamedThreadFactory threadFactory) {
        this.executors = new ArrayList<>(executors);
        this.executorIndex = new AtomicInteger(0);
        this.accessLock = new StampedLock().asReadWriteLock();
        this.isOpen = true;
        for (int i = 0; i < executors; i++) this.executors.add(new RunnableExecutor(threadFactory));
    }

    private RunnableExecutor next() {
        return executors.get(executorIndex.getAndUpdate(i -> {
            i++; if (i % executors.size() == 0) i = 0; return i;
        }));
    }

    @Override
    public void execute(@Nonnull Runnable runnable) {
        try {
            accessLock.readLock().lock();
            if (!isOpen) throw GraknException.of(SERVER_SHUTDOWN);
            next().execute(runnable);
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
                executors.forEach(RunnableExecutor::clear);
                executors.forEach(RunnableExecutor::shutdown);
            }
        } finally {
            accessLock.writeLock().unlock();
        }
    }

    private static class Shutdown {}

    private static class RunnableExecutor implements Executor {

        private final BlockingQueue<Either<Runnable, Shutdown>> queue;

        private RunnableExecutor(NamedThreadFactory threadFactory) {
            this.queue = new LinkedBlockingQueue<>();
            threadFactory.newThread(this::run).start();
        }

        @Override
        public void execute(@Nonnull Runnable runnable) {
            try {
                queue.put(Either.first(runnable));
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
                Either<Runnable, Shutdown> runnable;
                try {
                    runnable = queue.take();
                } catch (InterruptedException e) {
                    throw GraknException.of(UNEXPECTED_INTERRUPTION);
                }
                if (runnable.isFirst()) {
                    runnable.first().run();
                } else break;
            }
        }

        public void clear() {
            queue.clear();
        }
    }
}
