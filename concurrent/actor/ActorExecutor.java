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
 */

package grakn.core.concurrent.actor;

import grakn.core.common.exception.GraknException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_OPERATION;
import static grakn.core.common.exception.ErrorMessage.Internal.UNEXPECTED_INTERRUPTION;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@ThreadSafe
public class ActorExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(ActorExecutor.class);

    private final BlockingQueue<Task> submittedTasks;
    private final ScheduledTaskQueue scheduledTasks;
    private final AtomicBoolean isStopped;
    private final Supplier<Long> clock;
    private final Thread thread;
    private State state;

    private enum State {READY, RUNNING, STOPPED}

    public ActorExecutor(ThreadFactory threadFactory, Supplier<Long> clock) {
        this.thread = threadFactory.newThread(this::run);
        this.clock = clock;
        // TODO: Benchmark and verify that LinkedTransferQueue is actually most performant
        submittedTasks = new LinkedTransferQueue<>();
        scheduledTasks = new ScheduledTaskQueue();
        isStopped = new AtomicBoolean(false);
        state = State.READY;
        thread.start();
    }

    private void run() {
        state = State.RUNNING;
        while (state == State.RUNNING) {
            Task task = scheduledTasks.poll();
            if (task != null) {
                task.run();
            } else {
                try {
                    task = submittedTasks.poll(scheduledTasks.timeToNext(), MILLISECONDS);
                    if (task != null) task.run();
                } catch (InterruptedException e) {
                    throw GraknException.of(UNEXPECTED_INTERRUPTION);
                }
            }
        }
    }

    public void submit(Runnable runnable, Consumer<Throwable> errorHandler) {
        assert state != State.STOPPED : "unexpected state: " + state;
        submittedTasks.offer(new Task(runnable, errorHandler));
    }

    public FutureTask schedule(Runnable runnable, long scheduleMillis, Consumer<Throwable> errorHandler) {
        assert state != State.STOPPED : "unexpected state: " + state;
        FutureTask task = new FutureTask(runnable, scheduleMillis, errorHandler);
        task.initialise();
        return task;
    }

    public void await() throws InterruptedException {
        thread.join();
    }

    public void stop() throws InterruptedException {
        if (isStopped.compareAndSet(false, true)) {
            submit(() -> state = State.STOPPED, e -> LOG.error("Unexpected error at stopping an ActorExecutor", e));
            await();
        }
    }

    @NotThreadSafe
    private static class Task implements Comparable<Task> {

        private final Runnable runnable;
        private final Consumer<Throwable> errorHandler;
        private final Long scheduleMillis;
        private boolean isCancelled;
        private boolean isRan;

        private Task(Runnable runnable, Consumer<Throwable> errorHandler) {
            this(runnable, null, errorHandler);
        }

        private Task(Runnable runnable, @Nullable Long scheduleMillis, Consumer<Throwable> errorHandler) {
            this.runnable = runnable;
            this.scheduleMillis = scheduleMillis;
            this.errorHandler = errorHandler;
            isCancelled = false;
            isRan = false;
        }

        private void run() {
            if (isCancelled) throw GraknException.of(ILLEGAL_OPERATION);
            isRan = true;
            try {
                runnable.run();
            } catch (Throwable e) {
                errorHandler.accept(e);
            }
        }

        private Optional<Long> scheduleMillis() {
            return Optional.ofNullable(scheduleMillis);
        }

        private void cancel() {
            if (isRan) throw GraknException.of(ILLEGAL_OPERATION);
            isCancelled = true;
        }

        private boolean isCancelled() {
            return isCancelled;
        }

        @Override
        public int compareTo(Task other) {
            return Long.compare(scheduleMillis().orElse(0L), other.scheduleMillis().orElse(0L));
        }
    }

    @ThreadSafe
    public class FutureTask {

        private final Task task;

        private FutureTask(Runnable runnable, long scheduleMillis, Consumer<Throwable> errorHandler) {
            this.task = new Task(runnable, scheduleMillis, errorHandler);
        }

        private void initialise() {
            ActorExecutor.this.submit(() -> scheduledTasks.offer(task), task.errorHandler);
        }

        public void cancel() {
            ActorExecutor.this.submit(task::cancel, task.errorHandler);
        }
    }

    @ThreadSafe
    private class ScheduledTaskQueue {

        private final PriorityQueue<Task> queue;

        private ScheduledTaskQueue() {
            queue = new PriorityQueue<>();
        }

        private long timeToNext() {
            Task task = peek();
            if (task == null) return Long.MAX_VALUE;
            else return task.scheduleMillis().get() - clock.get();
        }

        private Task peek() {
            Task task;
            while ((task = queue.peek()) != null && task.isCancelled()) queue.poll();
            return task;
        }

        private Task poll() {
            Task task = peek();
            if (task == null) return null;
            else if (task.scheduleMillis().get() > clock.get()) return null;
            else queue.poll();
            return task;
        }

        public void offer(Task task) {
            assert task.scheduleMillis().isPresent();
            queue.add(task);
        }
    }
}
