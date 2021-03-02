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
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TransferQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_OPERATION;
import static grakn.core.common.exception.ErrorMessage.Internal.UNEXPECTED_INTERRUPTION;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@ThreadSafe
public class EventLoop {

    private static final Logger LOG = LoggerFactory.getLogger(EventLoop.class);
    private static final Consumer<Throwable> DEFAULT_ERROR_HANDLER = e -> LOG.error("An unexpected error has occurred.", e);

    private final TransferQueue<Job> submittedJobs;
    private final ScheduledJobQueue scheduledJobs;
    private final Supplier<Long> clock;
    private final Thread thread;
    private final AtomicBoolean isStopped;
    private State state;

    private enum State {READY, RUNNING, STOPPED}

    public EventLoop(ThreadFactory threadFactory, Supplier<Long> clock) {
        this.thread = threadFactory.newThread(this::loop);
        this.clock = clock;
        submittedJobs = new LinkedTransferQueue<>();
        scheduledJobs = new ScheduledJobQueue();
        isStopped = new AtomicBoolean(false);
        state = State.READY;
        thread.start();
    }

    public void submit(Runnable runnable, Consumer<Throwable> errorHandler) {
        assert state != State.STOPPED : "unexpected state: " + state;
        submittedJobs.offer(new Job(runnable, errorHandler));
    }

    public FutureJob schedule(Runnable runnable, long scheduleMillis, Consumer<Throwable> errorHandler) {
        assert state != State.STOPPED : "unexpected state: " + state;
        FutureJob job = new FutureJob(runnable, scheduleMillis, errorHandler);
        job.initialise();
        return job;
    }

    public void await() throws InterruptedException {
        thread.join();
    }

    public void stop() throws InterruptedException {
        if (isStopped.compareAndSet(false, true)) {
            submit(() -> state = State.STOPPED, DEFAULT_ERROR_HANDLER);
            await();
        }
    }

    private void loop() {
        state = State.RUNNING;
        while (state == State.RUNNING) {
            Job job = scheduledJobs.poll();
            if (job != null) {
                job.run();
            } else {
                try {
                    job = submittedJobs.poll(scheduledJobs.timeToNext(), MILLISECONDS);
                    if (job != null) job.run();
                } catch (InterruptedException e) {
                    throw GraknException.of(UNEXPECTED_INTERRUPTION);
                }
            }
        }
    }

    @NotThreadSafe
    private static class Job implements Comparable<Job> {

        private final Runnable runnable;
        private final Consumer<Throwable> errorHandler;
        private final Long scheduleMillis;
        private boolean isCancelled;
        private boolean isRan;

        private Job(Runnable runnable, Consumer<Throwable> errorHandler) {
            this(runnable, null, errorHandler);
        }

        private Job(Runnable runnable, @Nullable Long scheduleMillis, Consumer<Throwable> errorHandler) {
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
        public int compareTo(Job other) {
            return Long.compare(scheduleMillis().orElse(0L), other.scheduleMillis().orElse(0L));
        }
    }

    @ThreadSafe
    public class FutureJob {

        private final Job job;

        private FutureJob(Runnable runnable, long scheduleMillis, Consumer<Throwable> errorHandler) {
            this.job = new Job(runnable, scheduleMillis, errorHandler);
        }

        private void initialise() {
            EventLoop.this.submit(() -> scheduledJobs.offer(job), job.errorHandler);
        }

        public void cancel() {
            EventLoop.this.submit(job::cancel, job.errorHandler);
        }
    }

    @ThreadSafe
    private class ScheduledJobQueue {

        private final PriorityQueue<Job> queue;

        private ScheduledJobQueue() {
            queue = new PriorityQueue<>();
        }

        private long timeToNext() {
            Job job = peek();
            if (job == null) return Long.MAX_VALUE;
            else return job.scheduleMillis().get() - clock.get();
        }

        private Job peek() {
            Job job;
            while ((job = queue.peek()) != null && job.isCancelled()) queue.poll();
            return job;
        }

        private Job poll() {
            Job job = peek();
            if (job == null) return null;
            else if (job.scheduleMillis().get() > clock.get()) return null;
            else queue.poll();
            return job;
        }

        public void offer(Job job) {
            assert job.scheduleMillis().isPresent();
            queue.add(job);
        }
    }
}
