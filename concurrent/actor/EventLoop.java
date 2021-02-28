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

import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TransferQueue;
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
    private final Random random;
    private final Thread thread;
    private State state;

    private enum State {READY, RUNNING, STOPPED}

    public EventLoop(ThreadFactory threadFactory, Supplier<Long> clock, Random random) {
        this.thread = threadFactory.newThread(this::loop);
        this.clock = clock;
        this.random = random; // TODO: this does not belong here, we should move Random to the owner, and delete this field
        submittedJobs = new LinkedTransferQueue<>();
        scheduledJobs = new ScheduledJobQueue();
        state = State.READY;
        thread.start();
    }

    public void submit(Runnable runnable, Consumer<Throwable> errorHandler) {
        assert state != State.STOPPED : "unexpected state: " + state;
        submittedJobs.offer(new Job(runnable, errorHandler));
    }

    public ScheduledJob schedule(Runnable runnable, Consumer<Throwable> errorHandler, long scheduleMillis) {
        assert state != State.STOPPED : "unexpected state: " + state;
        return scheduledJobs.offer(new Job(runnable, errorHandler), scheduleMillis);
    }

    public synchronized void await() throws InterruptedException {
        thread.join();
    }

    public synchronized void stop() throws InterruptedException {
        submit(() -> state = State.STOPPED, DEFAULT_ERROR_HANDLER);
        await();
    }

    public long time() {
        // TODO: Does EventLoop need to provide time? It does not look like this class is the right domain for it.
        return clock.get();
    }

    public Random random() {
        // TODO: this does not belong here, we should move Random to the owner, and delete this method
        return random;
    }

    private void loop() {
        state = State.RUNNING;
        while (state == State.RUNNING) {
            long boundMillis = clock.get();
            ScheduledJob scheduledJob = scheduledJobs.poll(boundMillis);
            if (scheduledJob != null) {
                scheduledJob.run();
            } else {
                try {
                    Job job = submittedJobs.poll(scheduledJobs.timeToNext(boundMillis), MILLISECONDS);
                    if (job != null) job.run();
                } catch (InterruptedException e) {
                    throw GraknException.of(UNEXPECTED_INTERRUPTION);
                }
            }
        }
    }

    @NotThreadSafe
    private static class Job {
        private final Runnable runnable;
        private final Consumer<Throwable> errorHandler;

        private Job(Runnable runnable, Consumer<Throwable> errorHandler) {
            this.runnable = runnable;
            this.errorHandler = errorHandler;
        }

        private void run() {
            try {
                runnable.run();
            } catch (Throwable e) {
                errorHandler.accept(e);
            }
        }
    }

    @NotThreadSafe
    public class ScheduledJob implements Comparable<ScheduledJob> {

        private final Job runnable;
        private final long scheduleMillis;
        private boolean isCancelled;
        private boolean hasRan;

        private ScheduledJob(Job runnable, long scheduleMillis) {
            this.scheduleMillis = scheduleMillis;
            this.runnable = runnable;
            isCancelled = false;
            hasRan = false;
        }

        public long scheduleMillis() {
            return scheduleMillis;
        }

        public void run() {
            if (isCancelled) throw GraknException.of(ILLEGAL_OPERATION);
            hasRan = true;
            runnable.run();
        }

        public void cancel() {
            if (hasRan) throw GraknException.of(ILLEGAL_OPERATION);
            isCancelled = true;
        }

        private boolean isCancelled() {
            return isCancelled;
        }

        @Override
        public int compareTo(ScheduledJob other) {
            return Long.compare(scheduleMillis, other.scheduleMillis);
        }
    }

    @ThreadSafe
    private class ScheduledJobQueue {

        private final PriorityQueue<EventLoop.ScheduledJob> queue;

        private ScheduledJobQueue() {
            queue = new PriorityQueue<>();
        }

        private long timeToNext(long boundMillis) {
            EventLoop.ScheduledJob job = peek();
            if (job == null) return Long.MAX_VALUE;
            else return job.scheduleMillis - boundMillis;
        }

        private EventLoop.ScheduledJob peek() {
            EventLoop.ScheduledJob job;
            while ((job = queue.peek()) != null && job.isCancelled()) queue.poll();
            return job;
        }

        private EventLoop.ScheduledJob poll(long boundMillis) {
            EventLoop.ScheduledJob job = peek();
            if (job == null) return null;
            else if (job.scheduleMillis > boundMillis) return null;
            else queue.poll();
            return job;
        }

        public ScheduledJob offer(Job job, long scheduleMillis) {
            EventLoop.ScheduledJob scheduled = new ScheduledJob(job, scheduleMillis);
            queue.add(scheduled);
            return scheduled;
        }
    }
}
