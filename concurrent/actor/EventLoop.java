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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.PriorityQueue;
import java.util.Random;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TransferQueue;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class EventLoop {
    private static final Logger LOG = LoggerFactory.getLogger(EventLoop.class);
    private static final Consumer<Throwable> DEFAULT_ERROR_HANDLER = e -> LOG.error("An unexpected error has occurred.", e);

    private enum State {READY, RUNNING, STOPPED}

    private State state;
    private final TransferQueue<Job> jobs = new LinkedTransferQueue<>();
    private final ScheduledJobQueue scheduledJobs = new ScheduledJobQueue();
    private final Supplier<Long> clock;
    private final Random random;
    private final Thread thread;

    public EventLoop(ThreadFactory threadFactory, Supplier<Long> clock, Random random) {
        state = State.READY;
        this.clock = clock;
        this.random = random;
        thread = threadFactory.newThread(this::loop);
        thread.start();
    }

    public void schedule(Runnable job, Consumer<Throwable> errorHandler) {
        assert state != State.STOPPED : "unexpected state: " + state;

        jobs.offer(new Job(job, errorHandler));
    }

    public EventLoop.Cancellable schedule(long deadline, Runnable job, Consumer<Throwable> errorHandler) {
        assert state != State.STOPPED : "unexpected state: " + state;
        return new Cancellable(deadline, job, errorHandler);
    }

    public synchronized void await() throws InterruptedException {
        thread.join();
    }

    public synchronized void stop() throws InterruptedException {
        schedule(() -> state = State.STOPPED, DEFAULT_ERROR_HANDLER);
        await();
    }

    public long time() {
        return clock.get();
    }

    public Random random() {
        return random;
    }

    private void loop() {
        LOG.debug("Started");
        state = State.RUNNING;

        while (state == State.RUNNING) {
            long currentTimeMs = clock.get();
            Job scheduledJob = scheduledJobs.poll(currentTimeMs);
            if (scheduledJob != null) {
                scheduledJob.run();
            } else {
                try {
                    Job job = jobs.poll(scheduledJobs.timeToNext(currentTimeMs), TimeUnit.MILLISECONDS);
                    if (job != null) {
                        job.run();
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        LOG.debug("stopped");
    }

    public class Cancellable {
        private ScheduledJobQueue.Scheduled scheduled;

        public Cancellable(long scheduleMs, Runnable job, Consumer<Throwable> errorHandler) {
            scheduled = null;
            EventLoop.this.schedule(() -> scheduled = scheduledJobs.offer(scheduleMs, new Job(job, errorHandler)), errorHandler);
        }

        public void cancel() {
            EventLoop.this.schedule(() -> scheduled.cancel(), DEFAULT_ERROR_HANDLER);
        }
    }

    private static class Job {
        private final Runnable job;
        private final Consumer<Throwable> errorHandler;

        public Job(Runnable job, Consumer<Throwable> errorHandler) {
            this.job = job;
            this.errorHandler = errorHandler;
        }

        public void run() {
            try {
                job.run();
            } catch (Throwable e) {
                errorHandler.accept(e);
            }
        }
    }

    private static class ScheduledJobQueue {
        private final PriorityQueue<Scheduled> queue = new PriorityQueue<>();
        private long counter = 0L;

        public Scheduled offer(long expireAtMs, Job job) {
            counter++;
            Scheduled scheduled = new Scheduled(counter, expireAtMs, job);
            queue.add(scheduled);
            return scheduled;
        }

        public Job poll(long currentTimeMs) {
            Scheduled timer = peekToNextReady();
            if (timer == null) return null;
            if (timer.expireAtMs > currentTimeMs) return null;
            queue.poll();
            return timer.job;
        }

        public long timeToNext(long currentTimeMs) {
            Scheduled timer = peekToNextReady();
            if (timer == null) return Long.MAX_VALUE;
            return timer.expireAtMs - currentTimeMs;
        }

        private Scheduled peekToNextReady() {
            Scheduled scheduled;
            while ((scheduled = queue.peek()) != null && scheduled.isCancelled()) {
                queue.poll();
            }
            return scheduled;
        }

        private static class Scheduled implements Comparable<Scheduled> {
            private final long version;
            private final long expireAtMs;
            private final Job job;
            private boolean cancelled = false;

            public Scheduled(long version, long expireAtMs, Job job) {
                this.expireAtMs = expireAtMs;
                this.job = job;
                this.version = version;
            }

            @Override
            public int compareTo(Scheduled other) {
                if (expireAtMs < other.expireAtMs) {
                    return -1;
                } else if (expireAtMs > other.expireAtMs) {
                    return 1;
                } else {
                    return Long.compare(version, other.version);
                }
            }

            public void cancel() {
                cancelled = true;
            }

            public boolean isCancelled() {
                return cancelled;
            }
        }
    }
}
