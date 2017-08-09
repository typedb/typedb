/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 *
 */
package ai.grakn.engine.tasks.manager.redisqueue;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import java.io.IOException;
import java.math.BigInteger;
import java.security.SecureRandom;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import net.greghaines.jesque.Config;
import net.greghaines.jesque.Job;
import net.greghaines.jesque.json.ObjectMapperFactory;
import net.greghaines.jesque.utils.PoolUtils;
import net.greghaines.jesque.utils.PoolUtils.PoolWork;
import static net.greghaines.jesque.utils.ResqueConstants.DATE_FORMAT;
import static net.greghaines.jesque.utils.ResqueConstants.FAILED;
import static net.greghaines.jesque.utils.ResqueConstants.INFLIGHT;
import static net.greghaines.jesque.utils.ResqueConstants.PROCESSED;
import static net.greghaines.jesque.utils.ResqueConstants.QUEUE;
import static net.greghaines.jesque.utils.ResqueConstants.STARTED;
import static net.greghaines.jesque.utils.ResqueConstants.STAT;
import static net.greghaines.jesque.utils.ResqueConstants.WORKER;
import static net.greghaines.jesque.utils.ResqueConstants.WORKERS;
import static net.greghaines.jesque.worker.JobExecutor.State.NEW;
import static net.greghaines.jesque.worker.JobExecutor.State.RUNNING;
import static net.greghaines.jesque.worker.JobExecutor.State.SHUTDOWN;
import static net.greghaines.jesque.worker.JobExecutor.State.SHUTDOWN_IMMEDIATE;
import net.greghaines.jesque.worker.JobFactory;
import static net.greghaines.jesque.worker.WorkerEvent.WORKER_ERROR;
import static net.greghaines.jesque.worker.WorkerEvent.WORKER_POLL;
import static net.greghaines.jesque.worker.WorkerEvent.WORKER_START;
import static net.greghaines.jesque.worker.WorkerEvent.WORKER_STOP;
import net.greghaines.jesque.worker.WorkerPoolImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.util.Pool;


/**
 * Queue implemented in redis
 *
 * @author Domenico Corapi
 */
public class BlockingWorker extends WorkerPoolImpl {
    private static final Logger LOG = LoggerFactory.getLogger(BlockingWorker.class);
    private static SecureRandom random = new SecureRandom();
    private final String name;
    private final Timer popTimer;
    private final Timer processTimer;
    private final Timer pollTimer;
    private final Counter processingCounter;
    private final ExecutorService executor;

    public BlockingWorker(Config config, String queue,
            JobFactory jobFactory, Pool<Jedis> jedis, ExecutorService executor, MetricRegistry metricRegistry) {
        super(config, Collections.singletonList(queue), jobFactory, jedis);
        this.executor = executor;

        this.name = "worker_" + new BigInteger(130, random).toString(32);
        this.popTimer = metricRegistry.timer(MetricRegistry.name(BlockingWorker.class, "pop"));
        this.pollTimer = metricRegistry.timer(MetricRegistry.name(BlockingWorker.class, "poll"));
        this.processTimer = metricRegistry.timer(MetricRegistry.name(BlockingWorker.class, "process"));
        this.processingCounter = metricRegistry.counter(MetricRegistry.name(BlockingWorker.class, "processing"));
    }

    @Override
    public void togglePause(boolean paused) {
        throw new RuntimeException("Pausing not implemented");
    }

    @Override
    public void run() {
        if (this.state.compareAndSet(NEW, RUNNING)) {
            try {
                initWorker();
                poll();
            } catch (Exception e) {
                LOG.error("Uncaught exception in worker run loop", e);
                this.listenerDelegate.fireEvent(WORKER_ERROR, this, null, null, null, null, e);
            } finally {
                this.listenerDelegate.fireEvent(WORKER_STOP, this, null, null, null, null, null);
                clean();
            }
        } else if (RUNNING.equals(this.state.get())) {
            throw new IllegalStateException("This WorkerImpl is already running");
        } else {
            throw new IllegalStateException("This WorkerImpl is shutdown");
        }
    }

    private void clean() {
        PoolUtils.doWorkInPoolNicely(this.jedisPool, new PoolWork<Jedis, Void>() {
            /**
             * {@inheritDoc}
             */
            @Override
            public Void doWork(final Jedis jedis) {
                jedis.srem(key(WORKERS), name);
                jedis.del(key(WORKER, name), key(WORKER, name, STARTED), key(STAT, FAILED, name),
                        key(STAT, PROCESSED, name));
                return null;
            }
        });
    }

    @Override
    protected void poll() {
        // This supports only one queue
        String curQueue = queueNames.pop();
        while (RUNNING.equals(this.state.get())) {
            try (Context ignoredPoll = pollTimer.time()) {
                this.listenerDelegate
                        .fireEvent(WORKER_POLL, this, curQueue, null, null, null, null);
                final String payload = pop(curQueue);
                if (payload != null) {
                    Job job = ObjectMapperFactory.get().readValue(payload, Job.class);
                    executor.execute(() -> {
                        try (Context ignoredProcess = processTimer.time()) {
                            processingCounter.inc();
                            process(job, curQueue);
                            processingCounter.dec();
                        }
                    });
                }
            } catch (JsonParseException | JsonMappingException e) {
                // If the job JSON is not deserializable, we never want to submit it again
                final String fCurQueue = curQueue;
                PoolUtils.doWorkInPoolNicely(this.jedisPool, new PoolWork<Jedis, Void>() {
                    @Override
                    public Void doWork(final Jedis jedis) {
                        removeInFlight(jedis, fCurQueue);
                        return null;
                    }
                });
                recoverFromException(curQueue, e);
            } catch (Exception e) {
                recoverFromException(curQueue, e);
            }
        }
    }

    @Override
    protected String pop(final String curQueue) {
        try (Context ignored = popTimer.time()){
            final String key = key(QUEUE, curQueue);
            return PoolUtils.doWorkInPoolNicely(this.jedisPool, new PoolWork<Jedis, String>() {
                /**
                 * {@inheritDoc}
                 */
                @Override
                public String doWork(final Jedis jedis) {
                    return jedis.brpoplpush(key, key(INFLIGHT, name, curQueue), 0);
                }
            });
        }
    }

    private void initWorker() throws IOException {
        try (Jedis poolResource = jedisPool.getResource()) {
            poolResource.sadd(key(WORKERS), name);
            poolResource.set(key(WORKER, name, STARTED), new SimpleDateFormat(DATE_FORMAT).format(new Date()));
            listenerDelegate.fireEvent(WORKER_START, this, null, null, null, null, null);
        }
    }

    @Override
    protected String lpoplpush(final Jedis jedis, final String from, final String to) {
        return jedis.brpoplpush(from, to, 0);
    }

    private void removeInFlight(final Jedis jedis, final String curQueue) {
        if (SHUTDOWN_IMMEDIATE.equals(this.state.get())) {
            lpoplpush(jedis, key(INFLIGHT, this.name, curQueue), key(QUEUE, curQueue));
        } else {
            jedis.lpop(key(INFLIGHT, this.name, curQueue));
        }
    }

    @Override
    public void end(final boolean now) {
        this.state.set(SHUTDOWN);
    }
}
