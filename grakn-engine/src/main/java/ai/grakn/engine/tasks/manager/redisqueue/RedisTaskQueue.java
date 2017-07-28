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


import ai.grakn.engine.GraknEngineConfig;
import ai.grakn.engine.factory.EngineGraknGraphFactory;
import ai.grakn.engine.lock.LockProvider;
import ai.grakn.engine.util.EngineID;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import static com.codahale.metrics.MetricRegistry.name;
import java.time.Duration;
import java.util.Arrays;
import java.util.Date;
import java.util.Timer;
import net.greghaines.jesque.Config;
import net.greghaines.jesque.ConfigBuilder;
import net.greghaines.jesque.Job;
import net.greghaines.jesque.client.Client;
import net.greghaines.jesque.client.ClientPoolImpl;
import static net.greghaines.jesque.utils.JesqueUtils.entry;
import static net.greghaines.jesque.utils.JesqueUtils.map;
import net.greghaines.jesque.worker.MapBasedJobFactory;
import net.greghaines.jesque.worker.RecoveryStrategy;
import net.greghaines.jesque.worker.Worker;
import net.greghaines.jesque.worker.WorkerEvent;
import net.greghaines.jesque.worker.WorkerPool;
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
class RedisTaskQueue {

    private final static Logger LOG = LoggerFactory.getLogger(RedisTaskQueue.class);

    private static final String QUEUE_NAME = "grakn_engine_queue";
    private static final String SUBSCRIPTION_CLASS_NAME = Task.class.getName();
    static final MapBasedJobFactory JOB_FACTORY = new MapBasedJobFactory(
            map(entry(
                    // Assign elements with this class
                    SUBSCRIPTION_CLASS_NAME,
                    // To be run by this class
                    RedisTaskQueueConsumer.class)));

    private final Client redisClient;
    private final Config config;
    private final Histogram queueSize;
    private final Meter failures;
    private final int processingDelay;
    private final Timer timer;
    private Pool<Jedis> jedisPool;
    private LockProvider lockProvider;
    private final MetricRegistry metricRegistry;

    private final Meter putJobMeter;
    private WorkerPool workerPool;

    RedisTaskQueue(
            Pool<Jedis> jedisPool,
            LockProvider lockProvider,
            MetricRegistry metricRegistry,
            int processingDelay) {
        this.jedisPool = jedisPool;
        this.lockProvider = lockProvider;
        this.metricRegistry = metricRegistry;
        this.config = new ConfigBuilder().build();
        this.redisClient = new ClientPoolImpl(config, jedisPool);
        this.processingDelay = processingDelay;
        this.putJobMeter = metricRegistry.meter(name(RedisTaskQueue.class, "put-job"));
        this.queueSize = metricRegistry.histogram(name(RedisTaskQueue.class, "queue-size"));
        this.failures = metricRegistry.meter(name(RedisTaskQueue.class, "failures"));
        this.timer = new Timer();
    }

    void close() {
        timer.cancel();
        synchronized(this) {
            if (workerPool != null) {
                workerPool.end(true);
            }
        }
        redisClient.end();
    }

    void putJob(Task job) {
        putJobMeter.mark();
        LOG.debug("Enqueuing job {}", job.getTaskState().getId());
        final Job queueJob = new Job(SUBSCRIPTION_CLASS_NAME, job);
        redisClient.enqueue(QUEUE_NAME, queueJob);
    }

    void runInFlightProcessor() {
        timer.scheduleAtFixedRate(new RedisInflightTaskConsumer(jedisPool, Duration.ofSeconds(
                processingDelay), config, QUEUE_NAME), new Date(), 2000);
    }

    void subscribe(
            RedisTaskManager redisTaskManager,
            EngineID engineId,
            GraknEngineConfig engineConfig,
            EngineGraknGraphFactory factory,
            int poolSize) {
        LOG.info("Subscribing worker to jobs in queue {}", QUEUE_NAME);
        // sync to avoid close while starting
        synchronized(this) {
            this.workerPool = new WorkerPool(() -> getWorker(redisTaskManager, engineId, engineConfig, factory), poolSize);
            // This just starts poolSize threads
            workerPool.run();
        }
    }

    private Worker getWorker(RedisTaskManager redisTaskManager, EngineID engineId,
            GraknEngineConfig engineConfig, EngineGraknGraphFactory factory) {
        Worker worker = new WorkerPoolImpl(config, Arrays.asList(QUEUE_NAME), JOB_FACTORY, jedisPool);
        // We need this since the job can only be instantiated with the
        // task coming from the queue
        worker.getWorkerEventEmitter().addListener(
                (event, worker1, queue, job, runner, result, t) -> {
                    queueSize.update(queue.length());
                    if (runner instanceof RedisTaskQueueConsumer) {
                        ((RedisTaskQueueConsumer) runner)
                                .setRunningState(redisTaskManager, engineId, engineConfig, jedisPool,
                                        factory, lockProvider, metricRegistry);
                    } else {
                        LOG.error("Found unexoected job in queue of type {}", runner.getClass().getName());
                    }
                }, WorkerEvent.JOB_EXECUTE);
        worker.setExceptionHandler((jobExecutor, exception, curQueue) -> {
            // TODO review this strategy
            failures.mark();
            LOG.error("Exception while trying to run task, terminating!", exception);
            return RecoveryStrategy.TERMINATE;
        });
        return worker;
    }
}