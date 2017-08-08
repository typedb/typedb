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
import static ai.grakn.engine.GraknEngineConfig.TASKS_RETRY_DELAY;
import ai.grakn.engine.TaskId;
import ai.grakn.engine.factory.EngineGraknGraphFactory;
import ai.grakn.engine.lock.LockProvider;
import ai.grakn.engine.tasks.manager.TaskConfiguration;
import ai.grakn.engine.tasks.manager.TaskManager;
import ai.grakn.engine.tasks.manager.TaskState;
import ai.grakn.engine.util.EngineID;
import com.codahale.metrics.MetricRegistry;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.util.Pool;

/**
 * Handle the lifecycle of tasks in Redis. Given a jedis pool
 * it starts a set of consumers that subscribe to the task queue.
 * Tasks can be added using the addTask method.
 *
 * @author pluraliseseverythings
 */
public class RedisTaskManager implements TaskManager {

    private final static Logger LOG = LoggerFactory.getLogger(RedisTaskManager.class);
    private final EngineID engineId;
    private final GraknEngineConfig config;
    private final RedisTaskStorage redisTaskStorage;
    private final EngineGraknGraphFactory factory;
    private final RedisTaskQueue redisTaskQueue;
    private final int threads;

    public RedisTaskManager(EngineID engineId, GraknEngineConfig config, Pool<Jedis> jedisPool,
            EngineGraknGraphFactory factory, LockProvider distributedLockClient,
            MetricRegistry metricRegistry) {
        this(engineId, config, jedisPool, Runtime.getRuntime().availableProcessors(), factory, distributedLockClient, metricRegistry);
    }

    public RedisTaskManager(EngineID engineId, GraknEngineConfig config, Pool<Jedis> jedisPool,
            int threads, EngineGraknGraphFactory factory, LockProvider distributedLockClient,
            MetricRegistry metricRegistry) {
        this.engineId = engineId;
        this.config = config;
        this.factory = factory;
        this.redisTaskStorage = RedisTaskStorage.create(jedisPool, metricRegistry);
        this.redisTaskQueue = new RedisTaskQueue(jedisPool, distributedLockClient, metricRegistry,
                Integer.parseInt(config.tryProperty(TASKS_RETRY_DELAY).orElse("180")));
        this.threads = threads;
    }

    @Override
    public void close() {
        LOG.info("Closing task manager");
        this.redisTaskQueue.close();
    }

    @Override
    public CompletableFuture<Void> start() {
        return CompletableFuture
                .runAsync(this::startBlocking)
                .exceptionally(e -> {
                    close();
                    throw new RuntimeException("Failed to intitialize subscription");
                });
    }

    private void startBlocking() {
        redisTaskQueue.runInFlightProcessor();
        for (int i = 0; i < threads; i++) {
            redisTaskQueue.subscribe(this, engineId, config, factory, threads);
        }
        LOG.info("Redis task manager started with {} subscriptions", threads);
    }

    @Override
    public void stopTask(TaskId id) {
        // TODO Just a prototype, make sure this work in all scenarios
        TaskState task = redisTaskStorage.getState(id);
        if (task == null) {
            // TODO make sure other parts of the code can handle a partially defined task state
            // TODO also fix the Java representation of a task state
            task = TaskState.of(id);
        }
        try {
            task.markStopped();
            redisTaskStorage.updateState(task);
        } catch (Exception e) {
            LOG.error("Unexpected error while stopping {}", id);
            throw e;
        }
    }

    @Override
    public RedisTaskStorage storage() {
        return redisTaskStorage;
    }

    @Override
    public void addTask(TaskState taskState, TaskConfiguration configuration) {
        Task task = Task.builder()
                .setTaskConfiguration(configuration)
                .setTaskState(taskState).build();
        redisTaskQueue.putJob(task);
    }

    public RedisTaskQueue getQueue() {
        return redisTaskQueue;
    }
}
