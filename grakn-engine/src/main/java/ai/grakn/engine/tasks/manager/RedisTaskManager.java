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

package ai.grakn.engine.tasks.manager;

import ai.grakn.engine.GraknEngineConfig;
import ai.grakn.engine.TaskId;
import ai.grakn.engine.tasks.TaskConfiguration;
import ai.grakn.engine.tasks.TaskManager;
import ai.grakn.engine.tasks.TaskState;
import ai.grakn.engine.tasks.TaskStateStorage;
import ai.grakn.engine.tasks.connection.RedisCountStorage;
import ai.grakn.engine.util.EngineID;
import com.codahale.metrics.MetricRegistry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPool;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * Handle the lifecycle of tasks in Redis
 *
 * @author pluraliseseverythings
 */
public class RedisTaskManager implements TaskManager {
    private final static Logger LOG = LoggerFactory.getLogger(RedisTaskManager.class);
    private final RedisTaskStorage redisTaskStorage;
    private final RedisTaskQueue<Task> redisTaskQueue;
    private final ExecutorService consumerExecutor;


    public RedisTaskManager(EngineID engineId, GraknEngineConfig graknEngineConfig, RedisCountStorage redisCountStorage, MetricRegistry metricsRegistry) {
        // TODO hacky way, the pool should be created in the main server class and passed here
        this(engineId, graknEngineConfig, redisCountStorage.getJedisPool(), 4, redisCountStorage, metricsRegistry);
    }

    public RedisTaskManager(EngineID engineId, GraknEngineConfig config, JedisPool jedisPool,
            int threads, RedisCountStorage redisCountStorage, MetricRegistry metricRegistry) {
        this.redisTaskStorage = RedisTaskStorage.create(jedisPool);
        this.redisTaskQueue = new RedisTaskQueue<>(jedisPool);
        this.consumerExecutor = Executors.newFixedThreadPool(threads);

        // TODO move this to a start method and add to TaskManager
        IntStream.rangeClosed(1, threads).forEach(
                ignored -> new RedisTaskQueueConsumer(
                        this, engineId, config, redisCountStorage, metricRegistry)
                        .start(consumerExecutor));
        LOG.debug("Redis task manager started");
    }

    @Override
    public void close() {
        this.consumerExecutor.shutdown();
        try {
            this.consumerExecutor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOG.error("Termination not completed", e);
        }
        LOG.debug("TaskManager closed");
    }

    @Override
    public void stopTask(TaskId id) {
        throw new NotImplementedException();
    }

    @Override
    public TaskStateStorage storage() {
        return redisTaskStorage;
    }

    @Override
    public void addTask(TaskState taskState, TaskConfiguration configuration){
        Task task = Task.builder()
                .setId(taskState.getId().getValue())
                .setTaskConfiguration(configuration)
                .setTaskState(taskState).build();
        redisTaskQueue.putJob(task);
    }

    public RedisTaskQueue<Task> getQueue() {
        return redisTaskQueue;
    }
}
