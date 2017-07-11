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
 */

package ai.grakn.engine.tasks.manager.redisqueue;

import ai.grakn.engine.GraknEngineConfig;
import static ai.grakn.engine.TaskStatus.FAILED;
import static ai.grakn.engine.TaskStatus.RUNNING;
import static ai.grakn.engine.TaskStatus.STOPPED;
import ai.grakn.engine.factory.EngineGraknGraphFactory;
import ai.grakn.engine.lock.LockProvider;
import ai.grakn.engine.tasks.BackgroundTask;
import ai.grakn.engine.tasks.connection.RedisCountStorage;
import ai.grakn.engine.tasks.manager.TaskCheckpoint;
import ai.grakn.engine.tasks.manager.TaskConfiguration;
import ai.grakn.engine.tasks.manager.TaskState;
import ai.grakn.engine.tasks.manager.TaskStateStorage;
import ai.grakn.engine.util.EngineID;
import com.codahale.metrics.MetricRegistry;
import static com.codahale.metrics.MetricRegistry.name;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import static java.time.Instant.now;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.util.Pool;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * Consumer from a redis queue
 *
 * @author Domenico Corapi
 */
public class RedisTaskQueueConsumer implements Runnable {

    private final static Logger LOG = LoggerFactory.getLogger(RedisTaskQueueConsumer.class);
    private final static ObjectMapper objectMapper = new ObjectMapper();

    private RedisTaskManager redisTaskManager;
    private EngineID engineId;
    private GraknEngineConfig config;
    private RedisCountStorage redisCountStorage;
    private MetricRegistry metricRegistry;
    private Task task;
    private EngineGraknGraphFactory factory;
    private LockProvider lockProvider;

    @SuppressWarnings("unused")
    public RedisTaskQueueConsumer(String taskId, TaskState taskState, TaskConfiguration taskConfiguration) {
        this.task = Task.builder().setTaskConfiguration(taskConfiguration).setTaskState(taskState).build();
    }

    @SuppressWarnings("unused")
    public RedisTaskQueueConsumer(Task task) {
        this.task = task;
    }

    @SuppressWarnings("unused")
    public RedisTaskQueueConsumer(Map<String, Object> task) {
        this.task = objectMapper.convertValue(task, Task.class);
    }

    @Override
    public void run() {
        checkPreconditions();
        Timer executeTimer = metricRegistry
                .timer(name(RedisTaskQueueConsumer.class, "execute"));
        Context context = executeTimer.time();
        TaskState taskState = task.getTaskState();
        TaskConfiguration taskConfiguration = task.getTaskConfiguration();
        if (shouldStopTask(taskState)) {
            taskState.markStopped();
            redisTaskManager.storage().updateState(taskState);
            LOG.info("{}\t marked as stopped", task);
        } else if(shouldDelayTask(taskState)) {
            redisTaskManager.storage().updateState(taskState);
            LOG.info("{}\t resubmitted", task);
        } else {
            BackgroundTask runningTask;
            try {
                runningTask = taskState.taskClass().newInstance();
                runningTask.initialize(saveCheckpoint(taskState, redisTaskManager.storage()),
                        taskConfiguration, redisTaskManager, config, redisCountStorage, factory, lockProvider, metricRegistry);
                metricRegistry.meter(name(RedisTaskQueueConsumer.class, "initialized")).mark();
                boolean completed;
                if (taskShouldResume(task)) {
                    // Not implemented
                    throw new NotImplementedException();
                } else {
                    //Mark as running
                    taskState.markRunning(engineId);
                    redisTaskManager.storage().newState(taskState);
                    LOG.debug("{} marked as running", task);
                    completed = runningTask.start();
                    metricRegistry.meter(name(RedisTaskQueueConsumer.class, "run")).mark();
                }
                if (completed) {
                    taskState.markCompleted();
                } else {
                    taskState.markStopped();
                }
                if(taskShouldRecur(taskState)){
                    resubmitTask(taskState);
                }
                // TODO check if we can simplify the storage using just the queue or a different data structure
                redisTaskManager.storage().updateState(taskState);
            } catch (Throwable throwable) {
                taskState.markFailed(throwable);
                LOG.error("{} could not be completed successfully", task.getTaskState().getId(), throwable);
            } finally {
                redisTaskManager.storage().updateState(taskState);
                context.stop();
            }
        }
    }

    private void checkPreconditions() {
        try {
            Preconditions.checkNotNull(metricRegistry);
            Preconditions.checkNotNull(engineId);
            Preconditions.checkNotNull(config);
            Preconditions.checkNotNull(redisCountStorage);
            Preconditions.checkNotNull(redisTaskManager);
            Preconditions.checkNotNull(lockProvider);
        } catch (NullPointerException e){
            throw new IllegalStateException(String.format("%s was started but the state wasn't set explicitly", this.getClass().getName()));
        }
    }

    private void resubmitTask(TaskState taskState) {
        taskState.schedule(taskState.schedule().incrementByInterval());
    }

    private boolean taskShouldRecur(TaskState taskState) {
        return taskState.schedule().isRecurring() && !taskState.status().equals(FAILED) && !taskState.status().equals(STOPPED);
    }

    private boolean shouldDelayTask(TaskState taskState) {
        return !taskState.schedule().runAt().isBefore(now());
    }

    private boolean shouldStopTask(TaskState taskState) {
        return taskState.status() == STOPPED || redisTaskManager.storage().isTaskMarkedStopped(task.getTaskState().getId());
    }


    private boolean taskShouldResume(Task task) {
        return task.getTaskState().status() == RUNNING;
    }


    private java.util.function.Consumer<TaskCheckpoint> saveCheckpoint(TaskState taskState, TaskStateStorage storage) {
        return checkpoint -> storage.updateState(taskState.checkpoint(checkpoint));
    }


    public void setRunningState(RedisTaskManager redisTaskManager, EngineID engineId,
            GraknEngineConfig config, Pool<Jedis> jedisPool, EngineGraknGraphFactory factory,
            LockProvider lockProvider, MetricRegistry metricRegistry) {
        this.redisTaskManager = redisTaskManager;
        this.engineId = engineId;
        this.config = config;
        this.redisCountStorage = RedisCountStorage.create(jedisPool);
        this.lockProvider = lockProvider;
        this.metricRegistry = metricRegistry;
        this.factory = factory;
    }
}
