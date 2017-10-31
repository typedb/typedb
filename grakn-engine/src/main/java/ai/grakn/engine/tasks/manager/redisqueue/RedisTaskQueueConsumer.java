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
import ai.grakn.engine.factory.EngineGraknTxFactory;
import ai.grakn.engine.postprocessing.PostProcessor;
import ai.grakn.engine.tasks.BackgroundTask;
import ai.grakn.engine.tasks.manager.TaskConfiguration;
import ai.grakn.engine.tasks.manager.TaskState;
import ai.grakn.engine.util.EngineID;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.function.Consumer;

import static ai.grakn.engine.TaskStatus.FAILED;
import static ai.grakn.engine.TaskStatus.RUNNING;
import static ai.grakn.engine.TaskStatus.STOPPED;
import static com.codahale.metrics.MetricRegistry.name;

/**
 * Consumer from a redis queue
 *
 * @author Domenico Corapi
 */
public class RedisTaskQueueConsumer implements Consumer<Task> {

    private final static Logger LOG = LoggerFactory.getLogger(RedisTaskQueueConsumer.class);

    private final RedisTaskManager redisTaskManager;
    private final EngineID engineId;
    private final GraknEngineConfig config;
    private final MetricRegistry metricRegistry;
    private final EngineGraknTxFactory factory;
    private final PostProcessor postProcessor;


    public RedisTaskQueueConsumer(
            RedisTaskManager redisTaskManager, EngineID engineId,
            GraknEngineConfig config,
            MetricRegistry metricRegistry,
            EngineGraknTxFactory factory, PostProcessor postProcessor) {
        this.redisTaskManager = redisTaskManager;
        this.engineId = engineId;
        this.config = config;
        this.metricRegistry = metricRegistry;
        this.factory = factory;
        this.postProcessor = postProcessor;
    }

    private void checkPreconditions() {
        try {
            Preconditions.checkNotNull(metricRegistry);
            Preconditions.checkNotNull(engineId);
            Preconditions.checkNotNull(config);
            Preconditions.checkNotNull(redisTaskManager);
            Preconditions.checkNotNull(postProcessor);
        } catch (NullPointerException e) {
            throw new IllegalStateException(
                    String.format("%s was started but the state wasn't set explicitly",
                            this.getClass().getName()));
        }
    }

    private boolean taskShouldRecur(TaskState taskState) {
        return taskState.schedule().isRecurring() && !taskState.status().equals(FAILED)
                && !taskState.status().equals(STOPPED);
    }

    private boolean taskShouldResume(Task task) {
        return task.getTaskState().status() == RUNNING;
    }


    @Override
    public void accept(Task task) {
        checkPreconditions();
        Timer executeTimer = metricRegistry
                .timer(name(RedisTaskQueueConsumer.class, "execute"));
        Context context = executeTimer.time();
        TaskState taskState = task.getTaskState();
        TaskConfiguration taskConfiguration = task.getTaskConfiguration();
        BackgroundTask runningTask;
        try {
            runningTask = taskState.taskClass().newInstance();
            runningTask.initialize(taskConfiguration, redisTaskManager, config, factory,
                    metricRegistry, postProcessor);
            metricRegistry.meter(name(RedisTaskQueueConsumer.class, "initialized")).mark();
            if (taskShouldResume(task)) {
                // Not implemented
                throw new NotImplementedException();
            } else {
                runningTask.start();
                metricRegistry.meter(name(RedisTaskQueueConsumer.class, "run")).mark();
            }
            if (taskShouldRecur(taskState)) {
                // Not implemented
                throw new NotImplementedException();
            }
        } catch (IllegalAccessException | InstantiationException e) {
            metricRegistry.meter(name(RedisTaskQueueConsumer.class, "failed")).mark();
            LOG.error("{} had an instantiantion exception", task.getTaskState().getId(), e);
            throw new RuntimeException(e);
        } catch (RuntimeException throwable) {
            metricRegistry.meter(name(RedisTaskQueueConsumer.class, "failed")).mark();
            LOG.error("{} could not be completed successfully", task.getTaskState().getId(), throwable);
            throw new RuntimeException(throwable);
        } finally {
            context.stop();
        }
    }
}
