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

package ai.grakn.engine.tasks.manager;

import ai.grakn.engine.GraknEngineConfig;
import ai.grakn.engine.tasks.BackgroundTask;
import ai.grakn.engine.tasks.TaskCheckpoint;
import ai.grakn.engine.tasks.TaskState;
import ai.grakn.engine.tasks.connection.RedisCountStorage;
import ai.grakn.engine.tasks.manager.singlequeue.SingleQueueTaskRunner;
import ai.grakn.engine.util.EngineID;
import com.codahale.metrics.MetricRegistry;
import static com.codahale.metrics.MetricRegistry.name;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import static net.greghaines.jesque.worker.RecoveryStrategy.PROCEED;
import net.greghaines.jesque.worker.Worker;
import net.greghaines.jesque.worker.WorkerListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * Consumer from a redis queue
 *
 * @author Domenico Corapi
 */
public class RedisTaskQueueConsumer {
    private final static Logger LOG = LoggerFactory.getLogger(RedisTaskQueueConsumer.class);

    private final RedisTaskManager redisTaskManager;
    private final EngineID engineId;
    private final GraknEngineConfig config;
    private final RedisCountStorage redisCountStorage;
    private final MetricRegistry metricRegistry;

    private final Timer executeTimer;

    public RedisTaskQueueConsumer(
            RedisTaskManager redisTaskManager,
            EngineID engineId,
            GraknEngineConfig config,
            RedisCountStorage redisCountStorage,
            MetricRegistry metricRegistry) {
        this.redisTaskManager = redisTaskManager;
        this.engineId = engineId;
        this.config = config;
        this.redisCountStorage = redisCountStorage;
        this.metricRegistry = metricRegistry;
        this.executeTimer = metricRegistry
                .timer(name(SingleQueueTaskRunner.class, "execute"));
    }

    public void start(Executor executor) {
        // TODO copied from previous version, it has some unimplemented parts
        Worker worker = redisTaskManager.getQueue().getJobSubscriber();
        worker.setExceptionHandler((jobExecutor, exception, curQueue) -> {
            LOG.error("Unhandled exception while running consumer:", exception);
            return PROCEED;
        });
        executor.execute(worker);
    }

    class JobRunner implements Runnable {
        private WorkerListener jobRunner() {
            return (event, worker1, queue, job, runner, result, t) -> {
                Context context = executeTimer.time();
                Task task;
                BackgroundTask runningTask;
                task = (Task) job.getArgs()[0];
                try {
                    TaskState taskState = task.getTaskState();
                    runningTask = taskState.taskClass().newInstance();
                    runningTask.initialize(saveCheckpoint(task), task.getTaskConfiguration(), redisTaskManager, config,
                            redisCountStorage, metricRegistry);
                    boolean completed;

                    if(taskShouldResume(task)){
                        // Not implemented
                        throw new NotImplementedException();
                    } else {
                        //Mark as running
                        taskState.markRunning(engineId);
                        redisTaskManager.storage().newState(taskState);
                        LOG.debug("{}\tmarked as running", task);
                        completed = runningTask.start();
                    }
                    if (completed) {
                        task.getTaskState().markCompleted();
                    } else {
                        task.getTaskState().markStopped();
                    }
                } catch (Throwable throwable) {
                    task.getTaskState().markFailed(throwable);
                    LOG.error("{}\tfailed with {}", task.getId(), throwable.getMessage());
                } finally {
                    context.stop();
                }

            };
        }

        @Override
        public void run() {
            jobRunner();
        }
    }

    private boolean taskShouldResume(Task task) {
        // TODO
        return false;
    }

    private Consumer<TaskCheckpoint> saveCheckpoint(Task task) {
        // TODO
        return null;
    }
}
