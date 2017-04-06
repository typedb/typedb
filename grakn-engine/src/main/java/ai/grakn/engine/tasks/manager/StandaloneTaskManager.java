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

import ai.grakn.engine.cache.EngineCacheProvider;
import ai.grakn.engine.cache.EngineCacheStandAlone;
import ai.grakn.engine.tasks.BackgroundTask;
import ai.grakn.engine.TaskId;
import ai.grakn.engine.tasks.TaskCheckpoint;
import ai.grakn.engine.tasks.TaskManager;
import ai.grakn.engine.tasks.TaskSchedule;
import ai.grakn.engine.tasks.TaskState;
import ai.grakn.engine.tasks.TaskStateStorage;
import ai.grakn.engine.tasks.storage.TaskStateInMemoryStore;
import ai.grakn.engine.GraknEngineConfig;
import ai.grakn.engine.util.EngineID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

import static ai.grakn.engine.TaskStatus.COMPLETED;
import static ai.grakn.engine.TaskStatus.CREATED;
import static ai.grakn.engine.TaskStatus.RUNNING;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * <p>
 * In-memory task manager to schedule and execute tasks. By default this task manager uses an in-memory
 * state storage.
 * </p>
 *
 * <p>
 * If engine fails while using this task manager, there will be no possibility for task recovery.
 * </p>
 *
 * @author Denis Lobanov, alexandraorth
 */
public class StandaloneTaskManager implements TaskManager {
    private final Logger LOG = LoggerFactory.getLogger(StandaloneTaskManager.class);

    private final Map<TaskId, BackgroundTask> runningTasks;

    private final TaskStateStorage storage;
    private final ReentrantLock stateUpdateLock;

    private final ExecutorService executorService;
    private final ScheduledExecutorService schedulingService;
    private final EngineID engineID;

    public StandaloneTaskManager(EngineID engineId) {
        this.engineID = engineId;

        runningTasks = new ConcurrentHashMap<>();

        storage = new TaskStateInMemoryStore();
        stateUpdateLock = new ReentrantLock();

        GraknEngineConfig properties = GraknEngineConfig.getInstance();
        schedulingService = Executors.newScheduledThreadPool(1);
        executorService = Executors.newFixedThreadPool(properties.getAvailableThreads());

        EngineCacheProvider.init(EngineCacheStandAlone.getCache());
    }

    public TaskManager open() {
        return this;
    }

    @Override
    public void close(){
        executorService.shutdown();
        schedulingService.shutdown();
        runningTasks.clear();
        EngineCacheProvider.clearCache();
    }

    @Override
    public void addTask(TaskState taskState){
        storage.newState(taskState);

        // Schedule task to run.
        Instant now = Instant.now();
        TaskSchedule schedule = taskState.schedule();
        long delay = Duration.between(now, taskState.schedule().runAt()).toMillis();

        Runnable taskExecution = submitTaskForExecution(taskState);

        if(schedule.isRecurring()){
            schedulingService.scheduleAtFixedRate(taskExecution, delay, schedule.interval().get().toMillis(), MILLISECONDS);
        } else {
            schedulingService.schedule(taskExecution, delay, MILLISECONDS);
        }
    }

    public void stopTask(TaskId id) {
        TaskState state = storage.getState(id);

        try {

            // Task has not been run- Mark the task as stopped and it will not run when picked up by the executor
            if (shouldRunTask(state)) {
                LOG.info("Stopping a currently scheduled task {}", id);

                state.markStopped();
            }

            // Kill the currently running task if it is running
            else if (state.status() == RUNNING && runningTasks.containsKey(id)) {
                LOG.info("Stopping running task {}", id);

                // Stop the task
                runningTasks.get(id).stop();

                state.markStopped();
            } else {
                LOG.warn("Task not running {}, was not stopped", id);
            }

        } finally {
            saveState(state);
        }
    }

    public TaskStateStorage storage() {
        return storage;
    }

    private Runnable executeTask(TaskState task) {
        return () -> {
            try {
                task.markRunning(engineID);

                saveState(task);

                BackgroundTask runningTask = task.taskClass().newInstance();
                runningTasks.put(task.getId(), runningTask);

                boolean completed = runningTask.start(saveCheckpoint(task), task.configuration());

                if (completed) {
                    task.markCompleted();
                } else {
                    task.markStopped();
                }
            }
            catch (Throwable throwable) {
                LOG.error("error", throwable);
                task.markFailed(throwable);
            } finally {
                saveState(task);
                runningTasks.remove(task.getId());
            }
        };
    }

    private Runnable submitTaskForExecution(TaskState taskState) {
        return () -> {
            if (shouldRunTask(storage.getState(taskState.getId()))) {
                executorService.submit(executeTask(taskState));
            }
        };
    }

    private boolean shouldRunTask(TaskState state){
        return state.status() == CREATED || state.schedule().isRecurring() && state.status() == COMPLETED;
    }

    private Consumer<TaskCheckpoint> saveCheckpoint(TaskState state) {
        return checkpoint -> saveState(state.checkpoint(checkpoint));
    }

    private void saveState(TaskState taskState){
        stateUpdateLock.lock();
        try {
            storage.updateState(taskState);
        } finally {
            stateUpdateLock.unlock();
        }
    }
}
