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

import ai.grakn.engine.tasks.BackgroundTask;
import ai.grakn.engine.tasks.TaskId;
import ai.grakn.engine.tasks.TaskManager;
import ai.grakn.engine.tasks.TaskSchedule;
import ai.grakn.engine.tasks.TaskState;
import ai.grakn.engine.tasks.TaskStateStorage;
import ai.grakn.engine.tasks.storage.TaskStateInMemoryStore;
import ai.grakn.engine.util.ConfigProperties;
import ai.grakn.engine.util.EngineID;
import javafx.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

import static ai.grakn.engine.TaskStatus.COMPLETED;
import static ai.grakn.engine.TaskStatus.RUNNING;
import static ai.grakn.engine.TaskStatus.SCHEDULED;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace;

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
    private static final String SAVE_CHECKPOINT_NAME = "Save task checkpoint.";

    private final Logger LOG = LoggerFactory.getLogger(StandaloneTaskManager.class);

    private final Map<TaskId, Pair<ScheduledFuture<?>, BackgroundTask>> instantiatedTasks;
    private final TaskStateStorage stateStorage;
    private final ReentrantLock stateUpdateLock;

    private final ExecutorService executorService;
    private final ScheduledExecutorService schedulingService;
    private final EngineID engineID;

    public StandaloneTaskManager(EngineID engineId) {
        this.engineID = engineId;
        instantiatedTasks = new ConcurrentHashMap<>();
        stateStorage = new TaskStateInMemoryStore();
        stateUpdateLock = new ReentrantLock();

        ConfigProperties properties = ConfigProperties.getInstance();
        schedulingService = Executors.newScheduledThreadPool(1);
        executorService = Executors.newFixedThreadPool(properties.getAvailableThreads());
    }

    public TaskManager open() {
        return this;
    }

    @Override
    public void close(){
        executorService.shutdown();
        schedulingService.shutdown();
        instantiatedTasks.clear();
    }

    @Override
    public void addTask(TaskState taskState){
        stateStorage.newState(taskState);

        // Schedule task to run.
        Instant now = Instant.now();
        TaskSchedule schedule = taskState.schedule();
        long delay = Duration.between(now, schedule.runAt()).toMillis();
        try {
            stateStorage.updateState(taskState.markScheduled());

            // Instantiate task.
            BackgroundTask task = taskState.taskClass().newInstance();

            ScheduledFuture<?> future = schedule.interval().map(interval ->
                schedulingService.scheduleAtFixedRate(runTask(taskState.getId(), task, true), delay, interval.toMillis(), MILLISECONDS)
            ).orElseGet(() ->
                (ScheduledFuture) schedulingService.schedule(runTask(taskState.getId(), task, false), delay, MILLISECONDS)
            );

            instantiatedTasks.put(taskState.getId(), new Pair<>(future, task));
        }
        catch (Throwable throwable) {
            LOG.error(getFullStackTrace(throwable));
            stateStorage.updateState(taskState.markFailed(throwable));
            instantiatedTasks.remove(taskState.getId());
        }
    }

    public void stopTask(TaskId id, String requesterName) {
        stateUpdateLock.lock();

        try {
            TaskState state = stateStorage.getState(id);
            if (state == null) {
                return;
            }

            Pair<ScheduledFuture<?>, BackgroundTask> pair = instantiatedTasks.get(id);
            synchronized (pair) {
                if (state.status() == SCHEDULED || (state.status() == COMPLETED && state.schedule().isRecurring())) {
                    LOG.info("Stopping a currently scheduled task " + id);
                    pair.getKey().cancel(true);
                    state.markStopped();
                } else if (state.status() == RUNNING) {
                    LOG.info("Stopping running task " + id);

                    BackgroundTask task = pair.getValue();
                    if (task != null) {
                        task.stop();
                    }

                    state.markStopped();
                } else {
                    LOG.warn("Task not running - " + id);
                }

                stateStorage.updateState(state);
            }
        } finally {
            stateUpdateLock.unlock();
        }
    }

    public TaskStateStorage storage() {
        return stateStorage;
    }

    private Runnable exceptionCatcher(TaskState state, BackgroundTask task) {
        return () -> {
            stateUpdateLock.lock();
            try {
                task.start(saveCheckpoint(state), state.configuration());

                if(state.status() == RUNNING) {
                    state.markCompleted();
                }
            }
            catch (Throwable throwable) {
                LOG.error(getFullStackTrace(throwable));
                state.markFailed(throwable);
            } finally {
                stateStorage.updateState(state);
                stateUpdateLock.unlock();
            }
        };
    }

    private Runnable runTask(TaskId id, BackgroundTask task, Boolean recurring) {
        return () -> {
            stateUpdateLock.lock();

            try {
                TaskState state = stateStorage.getState(id);
                if (state.status() == SCHEDULED || (recurring && state.status() == COMPLETED)) {
                    stateStorage.updateState(state.markRunning(engineID));
                    executorService.submit(exceptionCatcher(state, task));
                }
            } finally {
                stateUpdateLock.unlock();
            }
        };
    }

    private Consumer<String> saveCheckpoint(TaskState state) {
        return s -> {
            stateUpdateLock.lock();
            stateStorage.updateState(state.checkpoint(SAVE_CHECKPOINT_NAME));
            stateUpdateLock.unlock();
        };
    }
}
