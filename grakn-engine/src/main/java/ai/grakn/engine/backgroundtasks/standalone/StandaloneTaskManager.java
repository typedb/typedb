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

package ai.grakn.engine.backgroundtasks.standalone;

import ai.grakn.engine.backgroundtasks.BackgroundTask;
import ai.grakn.engine.backgroundtasks.TaskStateStorage;
import ai.grakn.engine.backgroundtasks.TaskManager;
import ai.grakn.engine.backgroundtasks.TaskState;
import ai.grakn.engine.backgroundtasks.taskstatestorage.TaskStateInMemoryStore;
import ai.grakn.engine.util.ConfigProperties;
import javafx.util.Pair;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

import static ai.grakn.engine.backgroundtasks.TaskStatus.COMPLETED;
import static ai.grakn.engine.backgroundtasks.TaskStatus.FAILED;
import static ai.grakn.engine.backgroundtasks.TaskStatus.RUNNING;
import static ai.grakn.engine.backgroundtasks.TaskStatus.SCHEDULED;
import static ai.grakn.engine.backgroundtasks.TaskStatus.STOPPED;
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
    private static final String RUN_ONCE_NAME = "One off task scheduler.";
    private static final String RUN_RECURRING_NAME = "Recurring task scheduler.";
    private static final String EXCEPTION_CATCHER_NAME = "Task Exception Catcher.";
    private static final String SAVE_CHECKPOINT_NAME = "Save task checkpoint.";

    private static StandaloneTaskManager instance = null;

    private final Logger LOG = LoggerFactory.getLogger(StandaloneTaskManager.class);

    private final Map<String, Pair<ScheduledFuture<?>, BackgroundTask>> instantiatedTasks;
    private final TaskStateStorage stateStorage;
    private final ReentrantLock stateUpdateLock;

    private final ExecutorService executorService;
    private final ScheduledExecutorService schedulingService;

    private StandaloneTaskManager() {
        instantiatedTasks = new ConcurrentHashMap<>();
        stateStorage = new TaskStateInMemoryStore();
        stateUpdateLock = new ReentrantLock();

        ConfigProperties properties = ConfigProperties.getInstance();
        schedulingService = Executors.newScheduledThreadPool(1);
        executorService = Executors.newFixedThreadPool(properties.getAvailableThreads());
    }

    public static synchronized StandaloneTaskManager getInstance() {
        if (instance == null) {
            instance = new StandaloneTaskManager();
        }
        return instance;
    }

    public TaskManager open() {
        return this;
    }

    @Override
    public void close(){
        executorService.shutdown();
        schedulingService.shutdown();
        removeInstance();
    }

    private static void removeInstance() {
        instance = null;
    }

    public String scheduleTask(BackgroundTask task, String createdBy, Instant runAt, long period, JSONObject configuration) {
        Boolean recurring = (period != 0);

        TaskState taskState = new TaskState(task.getClass().getName())
                .creator(createdBy)
                .runAt(runAt)
                .isRecurring(recurring)
                .interval(period)
                .configuration(configuration);

        stateStorage.newState(taskState);

        // Schedule task to run.
        Instant now = Instant.now();
        long delay = Duration.between(runAt, now).toMillis();

        try {
            stateStorage.updateState(taskState.status(SCHEDULED).statusChangedBy(this.getClass().getName()));

            ScheduledFuture<?> future;
            if(recurring) {
                future = schedulingService.scheduleAtFixedRate(runTask(taskState.getId(), task, true), delay, period, MILLISECONDS);
            } else {
                future = schedulingService.schedule(runTask(taskState.getId(), task, false), delay, MILLISECONDS);
            }

            instantiatedTasks.put(taskState.getId(), new Pair<>(future, task));

        }
        catch (Throwable t) {
            LOG.error(getFullStackTrace(t));
            stateStorage.updateState(taskState.status(FAILED).exception(getFullStackTrace(t)));
            instantiatedTasks.remove(taskState.getId());
            return null;
        }

        return taskState.getId();
    }

    public CompletableFuture completableFuture(String taskId) {
        if(!instantiatedTasks.containsKey(taskId)){
            return null;
        }

        try {
            return CompletableFuture.runAsync(() -> {
                try {
                    while(true){
                        TaskState state = storage().getState(taskId);
                        if(state.status().equals(COMPLETED) || state.status().equals(FAILED)){
                            break;
                        }
                        Thread.sleep(1000);
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
        } catch (Throwable t){
            LOG.error(getFullStackTrace(t));
            throw new RuntimeException(t);
        }
    }

    public TaskManager stopTask(String id, String requesterName) {
        try {
            stateUpdateLock.lock();

            TaskState state = stateStorage.getState(id);
            if (state == null) {
                return this;
            }

            Pair<ScheduledFuture<?>, BackgroundTask> pair = instantiatedTasks.get(id);
            synchronized (pair) {
                if (state.status() == SCHEDULED || (state.status() == COMPLETED && state.isRecurring())) {
                    LOG.info("Stopping a currently scheduled task " + id);
                    pair.getKey().cancel(true);
                    stateStorage.updateState(state.status(STOPPED));
                } else if (state.status() == RUNNING) {
                    LOG.info("Stopping running task " + id);

                    BackgroundTask task = pair.getValue();
                    if (task != null) {
                        task.stop();
                    }

                    stateStorage.updateState(state.status(STOPPED));
                } else {
                    LOG.warn("Task not running - " + id);
                }
            }
        } finally {
            stateUpdateLock.unlock();
        }

        return this;
    }

    public TaskStateStorage storage() {
        return stateStorage;
    }

    private Runnable exceptionCatcher(TaskState state, BackgroundTask task) {
        return () -> {
            try {
                task.start(saveCheckpoint(state), state.configuration());

                stateUpdateLock.lock();
                if(state.status() == RUNNING) {
                    stateStorage.updateState(state.status(COMPLETED).statusChangedBy(EXCEPTION_CATCHER_NAME));
                }
                stateUpdateLock.unlock();
            }
            catch (Throwable t) {
                LOG.error(getFullStackTrace(t));
                stateStorage.updateState(state
                        .status(FAILED)
                        .statusChangedBy(EXCEPTION_CATCHER_NAME)
                        .exception(getFullStackTrace(t)));
            }
        };
    }

    private Runnable runTask(String id, BackgroundTask task, Boolean recurring) {
        return () -> {
            stateUpdateLock.lock();

            TaskState state = stateStorage.getState(id);
            if(recurring && (state.status() == SCHEDULED || state.status() == COMPLETED)) {
                stateStorage.updateState(state.status(RUNNING).isRecurring(true));
                executorService.submit(exceptionCatcher(state, task));
            }
            else if (!recurring && state.status() == SCHEDULED) {
                stateStorage.updateState(state.status(RUNNING).isRecurring(false));
                executorService.submit(exceptionCatcher(state, task));
            }

            stateUpdateLock.unlock();
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
