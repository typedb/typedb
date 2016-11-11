/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.engine.backgroundtasks;

import io.mindmaps.engine.util.ConfigProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static io.mindmaps.engine.backgroundtasks.TaskStatus.*;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class InMemoryTaskManager extends AbstractTaskManager {
    private static String STATUS_MESSAGE_SCHEDULED = "Task scheduled.";
    private final Logger LOG = LoggerFactory.getLogger(InMemoryTaskManager.class);
    private static InMemoryTaskManager instance = null;

    private Map<UUID, TaskState> taskStateStorage;
    private Map<UUID, ScheduledFuture<BackgroundTask>> taskStorage;

    private ScheduledExecutorService executorService;

    private InMemoryTaskManager() {
        taskStateStorage = new ConcurrentHashMap<>();
        taskStorage = new ConcurrentHashMap<>();

        ConfigProperties properties = ConfigProperties.getInstance();
        // One thread is reserved for the supervisor
        executorService = Executors.newScheduledThreadPool(properties.getAvailableThreads()-1);
        //FIXME: get from config
        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(this::supervisor, 2000, 1000, MILLISECONDS);
    }

    public static synchronized InMemoryTaskManager getInstance() {
        if (instance == null)
            instance = new InMemoryTaskManager();
        return instance;
    }

    /*
    TaskManager required.
     */
    public TaskState getTaskState(String id) {
        return taskStateStorage.get(UUID.fromString(id));
    }

    public Set<String> getAllTasks() {
        return taskStateStorage.keySet().stream().map(UUID::toString).collect(Collectors.toSet());
    }

    public Set<String> getTasks(TaskStatus taskStatus) {
        return taskStateStorage.entrySet().stream()
                .filter(x -> x.getValue().getStatus() == taskStatus)
                .map(x -> x.getKey().toString())
                .collect(Collectors.toSet());
    }

    /*
    AbstractTaskManage required.
     */
    protected String saveNewState(TaskState state) {
        state.setStatus(TaskStatus.SCHEDULED)
             .setStatusChangeMessage(STATUS_MESSAGE_SCHEDULED)
             .setStatusChangedBy(InMemoryTaskManager.class.getName())
             .setQueuedTime(new Date());

       UUID uuid = UUID.randomUUID();
       taskStateStorage.put(uuid, state);
       return uuid.toString();
    }

    protected String updateTaskState(String id, TaskState state) {
        taskStateStorage.put(UUID.fromString(id), state);
        return id;
    }


    protected void executeSingle(String id, BackgroundTask task, long delay) {
        ScheduledFuture<BackgroundTask> f = (ScheduledFuture<BackgroundTask>) executorService.schedule(
                runTask(id, task::start), delay, MILLISECONDS);

        UUID uuid = UUID.fromString(id);
        taskStorage.put(uuid, f);
    }

    protected void executeRecurring(String id, BackgroundTask task, long delay, long interval) {
        ScheduledFuture<BackgroundTask> f = (ScheduledFuture<BackgroundTask>) executorService.scheduleAtFixedRate(
                runTask(id, task::start), delay, interval, MILLISECONDS);

        UUID uuid = UUID.fromString(id);
        taskStorage.put(uuid, f);
    }

    protected ScheduledFuture<BackgroundTask> getTaskExecutionStatus(String id) {
        return taskStorage.get(UUID.fromString(id));
    }

    /*
    Internal Methods.
     */
    private Runnable runTask(String id, Runnable fn) {
        return () -> {
            TaskState state = getTaskState(id);
            if(state.getStatus() == STOPPED || state.getStatus() == PAUSED)
                return;

            state.setStatus(RUNNING).setStatusChangedBy("runTask");
            updateTaskState(id, state);
            fn.run();
        };
    }

    private void supervisor() {
        taskStorage.entrySet().forEach(x -> {
            // Validity check
            if(!taskStateStorage.containsKey(x.getKey())) {
                LOG.error("INTERNAL STATE DESYNCHRONISED: taskStorage("+x.getKey().toString()+") is not present in taskStateStorage! CANCELLING TASK.");
                x.getValue().cancel(true);
                taskStorage.remove(x.getKey());
            }

            // Update state of current tasks
            else {
                ScheduledFuture<BackgroundTask> f = x.getValue();

                TaskState state = getTaskState(x.getKey().toString());

                if(f.isDone()) {
                    if(f.isCancelled()) {
                        if(state.getStatus() != STOPPED && state.getStatus() != PAUSED)
                            state.setStatus(TaskStatus.DEAD)
                                 .setStatusChangedBy(this.getClass().getName()+" supervisor");
                    } else {
                        state.setStatus(COMPLETED);
                    }
                }

                //taskStateStorage.put(x.getKey(), state);
                updateTaskState(x.getKey().toString(), state);
            }
        });
    }
}
