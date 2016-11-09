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

public class InMemoryTaskManager implements TaskManager {
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
        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(this::supervisor, 2000, 1000, TimeUnit.MILLISECONDS);
    }

    public static synchronized InMemoryTaskManager getInstance() {
        if (instance == null)
            instance = new InMemoryTaskManager();
        return instance;
    }

    public UUID scheduleTask(BackgroundTask task, long delay) {
        TaskState state = new TaskState(task.getClass().getName())
                .setRecurring(false)
                .setDelay(delay);

        UUID uuid = saveNewState(state);
        executeSingle(uuid, task, delay);
        return uuid;
    }

    public UUID scheduleRecurringTask(BackgroundTask task, long delay, long interval) {
        TaskState state = new TaskState(task.getClass().getName())
                .setRecurring(true)
                .setDelay(delay)
                .setInterval(interval);

        UUID uuid = saveNewState(state);
        executeRecurring(uuid, task, delay, interval);
        return uuid;
    }

    public TaskManager stopTask(UUID uuid, String requesterName, String message) {
        TaskState state = taskStateStorage.get(uuid);
        state.setStatus(TaskStatus.STOPPED)
             .setStatusChangeMessage(message)
             .setStatusChangedBy(requesterName);
        taskStateStorage.put(uuid, state);

        ScheduledFuture<BackgroundTask> f = taskStorage.get(uuid);
        try {
            if (!f.isDone())
                instantiateTask(state.getName()).stop();

            f.cancel(true);
        } catch(ClassNotFoundException | IllegalAccessException | InstantiationException e) {
            LOG.error("Could not run .stop() on task "+state.getName()+" id: "+uuid.toString()+" the error was: "+e.getMessage());
        }

        return this;
    }

    public TaskManager stopTask(UUID uuid) {
        return stopTask(uuid, null, null);
    }

    public TaskManager pauseTask(UUID uuid, String requesterName, String message) {
        TaskState state = taskStateStorage.get(uuid);
        TaskStatus status = state.getStatus();

        if(status == TaskStatus.SCHEDULED || status == TaskStatus.RUNNING) {
            // Mark task state as paused.
            state.setStatus(TaskStatus.PAUSED)
                    .setStatusChangeMessage(message)
                    .setStatusChangedBy(requesterName);

            ScheduledFuture<BackgroundTask> f = taskStorage.get(uuid);
            try {
                // Call tasks .pause() method if it's still running.
                if (status == TaskStatus.RUNNING && (!f.isDone()))
                    state.setPauseState(f.get().pause());

                f.cancel(true);
            } catch (InterruptedException | ExecutionException e) {
                LOG.error(e.getMessage());
            }

            taskStateStorage.put(uuid, state);
        } else {
            LOG.warn("Ignoring pause request from: "+Thread.currentThread().getStackTrace()[1].toString()+
                     " for task "+uuid.toString()+
                     " because its status is "+state.getStatus());
        }

        return this;
    }

    public TaskManager pauseTask(UUID uuid) {
        return pauseTask(uuid, null, null);
    }

    public TaskManager resumeTask(UUID uuid, String requesterName, String message) {
        TaskState state = taskStateStorage.get(uuid);
        if(state.getStatus() != TaskStatus.PAUSED) {
           LOG.warn("Ignoring resume request from: "+Thread.currentThread().getStackTrace()[1].toString()+
                     " for task "+uuid.toString()+
                     " because its status is "+state.getStatus());
            return this;
        }

        state.setStatus(TaskStatus.RUNNING)
            .setStatusChangeMessage(message)
            .setStatusChangedBy(requesterName);
        taskStateStorage.put(uuid, state);

        try {
            BackgroundTask task = instantiateTask(state.getName());

            if(state.getRecurring())
                executeRecurring(uuid, task, state.getDelay(), state.getInterval());
            else
                executeSingle(uuid, task, state.getDelay());

        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            LOG.error("Unable to resume task "+uuid.toString()+" the error was: "+e.getMessage());
        }

        return this;
    }

    public TaskManager resumeTask(UUID uuid) {
        return resumeTask(uuid, null, null);
    }

    public TaskManager restartTask(UUID uuid, String requesterName, String message) {
        TaskState state = taskStateStorage.get(uuid);
        TaskStatus status = state.getStatus();
        if(status == TaskStatus.SCHEDULED || status == TaskStatus.CREATED || status == TaskStatus.RUNNING) {
            LOG.warn(Thread.currentThread().getStackTrace()[1].toString()+" tried to call restartTask() on a "+status.toString()+" task. IGNORING.");
            return this;
        }

        try {
            // Clean up etc
            BackgroundTask task = instantiateTask(state.getName());
            task.restart();

            // Start it up again
            if(state.getRecurring())
                executeRecurring(uuid, task, state.getDelay(), state.getInterval());
            else
                executeSingle(uuid, task, state.getDelay());

            // Update status message
            state.setStatus(TaskStatus.SCHEDULED)
                 .setStatusChangeMessage(message)
                 .setStatusChangedBy(requesterName);
            taskStateStorage.put(uuid, state);
        } catch(ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            LOG.error("Unable to start task "+uuid.toString()+" the error was: "+e.getMessage());
        }

        return this;
    }

    public TaskManager restartTask(UUID uuid) {
        return restartTask(uuid, null, null);
    }

    public TaskState getTaskState(UUID uuid) {
        return taskStateStorage.get(uuid);
    }

    public Set<UUID> getAllTasks() {
        return taskStateStorage.keySet();
    }

    public Set<UUID> getTasks(TaskStatus taskStatus) {
        return taskStateStorage.entrySet().stream()
                .filter(x -> x.getValue().getStatus() == taskStatus)
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
    }

    /*
    internal methods
     */
    private UUID saveNewState(TaskState state) {
        state.setStatus(TaskStatus.SCHEDULED)
             .setStatusChangeMessage(STATUS_MESSAGE_SCHEDULED)
             .setStatusChangedBy(InMemoryTaskManager.class.getName())
             .setQueuedTime(new Date());

       UUID uuid = UUID.randomUUID();
       taskStateStorage.put(uuid, state);
       return uuid;
    }

    private BackgroundTask instantiateTask(String className) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        Class<?> c = Class.forName(className);
        return (BackgroundTask) c.newInstance();
    }

    private void executeSingle(UUID uuid, BackgroundTask task, long delay) {
        ScheduledFuture<BackgroundTask> f = (ScheduledFuture<BackgroundTask>) executorService.schedule(
                runTask(uuid, task::start), delay, TimeUnit.MILLISECONDS);
        taskStorage.put(uuid, f);
    }

    private void executeRecurring(UUID uuid, BackgroundTask task, long delay, long interval) {
        ScheduledFuture<BackgroundTask> f = (ScheduledFuture<BackgroundTask>) executorService.scheduleAtFixedRate(
                runTask(uuid, task::start), delay, interval, TimeUnit.MILLISECONDS);
        taskStorage.put(uuid, f);
    }

    private Runnable runTask(UUID uuid, Runnable fn) {
        return () -> {
            TaskState state = taskStateStorage.get(uuid);
            state.setStatus(TaskStatus.RUNNING);
            taskStateStorage.put(uuid, state);
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
                TaskState state = taskStateStorage.get(x.getKey());

                if(f.isDone()) {
                    state.setStatus(TaskStatus.COMPLETED);
                }

                else if(f.isCancelled()) {
                    if(state.getStatus() != TaskStatus.STOPPED)
                        state.setStatus(TaskStatus.DEAD)
                        .setStatusChangedBy(this.getClass().getName()+" supervisor");
                }

                taskStateStorage.put(x.getKey(), state);
            }
        });
    }
}
