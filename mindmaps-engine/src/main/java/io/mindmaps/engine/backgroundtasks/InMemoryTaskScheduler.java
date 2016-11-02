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

public class InMemoryTaskScheduler implements TaskScheduler {
    private static String STATUS_MESSAGE_SCHEDULED = "Task scheduled.";

    private final Logger LOG = LoggerFactory.getLogger(InMemoryTaskScheduler.class);
    private static InMemoryTaskScheduler instance = null;

    private Map<UUID, TaskState> taskStateStorage;
    private Map<UUID, ScheduledFuture<BackgroundTask>> taskStorage;

    private ScheduledExecutorService executorService;

    private InMemoryTaskScheduler() {
        taskStateStorage = new HashMap<>();
        taskStorage = new HashMap<>();

        ConfigProperties properties = ConfigProperties.getInstance();
        // One thread is used internally to supervise tasks.
        executorService = Executors.newScheduledThreadPool(properties.getAvailableThreads()-1);
    }

    public static synchronized InMemoryTaskScheduler getInstance() {
        if (instance == null)
            instance = new InMemoryTaskScheduler();
        return instance;
    }

    public UUID scheduleTask(BackgroundTask task, long delay) {
        TaskState state = new TaskState(task.getClass().getName())
                .setRecurring(false)
                .setDelay(delay);

        UUID uuid = saveNewState(state);
        ScheduledFuture<BackgroundTask> f = (ScheduledFuture<BackgroundTask>) executorService.schedule(task::start, delay, TimeUnit.MILLISECONDS);
        taskStorage.put(uuid, f);

        return uuid;
    }

    public UUID scheduleRecurringTask(BackgroundTask task, long delay, long interval) {
        TaskState state = new TaskState(task.getClass().getName())
                .setRecurring(true)
                .setDelay(delay)
                .setInterval(interval);

        UUID uuid = saveNewState(state);
        ScheduledFuture<BackgroundTask> f = (ScheduledFuture<BackgroundTask>) executorService.scheduleAtFixedRate(task::start, delay, interval, TimeUnit.MILLISECONDS);
        taskStorage.put(uuid, f);

        return uuid;
    }

    public TaskScheduler stopTask(UUID uuid, String requesterName, String message) {
        TaskState state = taskStateStorage.get(uuid);
        state.setStatus(TaskStatus.STOPPED)
             .setStatusChangeMessage(message)
             .setStatusChangedBy(requesterName);
        taskStateStorage.put(uuid, state);

        try {
            ScheduledFuture<BackgroundTask> f = taskStorage.get(uuid);
            f.get().stop();
            f.cancel(true);
            taskStorage.put(uuid, f);
        } catch(InterruptedException | ExecutionException e) {
            LOG.error(e.getMessage());
        }

        return this;
    }

    public TaskScheduler stopTask(UUID uuid) {
        return stopTask(uuid, null, null);
    }

    public TaskScheduler pauseTask(UUID uuid, String requesterName, String message) {
        TaskState state = taskStateStorage.get(uuid);
        if(state.getStatus() != TaskStatus.RUNNING || state.getStatus() != TaskStatus.SCHEDULED) {
            LOG.warn("Ignoring pause request from: "+Thread.currentThread().getStackTrace()[1].toString()+
                     " for task "+uuid.toString()+
                     " because its status is "+state.getStatus());
            return this;
        }

        state.setStatus(TaskStatus.PAUSED)
             .setStatusChangeMessage(message)
             .setStatusChangedBy(requesterName);

        try {
            ScheduledFuture<BackgroundTask> f = taskStorage.get(uuid);
            state.setPauseState(f.get().pause());
            taskStateStorage.put(uuid, state);

            f.cancel(true);
            taskStorage.put(uuid, f);
        } catch(InterruptedException | ExecutionException e) {
            LOG.error(e.getMessage());
        }

        return this;
    }

    public TaskScheduler pauseTask(UUID uuid) {
        return pauseTask(uuid, null, null);
    }


    public TaskScheduler resumeTask(UUID uuid, String requesterName, String message) {
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
            Class<?> c = Class.forName(state.getName());
            BackgroundTask task = (BackgroundTask) c.newInstance();

            if(state.getRecurring())
                taskStorage.put(uuid,
                        (ScheduledFuture<BackgroundTask>)executorService.scheduleAtFixedRate(
                                task::start,
                                state.getDelay(),
                                state.getInterval(),
                                TimeUnit.MILLISECONDS));

            else
                taskStorage.put(uuid,
                        (ScheduledFuture<BackgroundTask>)executorService.schedule(
                                task::start,
                                state.getDelay(),
                                TimeUnit.MILLISECONDS));

        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            LOG.error("Unable to resume task "+uuid.toString()+" the error was: "+e.getMessage());
        }

        return this;
    }

    public TaskScheduler resumeTask(UUID uuid) {
        return resumeTask(uuid, null, null);
    }

    public TaskScheduler restartTask(UUID uuid, String requesterName, String message) {
        try {
            // Stop task
            stopTask(uuid, requesterName, message);

            // Clean up etc
            ScheduledFuture<BackgroundTask> f = taskStorage.get(uuid);
            f.get().restart();

            // Start it up again
            TaskState state = taskStateStorage.get(uuid);
            Class<?> c = Class.forName(state.getName());
            BackgroundTask task = (BackgroundTask) c.newInstance();

            if(state.getRecurring())
                scheduleRecurringTask(task, state.getDelay(), state.getInterval());
            else
                scheduleTask(task, state.getDelay());

            // Update status message
            state.setStatusChangeMessage(message)
                 .setStatusChangedBy(requesterName);
            taskStateStorage.put(uuid, state);

        } catch(InterruptedException | ExecutionException e) {
            LOG.error("Unable to call .restart() on task "+uuid.toString()+" the error was: "+e.getMessage());
        } catch(ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            LOG.error("Unable to start task "+uuid.toString()+" the error was: "+e.getMessage());
        }

        return this;
    }

    public TaskScheduler restartTask(UUID uuid) {
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
             .setStatusChangedBy(InMemoryTaskScheduler.class.getName())
             .setQueuedTime(new Date());

       UUID uuid = UUID.randomUUID();
       taskStateStorage.put(uuid, state);
       return uuid;
    }

















}
