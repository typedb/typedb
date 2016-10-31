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

import io.mindmaps.engine.backgroundtasks.types.BackgroundTask;
import io.mindmaps.engine.backgroundtasks.types.TaskStatus;
import io.mindmaps.engine.util.ConfigProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class InMemoryTaskRunner implements TaskRunner {
    private static String STATUS_MESSAGE_SCHEDULED = "Task scheduled.";
    private static String STATUS_MESSAGE_STOPPED = "Task stop requested.";
    private static String STATUS_MESSAGE_PAUSED = "Task pause requested.";
    private static String STATUS_MESSAGE_RESUMED = "Task resume requested.";

    private final Logger LOG = LoggerFactory.getLogger(InMemoryTaskRunner.class);
    private static InMemoryTaskRunner instance = null;
    private Map<UUID, BackgroundTask> storage;
    private ScheduledExecutorService scheduledThreadPool;

    private InMemoryTaskRunner() {
        storage = new HashMap<>();

        ConfigProperties properties = ConfigProperties.getInstance();
        scheduledThreadPool = Executors.newScheduledThreadPool(properties.getAvailableThreads());
    }

    public static synchronized InMemoryTaskRunner getInstance() {
        if (instance == null)
            instance = new InMemoryTaskRunner();
        return instance;
    }

    public UUID scheduleTask(BackgroundTask task, long delay) {
        UUID uuid = saveNewTask(task);

        scheduledThreadPool.schedule(task::start, delay, TimeUnit.MILLISECONDS);
        return uuid;
    }

    public UUID scheduleRecurringTask(BackgroundTask task, long delay, long interval) {
        UUID uuid = saveNewTask(task);

        scheduledThreadPool.scheduleAtFixedRate(task::start, delay, interval, TimeUnit.MILLISECONDS);
        return uuid;
    }

    public UUID scheduleDaemon(BackgroundTask task) {
        LOG.error(this.getClass().getName()+".scheduleDaemon() NOT IMPLEMENTED");

        task.setStatuChangeMessage("scheduleDaemon() NOT IMPLEMENTED");
        UUID uuid = UUID.randomUUID();
        storage.put(uuid, task);

        return uuid;
    }

    private UUID saveNewTask(BackgroundTask task) {
       task.setStatus(TaskStatus.SCHEDULED)
            .setStatusChangedBy(this.getClass().getName())
            .setStatuChangeMessage(STATUS_MESSAGE_SCHEDULED)
            .setQueuedTime(new Date());

       UUID uuid = UUID.randomUUID();
       storage.put(uuid, task);
       return uuid;
    }

    public TaskRunner stopTask(UUID uuid, String requesterName, String message) {
        BackgroundTask task = storage.get(uuid);
        task.setStatus(TaskStatus.STOPPED)
            .setStatusChangedBy(requesterName)
            .setStatuChangeMessage(message);

        storage.put(uuid, task);
        return this;
    }

    public TaskRunner stopTask(UUID uuid) {
        return stopTask(uuid, Thread.currentThread().getStackTrace()[1].toString(), STATUS_MESSAGE_STOPPED);
    }

    public TaskRunner pauseTask(UUID uuid, String requesterName, String message) {
        BackgroundTask task = storage.get(uuid);
        task.setStatus(TaskStatus.PAUSED)
            .setStatusChangedBy(requesterName)
            .setStatuChangeMessage(message);

        storage.put(uuid, task);

        return this;
    }

    public TaskRunner pauseTask(UUID uuid) {
        return pauseTask(uuid, "", STATUS_MESSAGE_PAUSED);
    }

    public TaskRunner resumeTask(UUID uuid, String requestName, String message) {
        BackgroundTask task = storage.get(uuid);
        task.setStatus(TaskStatus.RUNNING)
            .setStatuChangeMessage(message)
            .setStatusChangedBy(requestName);
        storage.put(uuid, task);

        return this;
    }

    public TaskRunner resumeTask(UUID uuid) {
        return resumeTask(uuid, "", STATUS_MESSAGE_RESUMED);
    }

    public TaskStatus taskStatus(UUID uuid) {
        BackgroundTask task = storage.get(uuid);
        return task.getStatus();
    }

    public Set<UUID> getAllTasks() {
        return storage.keySet();
    }

    public Set<UUID> getTasks(TaskStatus taskStatus) {
        return storage.entrySet().stream()
                .filter(x -> x.getValue().getStatus() == taskStatus)
                .map(x -> x.getKey())
                .collect(Collectors.toSet());
    }
}
