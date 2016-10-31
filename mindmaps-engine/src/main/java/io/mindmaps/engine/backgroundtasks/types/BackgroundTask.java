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

package io.mindmaps.engine.backgroundtasks.types;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * The main interface all BackgroundTasks must implement to be executable.
 */
public class BackgroundTask {
    private TaskStatus status;
    private String name;
    private Date queuedTime;
    private Date statusChangeTime;
    private String executingHostname;
    private String creator;
    private String statusChangedBy;
    private String statusChangeMessage;
    private Map<String, Object> customConfig;

    /**
     * Called to execute task.
     */
    public void start() {
        System.err.println("In: " + Thread.currentThread().getStackTrace()[1].toString() + " .start() should be overridden!");
        status = TaskStatus.COMPLETED;
    }

    /**
     * Called to stop task.
     */
    public void stop() {
        System.err.println("In: " + Thread.currentThread().getStackTrace()[1].toString() + " .stop() should be overridden!");
        status = TaskStatus.STOPPED;
    }

    /**
     * Print statistics to STDOUT.
     */
    public void dumpStats() {
        System.err.println("In: " + Thread.currentThread().getStackTrace()[1].toString() + " .dumpStats() should be overridden!");
    }


    public BackgroundTask(String name) {
        customConfig = new HashMap<>();
        status = TaskStatus.CREATED;
        this.name = name;
    }

    public BackgroundTask setStatus(TaskStatus status) {
        this.status = status;
        statusChangeTime = new Date();
        return this;
    }

    public TaskStatus getStatus() {
        return status;
    }

    public String getName() {
        return this.name;
    }

    public Date getStatusChangeTime() {
        return statusChangeTime;
    }

    public BackgroundTask setQueuedTime(Date queuedTime) {
        this.queuedTime = queuedTime;
        return this;
    }

    public Date getQueuedTime() {
        return queuedTime;
    }

    public BackgroundTask setExecutingHostname(String hostname) {
        executingHostname = hostname;
        return this;
    }

    public String getExecutingHostname() {
        return executingHostname;
    }

    public BackgroundTask setCreator(String creator) {
        this.creator = creator;
        return this;
    }

    public String getCreator() {
        return creator;
    }

    public BackgroundTask setStatusChangedBy(String statusChangedBy) {
        this.statusChangedBy = statusChangedBy;
        return this;
    }

    public String getStatusChangedBy() {
        return statusChangedBy;
    }

    public BackgroundTask setStatuChangeMessage(String message) {
        statusChangeMessage = message;
        return this;
    }

    public String getStatusChangeMessage() {
        return statusChangeMessage;
    }

    public BackgroundTask setConfigItem(String name, Object value) {
        customConfig.put(name, value);
        return this;
    }

    public Object getConfigItem(String name) {
        return customConfig.get(name);
    }

    public BackgroundTask setConfig(Map<String, Object> config) {
        customConfig = config;
        return this;
    }

    public Map<String, Object> getConfig() {
        return customConfig;
    }
}
