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

package ai.grakn.engine.backgroundtasks;

import java.util.Date;
import java.util.Map;

/**
 * Internal task state model used to keep track of scheduled tasks.
 */
public class TaskState {
    private TaskStatus status;
    private Date statusChangeTime;
    private String statusChangedBy;
    private String statusChangeMessage;

    private String name;
    private Date queuedTime;
    private String creator;

    private String executingHostname;
    private long delay;
    private Boolean recurring;
    private long interval;
    private Map<String, Object> pauseState;

    public TaskState(String name) {
        status = TaskStatus.CREATED;
        this.name = name;
    }

    public TaskState setStatus(TaskStatus status) {
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

    public TaskState setQueuedTime(Date queuedTime) {
        this.queuedTime = queuedTime;
        return this;
    }

    public Date getQueuedTime() {
        return queuedTime;
    }

    public TaskState setExecutingHostname(String hostname) {
        executingHostname = hostname;
        return this;
    }

    public String getExecutingHostname() {
        return executingHostname;
    }

    public TaskState setCreator(String creator) {
        this.creator = creator;
        return this;
    }

    public String getCreator() {
        return creator;
    }

    public TaskState setStatusChangedBy(String statusChangedBy) {
        this.statusChangedBy = statusChangedBy;
        return this;
    }

    public String getStatusChangedBy() {
        return statusChangedBy;
    }

    public TaskState setStatusChangeMessage(String message) {
        statusChangeMessage = message;
        return this;
    }

    public String getStatusChangeMessage() {
        return statusChangeMessage;
    }

    public long getDelay() {
        return delay;
    }

    public TaskState setDelay(long delay) {
        this.delay = delay;
        return this;
    }

    public Boolean getRecurring() {
        return recurring;
    }

    public TaskState setRecurring(Boolean recurring) {
        this.recurring = recurring;
        return this;
    }

    public long getInterval() {
        return interval;
    }

    public TaskState setInterval(long interval) {
        this.interval = interval;
        return this;
    }

    public Map<String, Object> getPauseState() {
        return pauseState;
    }

    public TaskState setPauseState(Map<String, Object> pauseState) {
        this.pauseState = pauseState;
        return this;
    }
}
