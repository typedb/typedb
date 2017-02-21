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

package ai.grakn.engine.tasks;

import ai.grakn.engine.TaskStatus;
import mjson.Json;
import org.apache.commons.lang.SerializationUtils;

import java.io.Serializable;
import java.time.Instant;
import java.util.Base64;
import java.util.UUID;

/**
 * Internal task state model used to keep track of scheduled tasks.
 *
 * @author Denis Lobanov
 */
public class TaskState implements Serializable {

    /**
     * Id of this task.
     */
    private final String taskId;
    /**
     * Task status, @see TaskStatus.
     */
    private TaskStatus status;
    /**
     * Time when task status was last updated.
     */
    private Instant statusChangeTime;
    /**
     * String identifying who last updated task status.
     */
    private String statusChangedBy;
    /**
     * Name of Class implementing the BackgroundTask interface that should be executed when task is run.
     */
    private final String taskClassName;
    /**
     * String identifying who created this task.
     */
    private String creator;
    /**
     * String identifying which engine instance is executing this task, set when task is scheduled.
     */
    private String engineID;
    /**
     * When this task should be executed.
     */
    private Instant runAt;
    /**
     * Should this task be run again after it has finished executing successfully.
     */
    private Boolean recurring;
    /**
     * If a task is marked as recurring, this represents the time delay between the next executing of this task.
     */
    private long interval;
    /**
     * Used to store any executing failures for the given task.
     */
    private String stackTrace;
    private String exception;
    /**
     * Used to store a task checkpoint allowing it to resume from the same point of execution as at the time of the checkpoint.
     */
    private String taskCheckpoint;
    /**
     * Configuration passed to the task on startup, can contain data/location of data for task to process, etc.
     */
    private Json configuration;

    public TaskState(Class<?> taskClass) {
        this(taskClass, UUID.randomUUID().toString());
    }

    public TaskState(Class<?> taskClass, String id) {
        this.status = TaskStatus.CREATED;
        this.statusChangeTime = Instant.now();
        this.taskClassName = taskClass.getName();
        this.taskId = id;

        //TODO Defaults until we refactor TaskState class
        this.runAt = Instant.now();
        this.recurring = false;
        this.interval = 0;
    }

    private TaskState(TaskState taskState) {
        this.taskId = taskState.taskId;
        this.status = taskState.status;
        this.statusChangeTime = taskState.statusChangeTime;
        this.statusChangedBy = taskState.statusChangedBy;
        this.taskClassName = taskState.taskClassName;
        this.creator = taskState.creator;
        this.engineID = taskState.engineID;
        this.runAt = taskState.runAt;
        this.recurring = taskState.recurring;
        this.interval = taskState.interval;
        this.stackTrace = taskState.stackTrace;
        this.exception = taskState.exception;
        this.taskCheckpoint = taskState.taskCheckpoint;
        this.configuration = taskState.configuration;
    }

    public String getId() {
        return taskId;
    }

    public TaskState status(TaskStatus status) {
        this.status = status;
        this.statusChangeTime = Instant.now();
        return this;
    }

    public TaskStatus status() {
        return status;
    }

    public TaskState statusChangeTime(Instant statusChangeTime) {
        this.statusChangeTime = statusChangeTime;
        return this;
    }

    public Instant statusChangeTime() {
        return statusChangeTime;
    }

    public TaskState statusChangedBy(String statusChangedBy) {
        this.statusChangedBy = statusChangedBy;
        return this;
    }

    public String statusChangedBy() {
        return statusChangedBy;
    }

    public Class<? extends BackgroundTask> taskClass() {
        try {
            return (Class<? extends BackgroundTask>) Class.forName(taskClassName);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public TaskState creator(String creator) {
        this.creator = creator;
        return this;
    }

    public String creator() {
        return creator;
    }

    public TaskState engineID(String engineID) {
        this.engineID = engineID;
        return this;
    }

    public String engineID() {
        return engineID;
    }

    public TaskState runAt(Instant runAt) {
        this.runAt = runAt;
        return this;
    }

    public Instant runAt() {
        return runAt;
    }

    public TaskState isRecurring(Boolean recurring) {
        this.recurring = recurring;
        return this;
    }

    public Boolean isRecurring() {
        return recurring != null && recurring;
    }

    public TaskState interval(long interval) {
        this.interval = interval;
        return this;
    }

    public long interval() {
        return interval;
    }

    public TaskState stackTrace(String stackTrace) {
        this.stackTrace = stackTrace;
        return this;
    }

    public String stackTrace() {
        return stackTrace;
    }

    public TaskState exception(String exceptionMessage) {
        this.exception = exceptionMessage;
        return this;
    }

    public String exception() {
        return exception;
    }

    public TaskState checkpoint(String taskCheckpoint) {
        this.taskCheckpoint = taskCheckpoint;
        return this;
    }

    public String checkpoint() {
        return taskCheckpoint;
    }

    public TaskState configuration(Json configuration) {
        this.configuration = configuration;
        return this;
    }

    public Json configuration() {
        return configuration;
    }

    public static String serialize(TaskState task){
        return Base64.getMimeEncoder().encodeToString(SerializationUtils.serialize(task));
    }

    public static TaskState deserialize(String task){
        return (TaskState) SerializationUtils.deserialize(Base64.getMimeDecoder().decode(task));
    }

    public TaskState copy() {
        return new TaskState(this);
    }

    @Override
    public String toString() {
        return "TaskState(" + taskClass().getSimpleName() + ", \"" + getId() + "\").status(" + status() + ")";
    }
}

