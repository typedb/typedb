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

package ai.grakn.engine.backgroundtasks;

import org.json.JSONObject;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;

import static ai.grakn.engine.util.SystemOntologyElements.CREATED_BY;
import static ai.grakn.engine.util.SystemOntologyElements.TASK_CLASS_NAME;
import static ai.grakn.engine.util.SystemOntologyElements.TASK_ID;
import static ai.grakn.engine.util.SystemOntologyElements.ENGINE_ID;
import static ai.grakn.engine.util.SystemOntologyElements.RECURRING;
import static ai.grakn.engine.util.SystemOntologyElements.RECUR_INTERVAL;
import static ai.grakn.engine.util.SystemOntologyElements.RUN_AT;
import static ai.grakn.engine.util.SystemOntologyElements.STACK_TRACE;
import static ai.grakn.engine.util.SystemOntologyElements.STATUS;
import static ai.grakn.engine.util.SystemOntologyElements.STATUS_CHANGE_BY;
import static ai.grakn.engine.util.SystemOntologyElements.STATUS_CHANGE_TIME;
import static ai.grakn.engine.util.SystemOntologyElements.TASK_CHECKPOINT;
import static ai.grakn.engine.util.SystemOntologyElements.TASK_CONFIGURATION;
import static ai.grakn.engine.util.SystemOntologyElements.TASK_EXCEPTION;

/**
 * Internal task state model used to keep track of scheduled tasks.
 *
 * @author Denis Lobanov
 */
public class TaskState implements Cloneable {

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
    private JSONObject configuration;

    public TaskState(String taskClassName) {
        this.status = TaskStatus.CREATED;
        this.statusChangeTime = Instant.now();
        this.taskClassName = taskClassName;
        this.taskId = UUID.randomUUID().toString();
    }

    public TaskState(String taskClassName, String id, TaskStatus status){
        this.taskId = id;
        this.taskClassName = taskClassName;
        this.status = status;
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

    public String taskClassName() {
        return this.taskClassName;
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

    public TaskState configuration(JSONObject configuration) {
        this.configuration = configuration;
        return this;
    }

    public JSONObject configuration() {
        return configuration;
    }

    public TaskState clone() throws CloneNotSupportedException {
        TaskState state = (TaskState) super.clone();

        state.status(status)
                .statusChangeTime(statusChangeTime)
                .statusChangedBy(statusChangedBy)
                .creator(creator)
                .engineID(engineID)
                .runAt(runAt)
                .isRecurring(recurring)
                .interval(interval)
                .stackTrace(stackTrace)
                .exception(exception)
                .checkpoint(taskCheckpoint)
                .configuration(new JSONObject(configuration.toString()));

        return state;
    }

    public byte[] serialise() {
        JSONObject object = new JSONObject();

        object.put(TASK_ID.getValue(), taskId);
        object.put(STATUS.getValue(), status.name());
        object.put(STATUS_CHANGE_TIME.getValue(), statusChangeTime.toEpochMilli());
        object.put(STATUS_CHANGE_BY.getValue(), statusChangedBy);
        object.put(CREATED_BY.getValue(), creator);
        object.put(ENGINE_ID.getValue(), engineID);
        object.put(RUN_AT.getValue(), runAt.toEpochMilli());
        object.put(RECURRING.getValue(), recurring);
        object.put(RECUR_INTERVAL.getValue(), interval);
        object.put(STACK_TRACE.getValue(), stackTrace);
        object.put(TASK_EXCEPTION.getValue(), exception);
        object.put(TASK_CHECKPOINT.getValue(), taskCheckpoint);
        object.put(TASK_CONFIGURATION.getValue(), configuration != null ? configuration.toString() : new JSONObject().toString());
        object.put(TASK_CLASS_NAME.getValue(), taskClassName);

        return object.toString().getBytes(StandardCharsets.UTF_8);
    }

    public static TaskState deserialise(byte[] b) {
        JSONObject serialised = new JSONObject(new String(b, StandardCharsets.UTF_8));

        String taskId = serialised.getString(TASK_ID.getValue());
        String taskClassName = serialised.getString(TASK_CLASS_NAME.getValue());
        TaskStatus status = TaskStatus.valueOf(serialised.getString(STATUS.getValue()));

        TaskState state = new TaskState(taskClassName, taskId, status);

        if(serialised.has(STATUS_CHANGE_TIME.getValue())) { state.statusChangeTime(Instant.ofEpochMilli(serialised.getLong(STATUS_CHANGE_TIME.getValue())));}
        if(serialised.has(STATUS_CHANGE_BY.getValue())) { state.statusChangedBy(serialised.getString(STATUS_CHANGE_BY.getValue()));}
        if(serialised.has(CREATED_BY.getValue())) { state.creator(serialised.getString(CREATED_BY.getValue()));}
        if(serialised.has(ENGINE_ID.getValue())) { state.engineID(serialised.getString(ENGINE_ID.getValue()));}
        if(serialised.has(RUN_AT.getValue())) { state.runAt(Instant.ofEpochMilli(serialised.getLong(RUN_AT.getValue())));}
        if(serialised.has(RECURRING.getValue())) { state.isRecurring(serialised.getBoolean(RECURRING.getValue()));}
        if(serialised.has(RECUR_INTERVAL.getValue())) { state.interval(serialised.getLong(RECUR_INTERVAL.getValue()));}
        if(serialised.has(STACK_TRACE.getValue())) { state.stackTrace(serialised.getString(STACK_TRACE.getValue()));}
        if(serialised.has(TASK_EXCEPTION.getValue())) { state.exception(serialised.getString(TASK_EXCEPTION.getValue()));}
        if(serialised.has(TASK_CHECKPOINT.getValue())) { state.checkpoint(serialised.getString(TASK_CHECKPOINT.getValue()));}
        if(serialised.has(TASK_CONFIGURATION.getValue())) { state.configuration(new JSONObject(serialised.getString(TASK_CONFIGURATION.getValue())));}

        return state;
    }
}

