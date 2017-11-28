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

package ai.grakn.engine.tasks.manager;

import ai.grakn.engine.TaskId;
import ai.grakn.engine.TaskStatus;
import ai.grakn.engine.tasks.BackgroundTask;
import ai.grakn.engine.util.EngineID;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.time.Instant;

import static ai.grakn.engine.TaskStatus.CREATED;
import static ai.grakn.engine.TaskStatus.FAILED;
import static java.time.Instant.now;

/**
 * Internal task state model used to keep track of scheduled tasks.
 * TODO: make immutable, fix json serialisation/deserialisation
 *
 * @author Denis Lobanov
 */
@JsonInclude(Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class TaskState implements Serializable {

    private static final long serialVersionUID = -7301340972479426653L;

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
     * Name of Class implementing the BackgroundTask interface that should be executed when task is run.
     */
    private final String taskClassName;
    /**
     * String identifying who created this task.
     */
    private final String creator;
    /**
     * String identifying which engine instance is executing this task, set when task is scheduled.
     */
    private EngineID engineID;
    /**
     * Used to store any executing failures for the given task.
     */
    private String stackTrace;
    private String exception;
    /**
     * Used to store a task checkpoint allowing it to resume from the same point of execution as at the time of the checkpoint.
     */
    private TaskCheckpoint taskCheckpoint;

    public static TaskState of(Class<?> taskClass, String creator) {
        return new TaskState(taskClass, creator, TaskId.generate());
    }

    public static TaskState of(TaskId id) {
        return new TaskState(null, null, id);
    }

    public static TaskState of(TaskId id, TaskStatus taskStatus) {
        return new TaskState(null, null, id, taskStatus);
    }

    @JsonCreator
    public TaskState(@JsonProperty("taskClass") Class<?> taskClass,
            @JsonProperty("creator") String creator,
            @JsonProperty("id") TaskId id,
            @JsonProperty("status") TaskStatus status) {
        this.status = status;
        this.statusChangeTime = now();
        this.taskClassName = taskClass != null ? taskClass.getName() : null;
        this.creator = creator;
        this.taskId = id.value();
    }

    public TaskState(Class<?> taskClass,
            String creator,
            TaskId id) {
        this(taskClass, creator, id, CREATED);
    }

    private TaskState(TaskState taskState) {
        this.taskId = taskState.taskId;
        this.status = taskState.status;
        this.statusChangeTime = taskState.statusChangeTime;
        this.taskClassName = taskState.taskClassName;
        this.creator = taskState.creator;
        this.engineID = taskState.engineID;
        this.stackTrace = taskState.stackTrace;
        this.exception = taskState.exception;
        this.taskCheckpoint = taskState.taskCheckpoint;
    }

    @JsonProperty("id")
    public TaskId getId() {
        return TaskId.of(taskId);
    }


    public TaskState markFailed(String reason){
        this.status = FAILED;
        this.exception = reason;
        this.stackTrace = "No Stack Trace Provided";
        this.statusChangeTime = now();
        return this;
    }

    public TaskStatus status() {
        return status;
    }

    public Instant statusChangeTime() {
        return statusChangeTime;
    }

    public Class<? extends BackgroundTask> taskClass() {
        try {
            return (Class<? extends BackgroundTask>) Class.forName(taskClassName);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public String stackTrace() {
        return stackTrace;
    }

    public String exception() {
        return exception;
    }

    public TaskState copy() {
        return new TaskState(this);
    }

    @JsonProperty("serialVersionUID")
    public static long getSerialVersionUID() {
        return serialVersionUID;
    }

    @JsonProperty("status")
    public TaskStatus getStatus() {
        return status;
    }

    @JsonProperty("taskClassName")
    public String getTaskClassName() {
        return taskClassName;
    }

    @JsonProperty("engineId")
    public EngineID getEngineID() {
        return engineID;
    }

    @JsonProperty("taskCheckpoint")
    public TaskCheckpoint getTaskCheckpoint() {
        return taskCheckpoint;
    }

    @Override
    public String toString() {
        return "TaskState(" + taskClass().getSimpleName() + ", \"" + getId() + "\").status(" + status() + ")";
    }
}

