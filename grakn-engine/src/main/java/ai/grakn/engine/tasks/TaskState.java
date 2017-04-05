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

import ai.grakn.engine.TaskId;
import ai.grakn.engine.TaskStatus;
import ai.grakn.engine.util.EngineID;
import mjson.Json;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.time.Instant;

import static ai.grakn.engine.TaskStatus.CREATED;
import static ai.grakn.engine.TaskStatus.COMPLETED;
import static ai.grakn.engine.TaskStatus.FAILED;
import static ai.grakn.engine.TaskStatus.RUNNING;
import static ai.grakn.engine.TaskStatus.SCHEDULED;
import static ai.grakn.engine.TaskStatus.STOPPED;
import static java.time.Instant.now;
import static org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace;

/**
 * Internal task state model used to keep track of scheduled tasks.
 *
 * @author Denis Lobanov
 */
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
     * Schedule for when this task should execute
     */
    private TaskSchedule schedule;
    /**
     * Used to store any executing failures for the given task.
     */
    private String stackTrace;
    private String exception;
    /**
     * Used to store a task checkpoint allowing it to resume from the same point of execution as at the time of the checkpoint.
     */
    private TaskCheckpoint taskCheckpoint;
    /**
     * Configuration passed to the task on startup, can contain data/location of data for task to process, etc.
     */
    private Json configuration;

    public static TaskState of(Class<?> taskClass, String creator, TaskSchedule schedule, Json configuration) {
        return of(taskClass, creator, schedule, configuration, TaskId.generate());
    }

    public static TaskState of(
            Class<?> taskClass, String creator, TaskSchedule schedule, Json configuration, TaskId id) {
        return new TaskState(taskClass, creator, schedule, configuration, id);
    }

    private TaskState(Class<?> taskClass, String creator, TaskSchedule schedule, Json configuration, TaskId id) {
        this.status = CREATED;
        this.statusChangeTime = now();
        this.taskClassName = taskClass != null ? taskClass.getName() : null;
        this.creator = creator;
        this.schedule = schedule;
        this.configuration = configuration;
        this.taskId = id.getValue();
    }

    private TaskState(TaskState taskState) {
        this.taskId = taskState.taskId;
        this.status = taskState.status;
        this.statusChangeTime = taskState.statusChangeTime;
        this.taskClassName = taskState.taskClassName;
        this.creator = taskState.creator;
        this.engineID = taskState.engineID;
        this.schedule = taskState.schedule;
        this.stackTrace = taskState.stackTrace;
        this.exception = taskState.exception;
        this.taskCheckpoint = taskState.taskCheckpoint;
        this.configuration = taskState.configuration;
    }

    public TaskId getId() {
        return TaskId.of(taskId);
    }

    public TaskState markRunning(EngineID engineID){
        this.status = RUNNING;
        this.engineID = engineID;
        this.statusChangeTime = now();
        return this;
    }

    public TaskState markCompleted(){
        this.status = COMPLETED;
        this.statusChangeTime = now();

        // Clearing out any not relevant information
        if(!this.schedule.isRecurring()) {
            this.configuration = null;
        }
        this.taskCheckpoint = null;

        return this;
    }

    public TaskState markScheduled(){
        this.status = SCHEDULED;
        this.statusChangeTime = now();
        return this;
    }

    public TaskState markStopped(){
        this.status = STOPPED;
        this.statusChangeTime = now();

        // Clearing out any not relevant information
        if(!this.schedule.isRecurring()) {
            this.configuration = null;
        }

        return this;
    }

    public TaskState markFailed(Throwable exception){
        this.status = FAILED;
        this.exception = exception.getClass().getName();
        this.stackTrace = getFullStackTrace(exception);
        this.statusChangeTime = now();

        // We want to keep the configuration and checkpoint here
        // It's useful to debug failed states

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

    public String creator() {
        return creator;
    }

    public EngineID engineID() {
        return engineID;
    }

    public TaskSchedule schedule() {
        return schedule;
    }

    public TaskState schedule(TaskSchedule schedule){
        this.schedule = schedule;
        return this;
    }

    public String stackTrace() {
        return stackTrace;
    }

    public String exception() {
        return exception;
    }

    public TaskState checkpoint(TaskCheckpoint taskCheckpoint) {
        this.taskCheckpoint = taskCheckpoint;
        return this;
    }

    public TaskCheckpoint checkpoint() {
        return taskCheckpoint;
    }

    public @Nullable Json configuration() {
        return configuration;
    }

    public TaskState copy() {
        return new TaskState(this);
    }

    @Override
    public String toString() {
        return "TaskState(" + taskClass().getSimpleName() + ", \"" + getId() + "\").status(" + status() + ")";
    }
}

