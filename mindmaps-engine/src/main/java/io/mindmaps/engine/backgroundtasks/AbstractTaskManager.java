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

import io.mindmaps.exception.MindmapsValidationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;

import static io.mindmaps.engine.backgroundtasks.TaskStatus.*;

public abstract class AbstractTaskManager implements TaskManager {
    private final Logger LOG = LoggerFactory.getLogger(AbstractTaskManager.class);
    private static String STATUS_MESSAGE_SCHEDULED = "Task scheduled.";

    public String scheduleTask(BackgroundTask task, long delay) {
        TaskState state = new TaskState(task.getClass().getName())
                .setRecurring(false)
                .setDelay(delay)
                .setStatus(SCHEDULED)
                .setStatusChangeMessage(STATUS_MESSAGE_SCHEDULED)
                .setStatusChangedBy(InMemoryTaskManager.class.getName());

        try {
            String id = saveNewState(state);
            executeSingle(id, task, delay);
            return id;
        } catch(MindmapsValidationException e) {
            LOG.error("Could not schedule task "+state.getName()+" the error was: "+e.getMessage());
        }

        return null;
    }

    public String scheduleRecurringTask(BackgroundTask task, long delay, long interval) {
        TaskState state = new TaskState(task.getClass().getName())
                .setRecurring(true)
                .setDelay(delay)
                .setInterval(interval);

        try {
            String id = saveNewState(state);
            executeRecurring(id, task, delay, interval);
            return id;
        } catch(MindmapsValidationException e) {
            LOG.error("Could not schedule task "+state.getName()+" the error was: "+e.getMessage());
        }

        return null;
    }

    public TaskManager stopTask(String id, String requesterName, String message) {
        TaskState state = getTaskState(id);

        try {
            ScheduledFuture<BackgroundTask> future = getTaskExecutionStatus(id);

            if (!future.isDone())
                instantiateTask(state.getName()).stop();

            future.cancel(true);

            // Update state
            state = getTaskState(id);
            state.setStatus(STOPPED)
                    .setStatusChangeMessage(message)
                    .setStatusChangedBy(requesterName);
            updateTaskState(id, state);

        } catch(ClassNotFoundException | IllegalAccessException | InstantiationException e) {
            LOG.error("Could not run .stop() on task "+state.getName()+" id: "+id+" the error was: "+e.getMessage());
        } catch(MindmapsValidationException e) {
            LOG.error("Could not update state for task "+state.getName()+" id: "+id+" the error was: "+e.getMessage());
        }

        return this;
    }

    public TaskManager pauseTask(String id, String requesterName, String message) {
        TaskState state = getTaskState(id);
        TaskStatus status = state.getStatus();

        if(status == SCHEDULED || status == RUNNING) {
            // Mark task state as paused.
            state.setStatus(PAUSED)
                 .setStatusChangeMessage(message)
                 .setStatusChangedBy(requesterName);

            ScheduledFuture<BackgroundTask> future = getTaskExecutionStatus(id);
            try {
                // Call tasks .pause() method if it's still running.
                if (status == RUNNING && (!future.isDone()))
                    state.setPauseState(future.get().pause());

                future.cancel(true);
                updateTaskState(id, state);

            } catch (InterruptedException | ExecutionException e) {
                LOG.error("Could not pause task "+state.getName()+" id: "+id+" the error was: "+e.getMessage());
            } catch (MindmapsValidationException e) {
                LOG.error("Could not update state for task"+state.getName()+" id: "+id+" the error was: "+e.getMessage());
            }

        } else {
            LOG.warn("Ignoring pause request from: "+Thread.currentThread().getStackTrace()[2].toString()+
                     " for task "+id+
                     " because its status is "+state.getStatus());
        }

        return this;
    }

    public TaskManager resumeTask(String id, String requesterName, String message) {
        TaskState state = getTaskState(id);
        if(state.getStatus() != PAUSED) {
           LOG.warn("Ignoring resume request from: "+Thread.currentThread().getStackTrace()[2].toString()+
                     " for task "+id+
                     " because its status is "+state.getStatus());
            return this;
        }

        state.setStatus(SCHEDULED)
             .setStatusChangeMessage(message)
             .setStatusChangedBy(requesterName);

        try {
            id = updateTaskState(id, state);

            BackgroundTask task = instantiateTask(state.getName());
            task.resume(state.getPauseState());

            if(state.getRecurring())
                executeRecurring(id, task, state.getDelay(), state.getInterval());
            else
                executeSingle(id, task, state.getDelay());

        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            LOG.error("Unable to resume task "+id+" the error was: "+e.getMessage());
        } catch (MindmapsValidationException e) {
            LOG.error("Unable to update state for task "+state.getName()+" id: "+id+" the error was: "+e.getMessage());
        }

        return this;
    }

    public TaskManager restartTask(String id, String requesterName, String message) {
        TaskState state = getTaskState(id);
        TaskStatus status = state.getStatus();
        if(status == SCHEDULED || status == CREATED || status == RUNNING) {
            LOG.warn(Thread.currentThread().getStackTrace()[2].toString()+" tried to call restartTask() on a "+status.toString()+" task. IGNORING.");
            return this;
        }

        state.setStatus(SCHEDULED)
             .setStatusChangeMessage(message)
             .setStatusChangedBy(requesterName);

        try {
            // Update status message
            updateTaskState(id, state);

            // Clean up etc
            BackgroundTask task = instantiateTask(state.getName());
            task.restart();

            // Start it up again
            if(state.getRecurring())
                executeRecurring(id, task, state.getDelay(), state.getInterval());
            else
                executeSingle(id, task, state.getDelay());

        } catch(ClassNotFoundException | InstantiationException | IllegalAccessException | MindmapsValidationException e) {
            LOG.error("Unable to start task "+id+" the error was: "+e.getMessage());
        }

        return this;
    }

    protected abstract String saveNewState(TaskState state) throws MindmapsValidationException;
    protected abstract String updateTaskState(String id, TaskState state) throws MindmapsValidationException;

    protected abstract ScheduledFuture<BackgroundTask> getTaskExecutionStatus(String id);

    protected abstract void executeSingle(String id, BackgroundTask task, long delay);
    protected abstract void executeRecurring(String id, BackgroundTask task, long delay, long interval);


    private BackgroundTask instantiateTask(String className) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        Class<?> c = Class.forName(className);
        return (BackgroundTask) c.newInstance();
    }

}
