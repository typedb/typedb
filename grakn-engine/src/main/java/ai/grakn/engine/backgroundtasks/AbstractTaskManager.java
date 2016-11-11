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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;

public abstract class AbstractTaskManager implements TaskManager {
    private final Logger LOG = LoggerFactory.getLogger(AbstractTaskManager.class);
    private static String STATUS_MESSAGE_SCHEDULED = "Task scheduled.";

    public UUID scheduleTask(BackgroundTask task, long delay) {
        TaskState state = new TaskState(task.getClass().getName())
                .setRecurring(false)
                .setDelay(delay)
                .setStatus(TaskStatus.SCHEDULED)
                .setStatusChangeMessage(STATUS_MESSAGE_SCHEDULED)
                .setStatusChangedBy(InMemoryTaskManager.class.getName())
                .setQueuedTime(new Date());

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
        TaskState state = getTaskState(uuid);
        state.setStatus(TaskStatus.STOPPED)
             .setStatusChangeMessage(message)
             .setStatusChangedBy(requesterName);
        uuid = updateTaskState(uuid, state);

        ScheduledFuture<BackgroundTask> future = getTaskExecutionStatus(uuid);
        try {
            if (!future.isDone())
                instantiateTask(state.getName()).stop();

            future.cancel(true);
        } catch(ClassNotFoundException | IllegalAccessException | InstantiationException e) {
            LOG.error("Could not run .stop() on task "+state.getName()+" id: "+uuid.toString()+" the error was: "+e.getMessage());
        }

        return this;
    }

    public TaskManager pauseTask(UUID uuid, String requesterName, String message) {
        TaskState state = getTaskState(uuid);
        TaskStatus status = state.getStatus();

        if(status == TaskStatus.SCHEDULED || status == TaskStatus.RUNNING) {
            // Mark task state as paused.
            state.setStatus(TaskStatus.PAUSED)
                    .setStatusChangeMessage(message)
                    .setStatusChangedBy(requesterName);

            ScheduledFuture<BackgroundTask> future = getTaskExecutionStatus(uuid);
            try {
                // Call tasks .pause() method if it's still running.
                if (status == TaskStatus.RUNNING && (!future.isDone()))
                    state.setPauseState(future.get().pause());

                future.cancel(true);
            } catch (InterruptedException | ExecutionException e) {
                LOG.error(e.getMessage());
            }

            updateTaskState(uuid, state);
        } else {
            LOG.warn("Ignoring pause request from: "+Thread.currentThread().getStackTrace()[2].toString()+
                     " for task "+uuid.toString()+
                     " because its status is "+state.getStatus());
        }

        return this;
    }

    public TaskManager resumeTask(UUID uuid, String requesterName, String message) {
        TaskState state = getTaskState(uuid);
        if(state.getStatus() != TaskStatus.PAUSED) {
           LOG.warn("Ignoring resume request from: "+Thread.currentThread().getStackTrace()[2].toString()+
                     " for task "+uuid.toString()+
                     " because its status is "+state.getStatus());
            return this;
        }

        state.setStatus(TaskStatus.RUNNING)
            .setStatusChangeMessage(message)
            .setStatusChangedBy(requesterName);
        uuid = updateTaskState(uuid, state);

        try {
            BackgroundTask task = instantiateTask(state.getName());
            task.resume(state.getPauseState());

            if(state.getRecurring())
                executeRecurring(uuid, task, state.getDelay(), state.getInterval());
            else
                executeSingle(uuid, task, state.getDelay());

        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            LOG.error("Unable to resume task "+uuid.toString()+" the error was: "+e.getMessage());
        }

        return this;
    }

    public TaskManager restartTask(UUID uuid, String requesterName, String message) {
        TaskState state = getTaskState(uuid);
        TaskStatus status = state.getStatus();
        if(status == TaskStatus.SCHEDULED || status == TaskStatus.CREATED || status == TaskStatus.RUNNING) {
            LOG.warn(Thread.currentThread().getStackTrace()[2].toString()+" tried to call restartTask() on a "+status.toString()+" task. IGNORING.");
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
            updateTaskState(uuid, state);
        } catch(ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            LOG.error("Unable to start task "+uuid.toString()+" the error was: "+e.getMessage());
        }

        return this;
    }

    protected abstract UUID saveNewState(TaskState state);
    protected abstract UUID updateTaskState(UUID uuid, TaskState state);

    protected abstract ScheduledFuture<BackgroundTask> getTaskExecutionStatus(UUID uuid);

    protected abstract void executeSingle(UUID uuid, BackgroundTask task, long delay);
    protected abstract void executeRecurring(UUID uuid, BackgroundTask task, long delay, long interval);

    protected abstract BackgroundTask instantiateTask(String className) throws ClassNotFoundException, InstantiationException, IllegalAccessException;
}
