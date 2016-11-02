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

import java.util.Set;
import java.util.UUID;

public interface TaskManager {
    /**
     * Schedule a single shot/one off BackgroundTask to run after a @delay in milliseconds.
     * @param task Any object implementing the BackgroundTask interface that is to be scheduled for later execution.
     * @param delay Delay after which the @task object should be executed, may be null.
     * @return Assigned UUID of task scheduled for later execution.
     */
    UUID scheduleTask(BackgroundTask task, long delay);

    /**
     * Schedule a task for recurring execution at every @period interval and after an initial @delay.
     * @param task Any object implementing the BackgroundTask interface that is to be scheduled for later execution.
     * @param delay Long delay after which the @task object should be executed, may be null.
     * @param period Long interval between subsequent calls to @task.start().
     * @return Assigned UUID of task scheduled for later execution.
     */
    UUID scheduleRecurringTask(BackgroundTask task, long delay, long period);

    /**
     * Stop a Scheduled, Paused or Running task. Task's .stop() method will be called to perform any cleanup and the
     * task is killed afterwards.
     * @param uuid UUID of task to stop.
     * @param requesterName Optional String to denote who requested this call; used for status reporting and may be null.
     * @param message Optional String denoting the reason for stopping the task; used for status reporting and may be null.
     * @return Instance of the class implementing TaskManager.
     */
    TaskManager stopTask(UUID uuid, String requesterName, String message);

    /**
     * Pause execution of a currently Running task.
     * @param uuid UUID of task to stop.
     * @param requesterName Optional String to denote who requested this call; used for status reporting and may be null.
     * @param message Optional String denoting the reason for stopping the task; used for status reporting and may be null.
     * @return Instance of the class implementing TaskManager.
     */
    TaskManager pauseTask(UUID uuid, String requesterName, String message);

    /**
     * Resume a previously Paused task to continue execution from where it left off. It is not guaranteed that the process
     * will not be garbage collected whilst paused.
     * Note:
     *  It is the responsibility of the Task's .pause() method to provide a consise state map that would allow its .resume()
     *  method to allow execution from where it was last left off.
     * @param uuid UUID of task to stop.
     * @param requesterName Optional String to denote who requested this call; used for status reporting and may be null.
     * @param message Optional String denoting the reason for stopping the task; used for status reporting and may be null.
     * @return Instance of the class implementing TaskManager.
     */
    TaskManager resumeTask(UUID uuid, String requesterName, String message);

    /**
     * Restart a previously Paused, Stopped or Dead task; this call causes the Tasks .restart() method to be called to
     * perform any cleanup necessary before it is scheduled for re-execution with the same delay/interval parameters as
     * used to originally schedule said task. Thus a recurring task will be re-scheduled as a recurring task, and a Stopped
     * or Dead on off task will only run once.
     * @param uuid UUID of task to stop.
     * @param requesterName Optional String to denote who requested this call; used for status reporting and may be null.
     * @param message Optional String denoting the reason for stopping the task; used for status reporting and may be null.
     * @return Instance of the class implementing TaskManager.
     */
    TaskManager restartTask(UUID uuid, String requesterName, String message);

    /**
     * Return the full TaskState object for a given task, containing full task metadata including the status change messages
     * and reqesterNames as provided in the stopTask/pauseTask/resumeTask and restartTask methods.
     * @param uuid UUID of task to stop.
     * @return TaskState object. See @TaskState.
     */
    TaskState getTaskState(UUID uuid);

    /**
     * Returns a Set of all tasks in the system - this includes Completed, Running, Dead, etc.
     * @return Set<> of task UUID's
     */
    Set<UUID> getAllTasks();

    /**
     * Return a Set of all tasks with a matching @TaskStatus.
     * Example:
     *  // Return all tasks which failed to complete execution.
     *  Set<UUID> failedTasks = myTaskManager.getTasks(TaskStatus.DEAD);
     *
     * @param taskStatus See TaskStatus enum.
     * @return Set<> of task UUID's matching the given @taskStatus.
     */
    Set<UUID> getTasks(TaskStatus taskStatus);
}
