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

public interface TaskScheduler {
    UUID scheduleTask(BackgroundTask task, long delay);
    UUID scheduleRecurringTask(BackgroundTask task, long delay, long period);

    TaskScheduler stopTask(UUID uuid);
    TaskScheduler stopTask(UUID uuid, String requesterName, String message);

    TaskScheduler pauseTask(UUID uuid);
    TaskScheduler pauseTask(UUID uuid, String requesterName, String message);

    TaskScheduler resumeTask(UUID uuid);
    TaskScheduler resumeTask(UUID uuid, String requesterName, String message);

    TaskScheduler restartTask(UUID uuid);
    TaskScheduler restartTask(UUID uuid, String requesterName, String message);

    TaskState getTaskState(UUID uuid);

    Set<UUID> getAllTasks();
    Set<UUID> getTasks(TaskStatus taskStatus);
}
