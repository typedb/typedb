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

package ai.grakn.test.engine.tasks;

import ai.grakn.engine.TaskStatus;
import ai.grakn.engine.tasks.TaskId;
import ai.grakn.engine.tasks.TaskSchedule;
import ai.grakn.engine.tasks.TaskState;
import ai.grakn.engine.tasks.TaskStateStorage;
import ai.grakn.engine.util.EngineID;
import com.google.common.collect.ConcurrentHashMultiset;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Maps;
import com.google.common.collect.Multiset;
import com.google.common.collect.Sets;
import mjson.Json;

import java.io.IOException;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static ai.grakn.engine.tasks.TaskSchedule.now;
import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.Stream.generate;
import static mjson.Json.object;

/**
 * Class holding useful methods for use throughout background task tests
 */
public class BackgroundTaskTestUtils {

    private static final ConcurrentHashMultiset<TaskId> COMPLETED_TASKS = ConcurrentHashMultiset.create();

    static void addCompletedTask(TaskId taskId) {
        COMPLETED_TASKS.add(taskId);
    }

    public static ImmutableMultiset<TaskId> completedTasks() {
        return ImmutableMultiset.copyOf(COMPLETED_TASKS);
    }

    public static void clearCompletedTasks() {
        COMPLETED_TASKS.clear();
    }

    public static Set<TaskState> createTasks(int n, TaskStatus status) {
        return createTasks(n, status, EngineID.me());
    }

    public static Set<TaskState> createTasks(int n, TaskStatus status, EngineID engineID) {
        return generate(() -> createTask(status, now(), object(), engineID)).limit(n).collect(toSet());
    }

    public static TaskState createTask(TaskStatus status, TaskSchedule schedule, Json configuration) {
        return createTask(status, schedule, configuration, EngineID.me());
    }

    public static TaskState createTask(TaskStatus status, TaskSchedule schedule, Json configuration, EngineID engineID) {
        TaskState taskState = TaskState.of(ShortExecutionTestTask.class, BackgroundTaskTestUtils.class.getName(), schedule, configuration);

        switch (status) {
            case RUNNING:
                taskState.markRunning(engineID);
                break;
            case COMPLETED:
                taskState.markCompleted();
                break;
            case FAILED:
                taskState.markFailed(new IOException());
                break;
            case STOPPED:
                taskState.markStopped();
                break;
            case SCHEDULED:
                taskState.markScheduled();
                break;
        }

        configuration.set("id", taskState.getId().getValue());
        return taskState;
    }
    
    public static void waitForStatus(TaskStateStorage storage, Collection<TaskState> tasks, TaskStatus... status) {
        HashSet<TaskStatus> statusSet = Sets.newHashSet(status);
        tasks.forEach(t -> waitForStatus(storage, t, statusSet));
    }

    private static void waitForStatus(TaskStateStorage storage, TaskState task, Set<TaskStatus> status) {
        final long initial = new Date().getTime();

        while((new Date().getTime())-initial < 60000) {
            try {
                TaskStatus currentStatus = storage.getState(task.getId()).status();
                if (status.contains(currentStatus)) {
                    return;
                }
            } catch (Exception ignored){}
        }
    }

    public static Multiset<TaskId> completableTasks(List<TaskState> tasks) {
        Map<TaskId, Long> tasksById = tasks.stream().collect(groupingBy(TaskState::getId, counting()));
        Set<TaskId> retriedTasks = Maps.filterValues(tasksById, count -> count != null && count > 1).keySet();

        Set<TaskId> completableTasks = Sets.newHashSet();
        Set<TaskId> visitedTasks = Sets.newHashSet();

        Set<TaskId> appearedTasks = Sets.newHashSet();

        tasks.forEach(task -> {
            // A task is expected to complete only if:
            // 1. It has not already executed and failed
            // 2. it is not a failing task
            // 3. it is RUNNING or not being retried
            TaskId id = task.getId();
            boolean visited = visitedTasks.contains(id);
            boolean willFail = task.taskClass().equals(FailingTestTask.class);
            boolean isRunning = appearedTasks.contains(id);
            boolean isRetried = retriedTasks.contains(id);
            if (!visited && (isRunning || !isRetried)) {
                if (!willFail) {
                    completableTasks.add(id);
                }
                visitedTasks.add(id);
            }
            appearedTasks.add(id);
        });

        return ImmutableMultiset.copyOf(completableTasks);
    }

    public static Set<TaskId> failingTasks(List<TaskState> tasks) {
        Multiset<TaskId> completableTasks = completableTasks(tasks);
        return tasks.stream().map(TaskState::getId).filter(task -> !completableTasks.contains(task)).collect(toSet());
    }
}
