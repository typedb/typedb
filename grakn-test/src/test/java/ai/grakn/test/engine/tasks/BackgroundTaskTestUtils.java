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
import ai.grakn.engine.tasks.BackgroundTask;
import ai.grakn.engine.tasks.TaskId;
import ai.grakn.engine.tasks.TaskSchedule;
import ai.grakn.engine.tasks.TaskState;
import ai.grakn.engine.tasks.TaskStateStorage;
import com.google.common.collect.ConcurrentHashMultiset;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Maps;
import com.google.common.collect.Multiset;
import com.google.common.collect.Sets;
import mjson.Json;

import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static ai.grakn.engine.TaskStatus.COMPLETED;
import static ai.grakn.engine.TaskStatus.FAILED;
import static ai.grakn.engine.TaskStatus.STOPPED;
import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.fail;

/**
 * Class holding useful methods for use throughout background task tests
 */
public class BackgroundTaskTestUtils {

    private static final ConcurrentHashMultiset<TaskId> COMPLETED_TASKS = ConcurrentHashMultiset.create();

    private static final ConcurrentHashMultiset<TaskId> CANCELLED_TASKS = ConcurrentHashMultiset.create();
    private static Consumer<TaskId> onTaskStart;
    private static Consumer<TaskId> onTaskFinish;

    static void addCompletedTask(TaskId taskId) {
        COMPLETED_TASKS.add(taskId);
    }

    public static ImmutableMultiset<TaskId> completedTasks() {
        return ImmutableMultiset.copyOf(COMPLETED_TASKS);
    }

    static void addCancelledTask(TaskId taskId) {
        CANCELLED_TASKS.add(taskId);
    }

    public static ImmutableMultiset<TaskId> cancelledTasks() {
        return ImmutableMultiset.copyOf(CANCELLED_TASKS);
    }

    public static void whenTaskStarts(Consumer<TaskId> beforeTaskStarts) {
        BackgroundTaskTestUtils.onTaskStart = beforeTaskStarts;
    }

    static void onTaskStart(TaskId taskId) {
        if (onTaskStart != null) onTaskStart.accept(taskId);
    }

    public static void whenTaskFinishes(Consumer<TaskId> onTaskFinish) {
        BackgroundTaskTestUtils.onTaskFinish = onTaskFinish;
    }

    static void onTaskFinish(TaskId taskId) {
        if (onTaskFinish != null) onTaskFinish.accept(taskId);
    }

    public static void clearTasks() {
        COMPLETED_TASKS.clear();
        CANCELLED_TASKS.clear();
        onTaskStart = null;
        onTaskFinish = null;
    }

    public static Set<TaskState> createTasks(int n, TaskStatus status) {
        return IntStream.range(0, n)
                .mapToObj(i -> createTask(status, TaskSchedule.now(), Json.object()))
                .collect(toSet());
    }

    public static TaskState createTask(TaskStatus status, TaskSchedule schedule, Json configuration) {
        return createTask(ShortExecutionTestTask.class, status, schedule, configuration);
    }

    public static TaskState createTask(Class<? extends BackgroundTask> backgroundTask, TaskStatus status, TaskSchedule schedule, Json configuration) {
        TaskState taskState = TaskState.of(backgroundTask, BackgroundTaskTestUtils.class.getName(), schedule, configuration)
                .status(status)
                .statusChangedBy(BackgroundTaskTestUtils.class.getName());
        configuration.set("id", taskState.getId().getValue());
        return taskState;
    }

    public static void waitForDoneStatus(TaskStateStorage storage, Collection<TaskState> tasks) {
        waitForStatus(storage, tasks, COMPLETED, FAILED, STOPPED);
    }

    public static void waitForStatus(TaskStateStorage storage, Collection<TaskState> tasks, TaskStatus... status) {
        HashSet<TaskStatus> statusSet = Sets.newHashSet(status);
        tasks.forEach(t -> waitForStatus(storage, t, statusSet));
    }

    private static void waitForStatus(TaskStateStorage storage, TaskState task, Set<TaskStatus> status) {
        final long initial = new Date().getTime();

        while((new Date().getTime())-initial < 60000) {
            if (storage.containsTask(task.getId())) {
                TaskStatus currentStatus = storage.getState(task.getId()).status();
                if (status.contains(currentStatus)) {
                    return;
                }
            }

            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        TaskStatus finalStatus = storage.containsTask(task.getId()) ? storage.getState(task.getId()).status() : null;

        fail("Timeout waiting for status of " + task + " to be any of " + status + ", but status is " + finalStatus);
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
