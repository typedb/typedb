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
import ai.grakn.engine.TaskId;
import ai.grakn.engine.tasks.TaskSchedule;
import ai.grakn.engine.tasks.TaskState;
import ai.grakn.engine.tasks.TaskStateStorage;
import ai.grakn.engine.tasks.mock.FailingMockTask;
import ai.grakn.engine.tasks.mock.ShortExecutionMockTask;
import ai.grakn.engine.util.EngineID;
import ai.grakn.migration.base.Migrator;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Maps;
import com.google.common.collect.Multiset;
import com.google.common.collect.Sets;
import java.time.Duration;
import java.time.Instant;
import mjson.Json;

import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static ai.grakn.engine.TaskStatus.COMPLETED;
import static ai.grakn.engine.TaskStatus.FAILED;
import static ai.grakn.engine.TaskStatus.STOPPED;
import static ai.grakn.engine.tasks.TaskSchedule.now;
import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.Stream.generate;
import static org.junit.Assert.fail;

/**
 * Class holding useful methods for use throughout background task tests
 */
public class BackgroundTaskTestUtils {

    private final static Logger LOG = LoggerFactory.getLogger(BackgroundTaskTestUtils.class);

    public static Set<TaskState> createTasks(int n) {
        return generate(() -> createTask(ShortExecutionMockTask.class)).limit(n).collect(toSet());
    }

    public static Set<TaskState> createScheduledTasks(int n) {
        return generate(() -> createTask(ShortExecutionMockTask.class).markScheduled()).limit(n).collect(toSet());
    }

    public static Set<TaskState> createRunningTasks(int n, EngineID engineID) {
        return generate(() -> createTask(ShortExecutionMockTask.class).markRunning(engineID)).limit(n).collect(toSet());
    }

    public static TaskState createTask() {
        return createTask(ShortExecutionMockTask.class);
    }

    public static TaskState createTask(Class<? extends BackgroundTask> clazz) {
        return createTask(clazz, now());
    }

    public static TaskState createTask(Class<? extends BackgroundTask> clazz, TaskSchedule schedule) {
        return createTask(clazz, schedule, Json.object());
    }

    public static TaskState createTask(Class<? extends BackgroundTask> clazz, TaskSchedule schedule, Json configuration) {
        TaskState taskState = TaskState.of(clazz, BackgroundTaskTestUtils.class.getName(), schedule, configuration);
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
        waitForStatus(storage, task.getId(), status);
    }

    public static void waitForStatus(TaskStateStorage storage, TaskId task, Set<TaskStatus> status) {
        Instant initial = Instant.now();

        while(true) {
            long duration = Duration.between(initial, Instant.now()).toMillis();
            if (duration > 60000){
                TaskStatus finalStatus = storage.containsTask(task) ? storage.getState(task).status() : null;
                LOG.warn("Waiting for status of " + task + " to be any of " + status + ", but status is " + finalStatus);
                initial = Instant.now();
            }

            if (storage.containsTask(task)) {
                TaskStatus currentStatus = storage.getState(task).status();
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
            boolean willFail = task.taskClass().equals(FailingMockTask.class);
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
