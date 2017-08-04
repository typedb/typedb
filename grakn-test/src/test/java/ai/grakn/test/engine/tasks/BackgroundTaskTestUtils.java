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

import ai.grakn.engine.TaskId;
import ai.grakn.engine.TaskStatus;
import static ai.grakn.engine.TaskStatus.COMPLETED;
import static ai.grakn.engine.TaskStatus.FAILED;
import static ai.grakn.engine.TaskStatus.STOPPED;
import ai.grakn.engine.tasks.BackgroundTask;
import ai.grakn.engine.tasks.manager.TaskConfiguration;
import ai.grakn.engine.tasks.manager.TaskSchedule;
import static ai.grakn.engine.tasks.manager.TaskSchedule.now;
import ai.grakn.engine.tasks.manager.TaskState;
import ai.grakn.engine.tasks.manager.TaskStateStorage;
import ai.grakn.engine.tasks.mock.FailingMockTask;
import ai.grakn.engine.tasks.mock.ShortExecutionMockTask;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Maps;
import com.google.common.collect.Multiset;
import com.google.common.collect.Sets;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toSet;
import mjson.Json;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class holding useful methods for use throughout background task tests
 * 
 * @author alex
 */
public class BackgroundTaskTestUtils {

    private final static Logger LOG = LoggerFactory.getLogger(BackgroundTaskTestUtils.class);

    public static TaskConfiguration configuration(TaskState taskState){
        return TaskConfiguration.of(Json.object("id", taskState.getId().getValue()));
    }

    public static TaskState createTask() {
        return createTask(ShortExecutionMockTask.class);
    }

    public static TaskState createTask(Class<? extends BackgroundTask> clazz) {
        return createTask(clazz, now());
    }

    public static TaskState createTask(Class<? extends BackgroundTask> clazz, TaskSchedule schedule) {
        return TaskState.of(clazz, BackgroundTaskTestUtils.class.getName(), schedule, TaskState.Priority.LOW);
    }

    public static void waitForDoneStatus(TaskStateStorage storage, Collection<TaskState> tasks) {
        waitForStatus(storage, tasks, COMPLETED, FAILED, STOPPED);
    }

    public static void waitForStatus(TaskStateStorage storage, Collection<TaskState> tasks, TaskStatus... status) {
        HashSet<TaskStatus> statusSet = Sets.newHashSet(status);
        Set<CompletableFuture<Void>> futures = tasks.stream()
                .map(t -> CompletableFuture.runAsync(() -> waitForStatus(storage, (TaskState) t, statusSet)))
                .collect(Collectors.toSet());
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).join();
    }

    private static void waitForStatus(TaskStateStorage storage, TaskState task, Set<TaskStatus> status) {
        waitForStatus(storage, task.getId(), status);
    }

    // Added the synchronized because for some reason some variable update wasn't shared between threads
    public static synchronized void waitForStatus(TaskStateStorage storage, TaskId task, Set<TaskStatus> status) {
        Instant initial = Instant.now();
        long duration = Duration.between(initial, Instant.now()).toMillis();
        while(duration < 120000) {
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
                break;
            }
            duration = Duration.between(initial, Instant.now()).toMillis();
        }
        TaskStatus finalStatus = storage.containsTask(task) ? storage.getState(task).status() : null;
        LOG.warn("Waiting for status of " + task + " to be any of " + status + ", but status is " + finalStatus);
    }

    public static Multiset<TaskId> completableTasks(Set<TaskState> tasks) {
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

    public static Set<TaskId> failingTasks(Set<TaskState> tasks) {
        Multiset<TaskId> completableTasks = completableTasks(tasks);
        return tasks.stream().map(TaskState::getId).filter(task -> !completableTasks.contains(task)).collect(toSet());
    }
}
