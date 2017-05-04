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

package ai.grakn.test.engine.tasks.manager;

import ai.grakn.engine.TaskStatus;
import ai.grakn.engine.tasks.TaskManager;
import ai.grakn.engine.tasks.TaskState;
import ai.grakn.engine.tasks.mock.EndlessExecutionMockTask;
import ai.grakn.engine.tasks.mock.ShortExecutionMockTask;
import ai.grakn.generator.TaskStates;
import ai.grakn.test.EngineContext;
import ai.grakn.test.engine.tasks.BackgroundTaskTestUtils;
import com.google.common.collect.ImmutableList;
import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.runner.RunWith;

import static ai.grakn.engine.TaskStatus.COMPLETED;
import static ai.grakn.engine.TaskStatus.FAILED;
import static ai.grakn.engine.TaskStatus.STOPPED;
import static ai.grakn.engine.tasks.TaskSchedule.now;
import static ai.grakn.engine.tasks.TaskSchedule.recurring;
import static ai.grakn.engine.tasks.mock.MockBackgroundTask.cancelledTasks;
import static ai.grakn.engine.tasks.mock.MockBackgroundTask.completedTasks;
import static ai.grakn.engine.tasks.mock.MockBackgroundTask.whenTaskFinishes;
import static ai.grakn.engine.tasks.mock.MockBackgroundTask.whenTaskStarts;
import static ai.grakn.test.engine.tasks.BackgroundTaskTestUtils.completableTasks;
import static ai.grakn.test.engine.tasks.BackgroundTaskTestUtils.configuration;
import static ai.grakn.test.engine.tasks.BackgroundTaskTestUtils.createTask;
import static ai.grakn.test.engine.tasks.BackgroundTaskTestUtils.failingTasks;
import static ai.grakn.test.engine.tasks.BackgroundTaskTestUtils.waitForDoneStatus;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isOneOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Test all implementations of the TaskManager interface
 * @author alexandraorth
 */
@RunWith(JUnitQuickcheck.class)
public class TaskManagerTest {

    @ClassRule
    public static EngineContext kafka = EngineContext.startKafkaServer();

    @Property(trials=10)
    public void afterRunning_AllNonFailingTasksAreRecordedAsCompleted(List<TaskState> tasks, TaskManager manager) {
        // Schedule tasks
        tasks.forEach((taskState) -> manager.addLowPriorityTask(taskState, configuration(taskState)));

        waitForDoneStatus(manager.storage(), tasks);

        completableTasks(tasks).forEach(task ->
                assertThat("Task " + task + " should have completed.", manager.storage().getState(task).status(), is(COMPLETED))
        );
    }

    @Property(trials=10)
    public void afterRunning_AllFailingTasksAreRecordedAsFailed(List<TaskState> tasks, TaskManager manager) {
        // Schedule tasks
        tasks.forEach((taskState) -> manager.addLowPriorityTask(taskState, configuration(taskState)));

        waitForDoneStatus(manager.storage(), tasks);

        failingTasks(tasks).forEach(task ->
                assertThat("Task " + task + " should have failed.", manager.storage().getState(task).status(), is(FAILED))
        );
    }

    @Ignore// Failing randomly - may be a race condition
    @Property(trials=10)
    public void whenStoppingATaskBeforeItsExecuted_TheTaskIsNotExecuted(TaskState task, TaskManager manager) {
        manager.stopTask(task.getId());

        manager.addLowPriorityTask(task, configuration(task));

        waitForDoneStatus(manager.storage(), ImmutableList.of(task));

        assertThat(completedTasks(), empty());
    }

    @Ignore// Failing randomly - may be a race condition
    @Property(trials=10)
    public void whenStoppingATaskBeforeItsExecuted_TheTaskIsMarkedAsStopped(TaskState task, TaskManager manager) {
        manager.stopTask(task.getId());

        manager.addLowPriorityTask(task, configuration(task));

        waitForDoneStatus(manager.storage(), ImmutableList.of(task));

        assertStatus(manager, task, STOPPED);
    }

    @Property(trials=10)
    public void whenStoppingATaskDuringExecution_TheTaskIsCancelled(
            @TaskStates.WithClass(EndlessExecutionMockTask.class) TaskState task, TaskManager manager) {
        whenTaskStarts(manager::stopTask);

        manager.addLowPriorityTask(task, configuration(task));

        waitForDoneStatus(manager.storage(), ImmutableList.of(task));

        assertThat(completedTasks(), empty());
        assertThat(cancelledTasks(), contains(task.getId()));
    }

    @Property(trials=10)
    public void whenStoppingATaskDuringExecution_TheTaskIsMarkedAsStopped(
            @TaskStates.WithClass(EndlessExecutionMockTask.class) TaskState task, TaskManager manager) {
        whenTaskStarts(manager::stopTask);

        manager.addLowPriorityTask(task, configuration(task));

        waitForDoneStatus(manager.storage(), ImmutableList.of(task));

        assertStatus(manager, task, STOPPED);
    }

    @Property(trials=10)
    public void whenStoppingATaskAfterExecution_TheTaskIsNotCancelled(TaskState task, TaskManager manager) {
        whenTaskFinishes(manager::stopTask);

        manager.addLowPriorityTask(task, configuration(task));

        waitForDoneStatus(manager.storage(), ImmutableList.of(task));

        assertThat(cancelledTasks(), empty());
    }

    @Property(trials=10)
    public void whenStoppingATaskAfterExecution_TheTaskIsMarkedAsCompleted(TaskState task, TaskManager manager) {
        whenTaskFinishes(manager::stopTask);

        manager.addLowPriorityTask(task, configuration(task));

        waitForDoneStatus(manager.storage(), ImmutableList.of(task));

        assertStatus(manager, task, COMPLETED, FAILED);
    }

    @Property(trials=10)
    public void whenRunningHighPriorityTaskAndManyLowPriorityTasks_TheHighPriorityRunsFirst(TaskManager manager)
            throws InterruptedException {
        List<TaskState> manyTasks = Stream.generate(BackgroundTaskTestUtils::createTask).limit(100).collect(toList());

        TaskState highPriorityTask = createTask(ShortExecutionMockTask.class, now());

        manyTasks.forEach((taskState) -> manager.addLowPriorityTask(taskState, configuration(taskState)));
        manager.addHighPriorityTask(highPriorityTask, configuration(highPriorityTask));

        waitForDoneStatus(manager.storage(), ImmutableList.of(highPriorityTask));
        waitForDoneStatus(manager.storage(), manyTasks);

        Instant highPriorityCompletedAt = manager.storage().getState(highPriorityTask.getId()).statusChangeTime();
        manyTasks.forEach(t -> {
            assertThat(highPriorityCompletedAt, lessThanOrEqualTo(manager.storage().getState(t.getId()).statusChangeTime().plusSeconds(1)));
        });
    }

    @Property(trials=10)
    @Ignore // race conditions on jenkins
    public void whenRunningARecurringTaskAndManyOtherTasks_TheRecurringTaskRunsRegularly(TaskManager manager)
            throws InterruptedException {
        Duration recurDur = Duration.ofMillis(200);
        Duration sleepDur = Duration.ofMillis(2000);

        Stream<TaskState> manyTasks = Stream.generate(BackgroundTaskTestUtils::createTask).limit(100);

        TaskState recurringTask = createTask(ShortExecutionMockTask.class, recurring(recurDur));

        // Since this test is sleeping and there are various overheads,
        // we are fairly liberal about how many times the task must run to avoid random failures
        long expectedTimesRecurringTaskCompleted = sleepDur.toMillis() / (2 * recurDur.toMillis());

        AtomicLong timesRecurringTaskCompleted = new AtomicLong(0);

        whenTaskFinishes(taskId -> {
            if (taskId.equals(recurringTask.getId())) {
                timesRecurringTaskCompleted.incrementAndGet();
            }
        });

        manyTasks.forEach((taskState) -> manager.addLowPriorityTask(taskState, configuration(taskState)));
        manager.addHighPriorityTask(recurringTask, configuration(recurringTask));

        Thread.sleep(sleepDur.toMillis());

        assertThat(timesRecurringTaskCompleted.get(), greaterThanOrEqualTo(expectedTimesRecurringTaskCompleted));
    }

    private void assertStatus(TaskManager manager, TaskState task, TaskStatus... status) {
        assertTrue("Task not in storage", manager.storage().containsTask(task.getId()));
        assertThat(manager.storage().getState(task.getId()).status(), isOneOf(status));
    }
}
