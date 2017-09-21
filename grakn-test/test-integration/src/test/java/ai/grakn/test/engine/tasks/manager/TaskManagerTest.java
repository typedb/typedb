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
import static ai.grakn.engine.TaskStatus.COMPLETED;
import ai.grakn.engine.tasks.manager.TaskCheckpoint;
import ai.grakn.engine.tasks.manager.TaskManager;
import static ai.grakn.engine.tasks.manager.TaskSchedule.now;
import static ai.grakn.engine.tasks.manager.TaskSchedule.recurring;
import ai.grakn.engine.tasks.manager.TaskState;
import ai.grakn.engine.tasks.mock.EndlessExecutionMockTask;
import ai.grakn.engine.tasks.mock.MockBackgroundTask;
import static ai.grakn.engine.tasks.mock.MockBackgroundTask.cancelledTasks;
import static ai.grakn.engine.tasks.mock.MockBackgroundTask.completedTasks;
import static ai.grakn.engine.tasks.mock.MockBackgroundTask.whenTaskFinishes;
import static ai.grakn.engine.tasks.mock.MockBackgroundTask.whenTaskResumes;
import static ai.grakn.engine.tasks.mock.MockBackgroundTask.whenTaskStarts;
import ai.grakn.engine.tasks.mock.ShortExecutionMockTask;
import ai.grakn.engine.util.EngineID;
import ai.grakn.generator.TaskManagers;
import ai.grakn.generator.TaskStates;
import ai.grakn.test.EngineContext;
import ai.grakn.test.engine.tasks.BackgroundTaskTestUtils;
import static ai.grakn.test.engine.tasks.BackgroundTaskTestUtils.completableTasks;
import static ai.grakn.test.engine.tasks.BackgroundTaskTestUtils.configuration;
import static ai.grakn.test.engine.tasks.BackgroundTaskTestUtils.createTask;
import static ai.grakn.test.engine.tasks.BackgroundTaskTestUtils.failingTasks;
import static ai.grakn.test.engine.tasks.BackgroundTaskTestUtils.waitForDoneStatus;
import static ai.grakn.test.engine.tasks.BackgroundTaskTestUtils.waitForStatus;
import com.google.common.collect.ImmutableList;
import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import java.time.Duration;
import static java.time.Duration.between;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import static java.util.stream.Collectors.toList;
import java.util.stream.Stream;
import mjson.Json;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isOneOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import org.junit.AfterClass;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test all implementations of the TaskManager interface
 * @author alexandraorth
 */
@RunWith(JUnitQuickcheck.class)
public class TaskManagerTest {

    private final static Logger LOG = LoggerFactory.getLogger(TaskManagerTest.class);

    @Before
    public void clearAllTasks(){
        MockBackgroundTask.clearTasks();
    }

    @ClassRule
    public static EngineContext engineContext = EngineContext.singleQueueServer();

    @AfterClass
    public static void closeTaskManagers(){
        TaskManagers.closeAndClear();
    }

    @Property(trials=10)
    public void afterRunning_AllNonFailingTasksAreRecordedAsCompleted(Set<TaskState> tasks, TaskManager manager) {
        // Schedule tasks
        tasks.forEach((taskState) -> manager.addTask(taskState, configuration(taskState)));

        waitForDoneStatus(manager.storage(), tasks);

        assertTrue(tasks.stream().map((taskState) -> manager.storage().getState(taskState.getId()))
                .peek(a -> System.out.println("ID: " + a.getId()))
                .allMatch(Objects::nonNull));

        completableTasks(tasks).forEach(task ->
                assertThat("Task " + task + " should have completed.", manager.storage().getState(task).status(), is(COMPLETED))
        );
    }

    @Property(trials=10)
    public void afterRunning_AllFailingTasksAreRecordedAsFailed(Set<TaskState> tasks, TaskManager manager) {
        // Schedule tasks
        tasks.forEach((taskState) -> manager.addTask(taskState, configuration(taskState)));

        waitForDoneStatus(manager.storage(), tasks);

        failingTasks(tasks).forEach(task ->
                assertThat("Task " + task + " should have failed.", manager.storage().getState(task).status(), is(TaskStatus.FAILED))
        );
    }

    @Property(trials=10)
    @Ignore("Stopping not implemented")
    public void whenStoppingATaskBeforeItsExecuted_TheTaskIsNotExecuted(TaskState task, TaskManager manager) {
        manager.stopTask(task.getId());

        manager.addTask(task, configuration(task));

        waitForDoneStatus(manager.storage(), ImmutableList.of(task));

        assertThat(completedTasks(), empty());
    }

    @Property(trials=10)
    @Ignore("Stopping not implemented")
    public void whenStoppingATaskBeforeItsExecuted_TheTaskIsMarkedAsStopped(TaskState task, TaskManager manager) {
        manager.stopTask(task.getId());

        manager.addTask(task, configuration(task));

        waitForDoneStatus(manager.storage(), ImmutableList.of(task));

        assertStatus(manager, task, TaskStatus.STOPPED);
    }

    @Property(trials=10)
    @Ignore("Stopping not implemented")
    public void whenStoppingATaskDuringExecution_TheTaskIsCancelled(
            @TaskStates.WithClass(EndlessExecutionMockTask.class) TaskState task, TaskManager manager) {
        whenTaskStarts(manager::stopTask);

        manager.addTask(task, configuration(task));

        waitForDoneStatus(manager.storage(), ImmutableList.of(task));

        assertThat(completedTasks(), empty());
        assertThat(cancelledTasks(), contains(task.getId()));
    }

    @Property(trials=10)
    @Ignore("Stopping not implemented")
    public void whenStoppingATaskDuringExecution_TheTaskIsMarkedAsStopped(
            @TaskStates.WithClass(EndlessExecutionMockTask.class) TaskState task, TaskManager manager) {
        whenTaskStarts(manager::stopTask);

        manager.addTask(task, configuration(task));

        waitForDoneStatus(manager.storage(), ImmutableList.of(task));

        assertStatus(manager, task, TaskStatus.STOPPED);
    }

    @Property(trials=10)
    @Ignore("Stopping not implemented")
    public void whenStoppingATaskAfterExecution_TheTaskIsNotCancelled(TaskState task, TaskManager manager) {
        whenTaskFinishes(manager::stopTask);

        manager.addTask(task, configuration(task));

        waitForDoneStatus(manager.storage(), ImmutableList.of(task));

        assertThat(cancelledTasks(), empty());
    }

    @Property(trials=10)
    @Ignore("Stopping not implemented")
    public void whenStoppingATaskAfterExecution_TheTaskIsMarkedAsCompleted(TaskState task, TaskManager manager) {
        whenTaskFinishes(manager::stopTask);

        manager.addTask(task, configuration(task));

        waitForDoneStatus(manager.storage(), ImmutableList.of(task));

        assertStatus(manager, task, COMPLETED, TaskStatus.FAILED);
    }

    @Property(trials=10)
    @Ignore // race conditions on jenkins
    public void whenRunningHighPriorityTaskAndManyLowPriorityTasks_TheHighPriorityRunsFirst(TaskManager manager)
            throws InterruptedException {
        List<TaskState> manyTasks = Stream.generate(BackgroundTaskTestUtils::createTask).limit(100).collect(toList());

        //TODO: When no longer ingnoring create actual high priority task
        TaskState highPriorityTask = createTask(ShortExecutionMockTask.class, now());

        manyTasks.forEach((taskState) -> manager.addTask(taskState, configuration(taskState)));
        manager.addTask(highPriorityTask, configuration(highPriorityTask));

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

        manyTasks.forEach((taskState) -> manager.addTask(taskState, configuration(taskState)));
        manager.addTask(recurringTask, configuration(recurringTask));

        Thread.sleep(sleepDur.toMillis());

        assertThat(timesRecurringTaskCompleted.get(), greaterThanOrEqualTo(expectedTimesRecurringTaskCompleted));
    }

    @Property(trials=10)
    @Ignore("Stopping not implemented")
    public void whenRecurringTaskSubmitted_ItExecutesMoreThanOnce(
            @TaskStates.WithClass(ShortExecutionMockTask.class) TaskState task, TaskManager manager) {

        final int numberOfExecutions = 5;
        final AtomicInteger startedCounter = new AtomicInteger(0);

        whenTaskStarts(taskId -> {
                    int numberTimesExecuted = startedCounter.incrementAndGet();
                    if(numberTimesExecuted == numberOfExecutions){
                        manager.stopTask(taskId);
                    }
                }
        );

        // Make task recurring
        task.schedule(recurring(Instant.now(), Duration.ofMillis(100)));

        // Execute task and wait for it to complete
        manager.addTask(task, configuration(task));
        waitForStatus(manager.storage(), ImmutableList.of(task), TaskStatus.STOPPED);

        // Assert correct results
        assertThat(manager.storage().getState(task.getId()).status(), is(TaskStatus.STOPPED));
        assertThat(startedCounter.get(), equalTo(numberOfExecutions));
    }

    @Property(trials=10)
    @Ignore("Stopping not implemented")
    public void whenRecurringTaskSubmitted_ThereIsAnIntervalBetweenExecutions(
            @TaskStates.WithClass(ShortExecutionMockTask.class) TaskState task, TaskManager manager) {

        final int numberOfExecutions = 5;
        final Duration interval = Duration.ofMillis(100);
        final Instant[] lastExecutionTime = {null};
        final AtomicInteger startedCounter = new AtomicInteger(0);

        whenTaskStarts(taskId -> {
            if(lastExecutionTime[0] != null) {
                assertThat(between(lastExecutionTime[0], Instant.now()), greaterThanOrEqualTo(interval));
            }

            // Store the previous execution time for next round
            lastExecutionTime[0] = manager.storage().getState(taskId).schedule().runAt();

            // Stop the recurring task so this test does not run forever
            if(startedCounter.incrementAndGet() == numberOfExecutions){
                manager.stopTask(taskId);
            }
        });

        // Make task recurring
        task.schedule(recurring(interval));

        // Execute task and wait for it to complete
        manager.addTask(task, configuration(task));
        waitForStatus(manager.storage(), ImmutableList.of(task), TaskStatus.STOPPED, TaskStatus.FAILED);

        // Assert correct results
        assertStatus(manager, task, TaskStatus.STOPPED);
        assertThat(startedCounter.get(), equalTo(numberOfExecutions));
    }

    @Property(trials=10)
    public void whenRecurringTaskThrowsException_ItStopsExecuting(
            @TaskStates.WithClass(ShortExecutionMockTask.class) TaskState task, TaskManager manager) {

        final int expectedExecutionsBeforeFailure = 1;
        final AtomicInteger startedCounter = new AtomicInteger(0);

        whenTaskStarts(taskId -> {
            startedCounter.incrementAndGet();
            throw new RuntimeException("Deliberate test failure");
        });

        // Make task recurring
        task.schedule(recurring(Instant.now(), Duration.ofSeconds(10)));

        // Execute task and wait for it to complete
        manager.addTask(task, configuration(task));
        waitForDoneStatus(manager.storage(), ImmutableList.of(task));

        // Assert correct state
        assertStatus(manager, task, TaskStatus.FAILED);
        assertThat(startedCounter.get(), equalTo(expectedExecutionsBeforeFailure));
    }

    @Property(trials=10)
    @Ignore("Checkpoint not implemented")
    public void whenATaskIsRestartedAfterExecution_ItIsResumed(
            @TaskStates.WithClass(ShortExecutionMockTask.class) TaskState task, TaskManager manager) {
        ShortExecutionMockTask.resumedCounter.set(0);

        TaskCheckpoint checkpoint = TaskCheckpoint.of(Json.object("checkpoint", true));
        task.markRunning(EngineID.me()).checkpoint(checkpoint);

        manager.addTask(task, configuration(task));
        waitForDoneStatus(manager.storage(), ImmutableList.of(task));

        assertEquals(1, ShortExecutionMockTask.resumedCounter.get());
    }

    @Property(trials=10)
    @Ignore("Checkpoint not implemented")
    public void whenATaskIsRestartedAfterExecution_ItIsResumedFromLastCheckpoint(
            @TaskStates.WithClass(ShortExecutionMockTask.class) TaskState task, TaskManager manager) {
        TaskCheckpoint checkpoint = TaskCheckpoint.of(Json.object("checkpoint", true));
        task.markRunning(EngineID.me()).checkpoint(checkpoint);

        whenTaskResumes((c) -> assertThat(c, equalTo(checkpoint)));

        // Execute task and wait for it to complete
        manager.addTask(task, configuration(task));
        waitForDoneStatus(manager.storage(), ImmutableList.of(task));

        // Assert that status is not FAILED
        assertStatus(manager, task, COMPLETED);
    }

    @Property(trials=10)
    @Ignore("Checkpoint not implemented")
    public void whenATaskIsStoppedDuringExecution_ItSavesItsLastCheckpoint(
            @TaskStates.WithClass(EndlessExecutionMockTask.class) TaskState task, TaskManager manager) {
        whenTaskStarts(manager::stopTask);

        manager.addTask(task, configuration(task));

        waitForDoneStatus(manager.storage(), ImmutableList.of(task));

        assertThat(manager.storage().getState(task.getId()).checkpoint(), notNullValue());
    }


    private void assertStatus(TaskManager manager, TaskState task, TaskStatus... expectedStatus) {
        assertTrue("Task not in storage", manager.storage().containsTask(task.getId()));

        // Get the state from storage
        TaskState stateInStorage = manager.storage().getState(task.getId());
        TaskStatus statusInStorage = stateInStorage.status();

        // If the task has failed and was not expected to, print why
        if(statusInStorage == TaskStatus.FAILED && !Arrays.asList(expectedStatus).contains(statusInStorage)){
            LOG.error(stateInStorage.stackTrace());
        }

        // Assert the final state was expected
        assertThat(statusInStorage, isOneOf(expectedStatus));
    }
}