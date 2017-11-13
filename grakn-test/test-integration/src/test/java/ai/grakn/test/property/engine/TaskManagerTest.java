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

package ai.grakn.test.property.engine;

import ai.grakn.engine.TaskStatus;
import ai.grakn.engine.tasks.manager.TaskManager;
import ai.grakn.engine.tasks.manager.TaskState;
import ai.grakn.engine.tasks.mock.MockBackgroundTask;
import ai.grakn.engine.tasks.mock.ShortExecutionMockTask;
import ai.grakn.generator.TaskStates;
import ai.grakn.test.rule.EngineContext;
import ai.grakn.test.engine.tasks.BackgroundTaskTestUtils;
import com.google.common.collect.ImmutableList;
import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static ai.grakn.engine.TaskStatus.COMPLETED;
import static ai.grakn.engine.tasks.manager.TaskSchedule.now;
import static ai.grakn.engine.tasks.manager.TaskSchedule.recurring;
import static ai.grakn.engine.tasks.mock.MockBackgroundTask.whenTaskFinishes;
import static ai.grakn.engine.tasks.mock.MockBackgroundTask.whenTaskStarts;
import static ai.grakn.test.engine.tasks.BackgroundTaskTestUtils.completableTasks;
import static ai.grakn.test.engine.tasks.BackgroundTaskTestUtils.configuration;
import static ai.grakn.test.engine.tasks.BackgroundTaskTestUtils.createTask;
import static ai.grakn.test.engine.tasks.BackgroundTaskTestUtils.failingTasks;
import static ai.grakn.test.engine.tasks.BackgroundTaskTestUtils.waitForDoneStatus;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isOneOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

//TODO: Is this test even valid anymore? The javadocs say no.
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
    public static EngineContext engine = EngineContext.createWithInMemoryRedis();

    @Property(trials=10)
    public void afterRunning_AllNonFailingTasksAreRecordedAsCompleted(Set<TaskState> tasks) {
        engine.getTaskManager();
        // Schedule tasks
        tasks.forEach((taskState) -> engine.getTaskManager().addTask(taskState, configuration(taskState)));

        waitForDoneStatus(engine.getTaskManager().storage(), tasks);

        assertTrue(tasks.stream().map((taskState) -> engine.getTaskManager().storage().getState(taskState.getId()))
                .peek(a -> System.out.println("ID: " + a.getId()))
                .allMatch(Objects::nonNull));

        completableTasks(tasks).forEach(task ->
                assertThat("Task " + task + " should have completed.", engine.getTaskManager().storage().getState(task).status(), is(COMPLETED))
        );
    }

    @Property(trials=10)
    public void afterRunning_AllFailingTasksAreRecordedAsFailed(Set<TaskState> tasks) {
        // Schedule tasks
        tasks.forEach((taskState) -> engine.getTaskManager().addTask(taskState, configuration(taskState)));

        waitForDoneStatus(engine.getTaskManager().storage(), tasks);

        failingTasks(tasks).forEach(task ->
                assertThat("Task " + task + " should have failed.", engine.getTaskManager().storage().getState(task).status(), is(TaskStatus.FAILED))
        );
    }

    @Property(trials=10)
    @Ignore // race conditions on jenkins
    public void whenRunningHighPriorityTaskAndManyLowPriorityTasks_TheHighPriorityRunsFirst()
            throws InterruptedException {
        List<TaskState> manyTasks = Stream.generate(BackgroundTaskTestUtils::createTask).limit(100).collect(toList());

        //TODO: When no longer ingnoring create actual high priority task
        TaskState highPriorityTask = createTask(ShortExecutionMockTask.class, now());

        manyTasks.forEach((taskState) -> engine.getTaskManager().addTask(taskState, configuration(taskState)));
        engine.getTaskManager().addTask(highPriorityTask, configuration(highPriorityTask));

        waitForDoneStatus(engine.getTaskManager().storage(), ImmutableList.of(highPriorityTask));
        waitForDoneStatus(engine.getTaskManager().storage(), manyTasks);

        Instant highPriorityCompletedAt = engine.getTaskManager().storage().getState(highPriorityTask.getId()).statusChangeTime();
        manyTasks.forEach(t -> {
            assertThat(highPriorityCompletedAt, lessThanOrEqualTo(engine.getTaskManager().storage().getState(t.getId()).statusChangeTime().plusSeconds(1)));
        });
    }

    @Property(trials=10)
    @Ignore // race conditions on jenkins
    public void whenRunningARecurringTaskAndManyOtherTasks_TheRecurringTaskRunsRegularly()
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

        manyTasks.forEach((taskState) -> engine.getTaskManager().addTask(taskState, configuration(taskState)));
        engine.getTaskManager().addTask(recurringTask, configuration(recurringTask));

        Thread.sleep(sleepDur.toMillis());

        assertThat(timesRecurringTaskCompleted.get(), greaterThanOrEqualTo(expectedTimesRecurringTaskCompleted));
    }

    @Property(trials=10)
    public void whenRecurringTaskThrowsException_ItStopsExecuting(
            @TaskStates.WithClass(ShortExecutionMockTask.class) TaskState task) {

        final int expectedExecutionsBeforeFailure = 1;
        final AtomicInteger startedCounter = new AtomicInteger(0);

        whenTaskStarts(taskId -> {
            startedCounter.incrementAndGet();
            throw new RuntimeException("Deliberate test failure");
        });

        // Make task recurring
        task.schedule(recurring(Instant.now(), Duration.ofSeconds(10)));

        // Execute task and wait for it to complete
        engine.getTaskManager().addTask(task, configuration(task));
        waitForDoneStatus(engine.getTaskManager().storage(), ImmutableList.of(task));

        // Assert correct state
        assertStatus(engine.getTaskManager(), task, TaskStatus.FAILED);
        assertThat(startedCounter.get(), equalTo(expectedExecutionsBeforeFailure));
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