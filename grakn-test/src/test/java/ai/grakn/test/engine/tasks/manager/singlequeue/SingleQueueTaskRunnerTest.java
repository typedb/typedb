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
 *
 */

package ai.grakn.test.engine.tasks.manager.singlequeue;

import ai.grakn.engine.tasks.TaskState;
import ai.grakn.engine.tasks.manager.singlequeue.SingleQueueTaskRunner;
import ai.grakn.engine.tasks.storage.TaskStateInMemoryStore;
import ai.grakn.test.engine.tasks.FailingTask;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Multiset;
import com.google.common.collect.Sets;
import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static ai.grakn.engine.TaskStatus.COMPLETED;
import static ai.grakn.engine.TaskStatus.CREATED;
import static ai.grakn.engine.TaskStatus.FAILED;
import static ai.grakn.engine.TaskStatus.RUNNING;
import static ai.grakn.test.engine.tasks.TestTask.clearCompletedTasks;
import static ai.grakn.test.engine.tasks.TestTask.completedTasks;
import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(JUnitQuickcheck.class)
public class SingleQueueTaskRunnerTest {

    private SingleQueueTaskRunner taskRunner;
    private TaskStateInMemoryStore storage;

    private MockGraknConsumer<String, String> consumer;
    private TopicPartition partition;
    private ExecutorService executor;

    @Before
    public void setUp() {
        clearCompletedTasks();

        storage = new TaskStateInMemoryStore();

        consumer = new MockGraknConsumer<>(OffsetResetStrategy.EARLIEST);

        partition = new TopicPartition("hi", 0);

        consumer.assign(ImmutableSet.of(partition));
        consumer.updateBeginningOffsets(ImmutableMap.of(partition, 0L));
        consumer.updateEndOffsets(ImmutableMap.of(partition, 0L));

        executor = Executors.newCachedThreadPool();
    }

    public void setUpTasks(List<List<TaskState>> tasks) {
        taskRunner = new SingleQueueTaskRunner(storage, consumer, executor);

        createValidQueue(tasks);

        for (List<TaskState> taskList : tasks) {
            consumer.schedulePollTask(() -> taskList.forEach(this::addTask));
        }

        Thread closeTaskRunner = new Thread(() -> {
            try {
                taskRunner.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        consumer.schedulePollTask(closeTaskRunner::start);
    }

    public void waitToComplete() {
        executor.shutdown();
        try {
            executor.awaitTermination(1, TimeUnit.HOURS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public Stream<TaskState> tasks(List<? extends List<TaskState>> tasks) {
        return tasks.stream().flatMap(Collection::stream);
    }

    public Set<String> completableTasks(List<? extends List<TaskState>> tasks) {
        Map<String, Long> tasksById = tasks(tasks).collect(groupingBy(TaskState::getId, counting()));
        Set<String> retriedTasks = Maps.filterValues(tasksById, count -> count != null && count > 1).keySet();

        Set<String> completableTasks = Sets.newHashSet();
        Set<String> visitedTasks = Sets.newHashSet();

        tasks(tasks).forEach(task -> {
            // A task is expected to complete only if:
            // 1. It has not already executed and failed
            // 2. it is not a failing task
            // 3. it is RUNNING or not being retried
            String id = task.getId();
            boolean visited = visitedTasks.contains(id);
            boolean willFail = task.taskClass().equals(FailingTask.class);
            boolean isRunning = task.status().equals(RUNNING);
            boolean isRetried = retriedTasks.contains(id);
            if (!visited && (isRunning || !isRetried)) {
                if (!willFail) {
                    completableTasks.add(id);
                }
                visitedTasks.add(id);
            }
        });

        return completableTasks;
    }

    public Set<String> failingTasks(List<List<TaskState>> tasks) {
        Set<String> completableTasks = completableTasks(tasks);
        return tasks(tasks).map(TaskState::getId).filter(task -> !completableTasks.contains(task)).collect(toSet());
    }

    private void createValidQueue(List<List<TaskState>> tasks) {
        Set<String> appearedTasks = Sets.newHashSet();

        tasks(tasks).forEach(task -> {
            String taskId = task.getId();

            if (!appearedTasks.contains(taskId)) {
                task.status(CREATED);
            } else {
                // The second time a task appears it must be in RUNNING state
                task.status(RUNNING);
                storage.updateState(task);
            }

            appearedTasks.add(taskId);
        });
    }

    private void addTask(TaskState task) {
        String serialized = TaskState.serialize(task);
        Long offset = consumer.endOffsets(ImmutableSet.of(partition)).get(partition);
        consumer.addRecord(new ConsumerRecord<>(partition.topic(), partition.partition(), offset, task.getId(), serialized));
        consumer.updateEndOffsets(ImmutableMap.of(partition, offset + 1));
    }

    @Property(trials=10)
    public void afterRunning_AllTasksAreAddedToStorage(List<List<TaskState>> tasks) throws Exception {
        setUpTasks(tasks);

        taskRunner.run();

        waitToComplete();

        tasks(tasks).forEach(task ->
                assertNotNull(storage.getState(task.getId()))
        );
    }

    @Property(trials=10)
    public void afterRunning_AllNonFailingTasksAreRecordedAsCompleted(List<List<TaskState>> tasks) throws Exception {
        setUpTasks(tasks);

        taskRunner.run();

        waitToComplete();

        completableTasks(tasks).forEach(task ->
                assertThat(storage.getState(task).status(), is(COMPLETED))
        );
    }

    @Property(trials=10)
    public void afterRunning_AllFailingTasksAreRecordedAsFailed(List<List<TaskState>> tasks) throws Exception {
        setUpTasks(tasks);

        taskRunner.run();

        waitToComplete();

        failingTasks(tasks).forEach(task ->
                assertThat(storage.getState(task).status(), is(FAILED))
        );
    }

    @Property(trials=10)
    public void afterRunning_AllNonFailingTasksHaveCompletedExactlyOnce(List<List<TaskState>> tasks) throws Exception {
        setUpTasks(tasks);

        taskRunner.run();

        waitToComplete();

        Multiset<String> expectedCompletedTasks = ImmutableMultiset.copyOf(completableTasks(tasks));

        assertEquals(expectedCompletedTasks, completedTasks());
    }

    @Property(trials=10)
    public void afterRunning_AllTasksHaveBeenSubmittedToExecutor(List<List<TaskState>> tasks) {
        executor = mock(ExecutorService.class);

        setUpTasks(tasks);

        taskRunner.run();

        int expectedSubmissions = completableTasks(tasks).size() + failingTasks(tasks).size();

        verify(executor, times(expectedSubmissions)).submit(any(Runnable.class));
    }

    @Test
    public void whenRunIsCalled_DontReturnUntilCloseIsCalled() throws Exception {
        setUpTasks(ImmutableList.of());

        Thread thread = new Thread(taskRunner);
        thread.start();

        assertTrue(thread.isAlive());

        taskRunner.close();
        thread.join();

        assertFalse(thread.isAlive());
    }
}